# dsh-transport-guard

把 `DeepSeek Messages transport failed` 从一句**无法定位**的兜底报错，变成一条**可归因、可区分、可自愈**的失败。

- 分类：区分「连接级瞬时故障」与「传输层 ~300 秒上限」，后者改写为 `TIMEOUT` 并写明真实耗时；
- 归因：尽力关联 undici 的底层错误码（`ECONNRESET` / `UND_ERR_HEADERS_TIMEOUT` …），附进消息；
- 自愈：在 provider 自带重试策略耗尽后，对「尚未收到任何输出」的连接级故障再给有限次重试。

---

## 1. 为什么要写它：先看证据

对 `.dsh/sessions/*/session.v4.jsonl.zstd` 解码后统计（6 个会话 / 2437 个事件 / 全部失败尝试）：

| 观察项 | 实测结果 |
| --- | --- |
| 每次失败耗时 | **0.06–0.3 秒**（`sincePrevEvent` 均为 0.1s 量级） |
| 失败时已收到的 provider 输出 | **0 个分片**（`streamedChunks = 0`） |
| 之后的重试 | 往往**立刻成功**（`delivery-accepted` → `assistant/message`） |
| 是否出现过 ~300 秒的等待 | **一次都没有** |
| 同时段的其它网络请求 | `web fetch failed: TypeError: fetch failed` |

结论：真正丢轮次的是**亚秒级的连接级瞬时故障**（复用的 keep-alive 连接已被对端关闭 / 网络抖动），
以及「provider 重试预算被 6 次连续快速失败耗尽后整轮结束」。

不过「误报」这件事本身是**真实存在**的架构问题，只是触发条件是另一条路径：

```
dsh-llm-deepseek 的兜底分支（lib/index.js:2132）
    throw new LlmError("DeepSeek Messages transport failed", "TRANSPORT", { cause: error })
```

它把 `cause` 丢掉，`LlmFailure` 只剩 `{ message, code }`，于是下面两种完全不同的故障
得到**一模一样**的文案：

1. **连接级瞬时故障** —— 旧连接失效 / 网络抖动 → 立刻失败；
2. **传输层的 ~300 秒上限** —— undici fetch 的默认 `headersTimeout` / `bodyTimeout`
   都是 `300_000 ms`（Node 内置 dispatcher 与 `dsh-http-proxy` 安装的 `new Agent()` 同值）。

而且 DSH 自己的空闲看门狗 `streamIdleTimeoutMs` **默认也是 `300_000 ms`**，与 undici
处在同一量级，构成一条**竞态**：

- 看门狗先到 → `TIMEOUT`（正确，文案是 "stream idle timeout"）；
- undici 先到 → `TRANSPORT`（**误报**，就是那句无从下手的兜底文案）。

本插件用「实测耗时」把这条竞态的两端重新分开，因此无论谁先到，你看到的信息都是准确的。

---

## 2. 它具体做什么

### A. 分类：用耗时区分故障性质

在 `llm/stream` 瀑布里为**每一次尝试**计时（从开始拉取到出现终局 `finish` 分片）：

| 情形 | 处理 |
| --- | --- |
| `code` 是网络族、耗时 ≥ `transportTimeoutFloorMs`（默认 290 s） | 归因为**传输层上限**：`code` 改写为 `TIMEOUT`，消息注明"这是 ~300s 传输上限，不是连接被拒" |
| `code` 是网络族、耗时很短 | 保留 `TRANSPORT`，消息注明"X 秒内失败、**未收到任何 provider 输出**、属连接级故障" |
| `code` 本来就是 `TIMEOUT`（DSH 空闲看门狗） | 保留，并注明是"空闲看门狗"，**不会**被误述成连接级故障 |

改写后的消息形如：

```
DeepSeek Messages transport failed [transport-guard: failed within 0.06s, before any provider
output — connection-level failure] (ECONNRESET: other side closed)

DeepSeek Messages transport failed [transport-guard: timed out after 300.2s of provider silence
— this is the ~300s transport limit, not a refused connection]
```

配合 `messageLanguage: 'zh'` 可输出中文文案。

### B. 归因：关联 undici 的底层错误

尽力订阅 Node 的 `undici:request:error` 诊断通道，并按**时间窗口**（本次尝试的开始~结束）
把底层错误码 / 消息关联到这次失败，附在消息末尾的括号里。

通道缺失（例如 fetch 不是 undici 实现）时**静默降级**，只是少了括号里那段，不影响其它行为。

### C. 自愈：预算耗尽后再给有限次重试

`agent/request-error` 是 DSH 的请求恢复扩展点（`dsh-llm-retry` 正常重试策略就装在这里）。
本插件在**下游已经放弃**时才接手，并对以下条件全部成立的失败追加一次额外重试：

- `code` ∈ `TRANSPORT` / `TIMEOUT` / `STREAM_CLOSED`；
- **本次尝试没有收到任何 provider 输出**（`onlyBeforeFirstChunk`，避免丢失已生成内容）；
- 该 `turn/step` 的额外重试预算未用尽（默认 4 次，指数退避 750ms → 8s）；
- 轮次中止信号未触发。

它**不与 `dsh-llm-retry` 抢决策**：下游若已决定重试，本插件原样传回且不消耗预算。

> 这是**最直接**的修复：一次几百毫秒的网络抖动不会再终结整轮对话。

---

## 3. 配置

在 profile 的 patch 层（`~/.dsh/profiles/<profile>/cordis.patch.yml`）里加一条 id 定位的条目：

```yaml
- id: dsh-transport-guard
  name: dsh-transport-guard
  config:
    transportTimeoutFloorMs: 290000
    extraRetries: 4
    messageLanguage: zh
```

| 键 | 默认值 | 说明 |
| --- | --- | --- |
| `enabled` | `true` | 总开关 |
| `transportTimeoutFloorMs` | `290000` | 耗时不小于它 → 归因为传输层 ~300s 上限 |
| `reclassifyTimeouts` | `true` | 是否把这类 `TRANSPORT` 的 `code` 改写成 `TIMEOUT` |
| `enrichMessages` | `true` | 是否补充耗时/阶段/底层原因 |
| `messageLanguage` | `'en'` | `'en'` 与 DSH 自有文案一致；`'zh'` 便于自己排查 |
| `extraRetries` | `4` | 额外重试次数，`0` 关闭自愈 |
| `extraRetryBaseDelayMs` / `extraRetryMaxDelayMs` | `750` / `8000` | 额外重试的指数退避区间 |
| `onlyBeforeFirstChunk` | `true` | 只在"尚未收到任何输出"时额外重试 |
| `providers` | `[]` | 只对这些 provider 生效；空数组 = 全部 |
| `diagnoseTransport` | `true` | 是否订阅 undici 诊断通道做归因 |

任何非法值都会**回退默认值并写一条 warn**，绝不让配置问题阻断插件加载。

> 本插件**不声明** `Config` schema（因此不依赖 `@deepseek-ai/schemastery`）。cordis 对没有
> schema 的插件会**原样**把 patch 里的 `config` 传给 `apply()`，所以上面的配置确实生效；
> 代价是插件设置页不会为它渲染表单。

---

## 4. 安装 / 卸载

**推荐：GUI** —— 设置 → 插件（Plugin manager），本插件会显示为 `dsh-transport-guard`。

**CLI**（注意 `dsh` 通常不在 PATH 上，用应用自带 shim）：

```powershell
$dsh  = 'D:\DSH\DeepSeek Harness\resources\runtime\cli\bin\dsh.cmd'   # 你的安装位置
$repo = 'D:\path\to\dsh-transport-guard'                              # 本仓库的绝对路径

# 安装（link 指向源码目录，改代码即时生效）
& $dsh plugin --profile desktop add "link:$repo"

# 卸载
& $dsh plugin --profile desktop remove dsh-transport-guard
```

### 安装后自检

宿主 Loader 树里应出现条目 `include:dsh-transport-guard`，且：

- `enabled: true`
- `fiberPhase: "active"` —— 表示 `apply()` 已成功执行、插件正在运行

（不需要重启宿主：已安装的 profile 会直接应用新的组合。）

生效后宿主日志里会出现一行：

```
transport-guard: active (timeoutFloor=290s, extraRetries=4, onlyBeforeFirstChunk=true, diagnose=true)
```

---

## 5. 验证

```powershell
# 行为测试（12 项，无依赖、无子进程）—— 常规跑法：
npm test

# 若所在环境禁止 spawn 子进程（node --test 默认会），用单进程模式：
node --test --test-isolation=none test/guard.test.mjs

# 与真实 cordis 的契约探针（15 项，需先按 forensics/README 取出 vendor 副本）
node forensics/cordis-probe/probe.mjs
```

探针用归档里**真正的** `@deepseek-ai/cordis 4.0.4` 装载本插件并真实分发 waterfall，
验证「插件形态 / `(options, next)` 签名 / 返回值成为最终决策 / 配置到达 `apply()`」这些
只靠读文档容易搞错的契约。当前结果：**12/12 与 15/15 全部通过**。

---

## 6. 注意

1. **不会把 ~300 秒的硬上限变长。** undici 的 `headersTimeout` / `bodyTimeout` 不可通过
   DSH 配置调整；要真正放宽只能替换全局 undici dispatcher，而 `dsh-http-proxy` 也在
   管理那个 dispatcher（代理支持会受影响），所以我**刻意不做**这件事。
   实际后果：**无论你把 `streamIdleTimeoutMs` 调多大，provider 静默超过 ~300 秒仍会失败**
   —— 区别是现在它会**准确**告诉你这是传输层超时（`TIMEOUT`），而不是"transport failed"。
2. **额外重试不写入 `llm/retry` 持久事件**（那是 `dsh-llm-retry` 的职责）。因此它不会出现在
   会话记录的"重试"标记里，只在宿主日志里留 warn。这是为了不改动 DSH 的持久化重试账本。
3. **归因依赖 Node 内置 undici 是否发布该诊断通道**；不发布时只是少一段括号说明。
4. 本插件只**读** `GenerateOptions`（agent loop 传入的是深冻结对象），只在终局失败分片上
   产出**新的**分片对象，不 patch 全局 `fetch`，不改任何其它插件的配置。

---

## 7. 可选搭配：直接放宽 provider 重试预算

你的日志里出现过"连续 6 次快速失败后整轮结束"。本插件的 C 部分已经在此基础上兜一层，
如果你想让 provider 自己的重试也更多，可以在同一个 patch 里调整（`retryPolicy` 属于
`dsh-llm-deepseek` 的配置字段）：

```yaml
- id: llm-deepseek-account        # 实际条目 id 以 profile 内为准
  name: "@deepseek-ai/dsh-llm-deepseek-account"
  config:
    retryPolicy:
      mode: normal
      maxRetries: 10
      retryableCodes: [EMPTY_RESPONSE, RATE_LIMIT, SERVER, TIMEOUT, TRANSPORT]
      initialDelayMs: 500
      maxDelayMs: 10000
      jitterRatio: 0.1
```

---

## 8. 取证工具

[`forensics/`](forensics/) 里有本次定位用的全部只读工具：ASAR 归档检查器、多帧 zstd
会话日志解码器、逐次尝试耗时统计、单会话事件时间线，以及真实 cordis 契约探针。
它们只读，不会改动 DSH 安装。用法见 [forensics/README.md](forensics/README.md)。
