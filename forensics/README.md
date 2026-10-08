# transport-forensics

定位 `DeepSeek Messages transport failed` 时用到的**只读**取证工具。它们不改动 DSH 安装、
不改动你的配置，只读取归档与会话日志。

## 为什么需要专门的工具

| 障碍 | 解决方式 |
| --- | --- |
| DSH 的运行时代码打包在 `resources/app.asar` 里，**不是**目录，`grep`/`ls` 直接失败 | `asar-scan.mjs` 自己解析 asar 头部，按需列目录 / 搜内容 / 取单文件 |
| 会话日志是 `session.v4.jsonl.zstd`，**多帧拼接**的 zstd；Node 的 `zstdDecompressSync` 与流式解压**都只吃第一帧**（实测 176 帧只解出 2 个事件） | `decode-sessions.mjs` 扫描帧魔数并逐帧校验解码，还原完整 jsonl |
| 报错只留 `{message, code}`，看不出每次尝试到底跑了多久 | `analyze-attempts.mjs` 统计每次失败尝试的耗时与已收分片数 |
| 需要看某一步前后的完整事件顺序 | `timeline.mjs` 输出单会话事件时间线 |

## 用法

```powershell
# 本目录（forensics/）的绝对路径；ASAR 路径按你的安装位置改
$F = "$PWD"
$A = 'D:\DSH\DeepSeek Harness\resources\app.asar'

# 1) 查看归档里的某个包（只读）
node "$F\asar-scan.mjs" $A list 'dsh-llm-deepseek'
node "$F\asar-scan.mjs" $A grep 'transport failed' 'dsh-llm'
node "$F\asar-scan.mjs" $A range 'dsh/node_modules/@deepseek-ai/dsh-llm-deepseek/lib/index.js' '2100:2140'
node "$F\asar-scan.mjs" $A extract '<asar 内路径>' 'C:\somewhere\out.js'

# 2) 解码会话日志（默认 $DSH_HOME/sessions -> 本目录/sessions）
node "$F\decode-sessions.mjs"

# 3) 统计每次失败尝试的真实耗时
node "$F\analyze-attempts.mjs"

# 4) 看某个会话的事件时间线（可按 seq 区间裁剪）
node "$F\timeline.mjs" 89e0bb43 96 110
```

> 第 2 步会在本目录下生成 `sessions/*.jsonl`：那是**会话日志的明文副本**（含你的提问与
> 模型输出）。本仓库出于隐私考虑**不保留**这些副本，需要时自行生成、用完自行删除。

## 本次定位的关键数据

- 解码 6 个会话（2437 → 会话仍在增长，最新一跑为 3000+ 事件），
  其中 `DeepSeek Messages transport failed` 出现 **329 次**；
- 每一次失败的 `sincePrevEvent` 都是 **0.1 秒**量级，`streamedChunks = 0`
  —— 即**还没收到任何 provider 输出就已失败**，不是 300 秒超时；
- 失败之后的重试经常在 0.5~1 秒内成功（`delivery-accepted` → `assistant/message`）；
- 同一批会话里还有 `web fetch failed: TypeError: fetch failed`，指向连接级瞬时故障。

## cordis-probe

`cordis-probe/` 用从归档里取出的**真实 `@deepseek-ai/cordis 4.0.4`**（+ `cosmokit`，
即 `node_modules/@deepseek-ai/` 下的原始副本）装载插件并真实分发 `waterfall`，
用来验证扩展点契约而不是"读文档猜"：

```powershell
node forensics\cordis-probe\probe.mjs
```

探针顺带确认了一个容易踩的事实：**`ctx.plugin()` 的 `apply()` 是推迟到微任务执行的**，
所以"调用 plugin() 之后同步立刻分发事件"会观察不到监听器——真实 DSH 启动时 Loader 早已
完成装载，不存在这个窗口。

重新生成 vendor 副本（若 DSH 升级）：

```powershell
$A = 'D:\DSH\DeepSeek Harness\resources\app.asar'
$B = 'forensics\cordis-probe\node_modules\@deepseek-ai'
foreach ($p in 'cordis','cosmokit') { New-Item -ItemType Directory -Force -Path "$B\$p\lib" | Out-Null }
node forensics\asar-scan.mjs $A extract 'dsh/node_modules/@deepseek-ai/cordis/package.json'   "$B\cordis\package.json"
node forensics\asar-scan.mjs $A extract 'dsh/node_modules/@deepseek-ai/cordis/lib/index.js'   "$B\cordis\lib\index.js"
node forensics\asar-scan.mjs $A extract 'dsh/node_modules/@deepseek-ai/cosmokit/package.json' "$B\cosmokit\package.json"
node forensics\asar-scan.mjs $A extract 'dsh/node_modules/@deepseek-ai/cosmokit/lib/index.js' "$B\cosmokit\lib\index.js"
```

> 这些 vendor 副本**不提交进仓库**（`.gitignore` 已忽略 `node_modules/`）：
> `cordis-probe` 需要真实的 DSH 安装才能运行，按上面的命令现场取出即可。
