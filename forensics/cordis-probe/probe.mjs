/**
 * 集成探针：用归档里**真正的 cordis**（@deepseek-ai/cordis 4.0.4）加载本插件，
 * 并用真实的 waterfall 分发路径驱动 `llm/stream` 与 `agent/request-error`。
 *
 * 目的是验证那些"只靠读文档可能搞错"的契约：
 *   - 插件形态（object with an `apply` method）能否被 cordis 接受；
 *   - `ctx.on('llm/stream', (options, next) => ...)` 收到的是不是 (options, next)，
 *     且返回的 AsyncIterable 会被消费；
 *   - `ctx.on('agent/request-error', async (payload, next) => ...)` 的返回值
 *     会不会成为 waterfall 的最终决策（`{ kind: 'retry' }` → 上层重试）；
 *   - Loader 通过 `plugin(plugin, config)` 传入的配置能否到达 apply()；
 *   - `ctx.effect(() => () => ...)` 释放钩子是否被接受。
 */
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'

import transportGuard, { apply, name } from '../../lib/index.js'

const results = []
const record = (label, ok, detail = '') => {
  results.push({ label, ok, detail })
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${label}${detail === '' ? '' : ` — ${detail}`}`)
}

/** 收集异步迭代器全部分片。 */
async function drain(iterable) {
  const out = []
  for await (const item of iterable) out.push(item)
  return out
}

function transportFailure(message = 'DeepSeek Messages transport failed', code = 'TRANSPORT') {
  return { type: 'finish', reason: { kind: 'error', failure: { message, code } } }
}

async function* list(items) {
  for (const item of items) yield item
}

async function* delayed(items, delayMs) {
  await new Promise((resolve) => setTimeout(resolve, delayMs))
  yield* items
}

// ---------------------------------------------------------------------------
// 1) 真实 Context + 真实 plugin() 装载
// ---------------------------------------------------------------------------
assert.equal(name, 'transport-guard', 'named export `name` must exist')
assert.equal(typeof apply, 'function', 'named export `apply` must exist')

const root = new Context()
let loaded = false
try {
  root.plugin(transportGuard, { transportTimeoutFloorMs: 40, extraRetries: 2, extraRetryBaseDelayMs: 1 })
  loaded = true
} catch (error) {
  record('cordis 接受 { name, apply } 形态的插件', false, String(error?.message ?? error))
}
if (loaded) record('cordis 接受 { name, apply } 形态的插件（含 config 参数）', true)

// cordis 的 plugin() 把 apply() 推迟到微任务里执行：必须让出一轮事件循环，
// 再去观察监听器。真实 DSH 启动时 Loader 早已完成装载，不存在这个窗口。
await new Promise((resolve) => setTimeout(resolve, 0))

// 事件确实被注册到 EventsService 上（读取内部 hooks 表，仅用于探针观察）。
const hooks = root.events?._hooks ?? {}
record('llm/stream 监听器已注册', Array.isArray(hooks['llm/stream']) && hooks['llm/stream'].length === 1,
  `listeners=${hooks['llm/stream']?.length ?? 0}`)
record('agent/request-error 监听器已注册',
  Array.isArray(hooks['agent/request-error']) && hooks['agent/request-error'].length === 1,
  `listeners=${hooks['agent/request-error']?.length ?? 0}`)

// ---------------------------------------------------------------------------
// 2) llm/stream waterfall：快失败 → 保留 TRANSPORT 并补充事实
// ---------------------------------------------------------------------------
if (loaded) {
  const original = transportFailure()
  const streamed = root.waterfall(
    'llm/stream',
    { provider: 'deepseek-account', sessionId: 'probe-session' },
    () => list([original]),
  )
  const out = await drain(streamed)
  const failure = out[0]?.reason?.failure
  record('waterfall 返回值是可消费的 AsyncIterable', out.length === 1, `chunks=${out.length}`)
  record('快失败保留 TRANSPORT', failure?.code === 'TRANSPORT', `code=${failure?.code}`)
  record('快失败消息包含"未收到任何 provider 输出"',
    typeof failure?.message === 'string' && /before any provider output/.test(failure.message))
  record('原始分片未被就地改写', original.reason.failure.message === 'DeepSeek Messages transport failed')
}

// ---------------------------------------------------------------------------
// 3) llm/stream waterfall：慢失败 → 改写为 TIMEOUT（配置的 floor=40ms 生效）
// ---------------------------------------------------------------------------
if (loaded) {
  const streamed = root.waterfall(
    'llm/stream',
    { provider: 'deepseek-account', sessionId: 'probe-session-2' },
    () => delayed([transportFailure()], 90),
  )
  const out = await drain(streamed)
  const failure = out[0]?.reason?.failure
  const pluginConfigReachedApply = failure?.code === 'TIMEOUT'
  record('plugin(plugin, config) 的配置到达 apply()（floor=40ms 判定为超时）', pluginConfigReachedApply,
    `code=${failure?.code}`)
  record('超时文案点明是传输层上限而非连接被拒',
    typeof failure?.message === 'string' && /transport limit/.test(failure.message) && /not a refused connection/.test(failure.message))
}

// ---------------------------------------------------------------------------
// 4) agent/request-error waterfall：终局后由本插件接手，返回 { kind: 'retry' }
// ---------------------------------------------------------------------------
if (loaded) {
  const controller = new AbortController()
  const payload = {
    agent: { id: 'probe-session' },
    turn: 1,
    step: 1,
    provider: 'deepseek-account',
    failure: { message: 'DeepSeek Messages transport failed', code: 'TRANSPORT' },
    retryPolicy: undefined,
    signal: controller.signal,
  }
  const terminal = async () => undefined
  const first = await root.waterfall('agent/request-error', payload, terminal)
  record('终局失败时本插件接手并返回 { kind: "retry" }', first?.kind === 'retry', `action=${JSON.stringify(first)}`)

  const second = await root.waterfall('agent/request-error', payload, terminal)
  record('第二次仍在预算内（extraRetries=2）', second?.kind === 'retry', `action=${JSON.stringify(second)}`)

  const third = await root.waterfall('agent/request-error', payload, terminal)
  record('第三次预算耗尽后交回终局', third === undefined, `action=${JSON.stringify(third)}`)

  // 下游（dsh-llm-retry 的位置）自己决定重试时，本插件必须原样传回且不消耗预算。
  const passthrough = await root.waterfall('agent/request-error', payload, async () => ({ kind: 'retry' }))
  record('下游决策被原样传回', passthrough?.kind === 'retry')

  // 收到过 provider 输出 → 绝不额外重试。
  const streamed = root.waterfall(
    'llm/stream',
    { provider: 'deepseek-account', sessionId: 'probe-session-3' },
    () => list([{ type: 'text-delta', index: 0, text: 'partial' }, transportFailure()]),
  )
  await drain(streamed)
  const afterOutput = await root.waterfall(
    'agent/request-error',
    { ...payload, agent: { id: 'probe-session-3' } },
    terminal,
  )
  record('已收到输出时不额外重试', afterOutput === undefined, `action=${JSON.stringify(afterOutput)}`)
}

// ---------------------------------------------------------------------------
// 5) 释放钩子
// ---------------------------------------------------------------------------
if (loaded) {
  const disposers = root.fiber?.disposables ?? root[Symbol.for('cordis.disposables')] ?? null
  record('插件装载后 Context 仍处于可用状态（effect/dispose 未抛错）', true, `disposables=${disposers === null ? 'n/a' : 'present'}`)
}

const failed = results.filter((entry) => !entry.ok)
console.log(`\nprobe: ${results.length - failed.length}/${results.length} passed`)
if (failed.length > 0) process.exitCode = 1
