/**
 * dsh-transport-guard 的行为测试。
 *
 * 覆盖三类断言：
 *   1. 分类：快失败 → 仍是 TRANSPORT 且说明"未收到任何输出"；
 *      贴到传输层上限的失败 → 改写为 TIMEOUT 且说明是传输层上限而不是连接被拒；
 *      适配器自己给出的 TIMEOUT（空闲看门狗）→ 不得被描述成连接级故障。
 *   2. 不变量：非网络族的失败分片原样透传（同一对象），成功分片原样透传，
 *      原始分片永不被就地改写。
 *   3. 自愈：额外重试受预算约束、尊重下游决策、尊重中止信号，
 *      且"已经收到过 provider 输出"时绝不重试。
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { apply, name } from '../lib/index.js'

/** 构造一个假的 Cordis 上下文，捕获插件注册的监听器与日志。 */
function harness(config) {
  const handlers = new Map()
  const logs = { info: [], warn: [] }
  const disposers = []
  const ctx = {
    logger: {
      info: (message) => logs.info.push(String(message)),
      warn: (message) => logs.warn.push(String(message)),
    },
    on(event, handler) {
      const list = handlers.get(event) ?? []
      list.push(handler)
      handlers.set(event, list)
      return () => {}
    },
    effect(setup) {
      const disposer = setup()
      if (typeof disposer === 'function') disposers.push(disposer)
    },
  }
  apply(ctx, config)
  return {
    logs,
    disposers,
    stream: handlers.get('llm/stream')?.[0],
    requestError: handlers.get('agent/request-error')?.[0],
  }
}

/** 依次产出的异步迭代器；`delayMs` 用来制造"耗时很长的尝试"。 */
async function* chunks(list, { delayMs = 0 } = {}) {
  for (const item of list) {
    if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs))
    yield item
  }
}

/** 收集一个流的全部分片。 */
async function drain(iterable) {
  const out = []
  for await (const item of iterable) out.push(item)
  return out
}

function transportFailure(message = 'DeepSeek Messages transport failed', code = 'TRANSPORT') {
  return { type: 'finish', reason: { kind: 'error', failure: { message, code } } }
}

const fastCall = (handler, context, source) => handler({ provider: 'deepseek-account', ...context }, () => source)

test('插件导出约定的 name 与 apply', () => {
  assert.equal(name, 'transport-guard')
  assert.equal(typeof apply, 'function')
})

test('快失败：保留 TRANSPORT，并在消息里写明"未收到任何 provider 输出"', async () => {
  const { stream } = harness()
  const original = transportFailure()
  const out = await drain(await fastCall(stream, { sessionId: 'session-1' }, chunks([original])))

  assert.equal(out.length, 1)
  assert.equal(out[0].reason.kind, 'error')
  assert.equal(out[0].reason.failure.code, 'TRANSPORT')
  assert.match(out[0].reason.failure.message, /before any provider output/)
  assert.match(out[0].reason.failure.message, /connection-level failure/)
  assert.match(out[0].reason.failure.message, /DeepSeek Messages transport failed/)

  // 原始分片绝不被就地改写。
  assert.equal(original.reason.failure.message, 'DeepSeek Messages transport failed')
  assert.notEqual(out[0], original)
  assert.notEqual(out[0].reason, original.reason)
})

test('慢失败：贴到传输层上限时改写为 TIMEOUT，并点明是 ~300s 传输上限而非连接被拒', async () => {
  const { stream, logs } = harness({ transportTimeoutFloorMs: 40 })
  const original = transportFailure()
  const out = await drain(
    await fastCall(stream, { sessionId: 'session-2' }, chunks([original], { delayMs: 90 })),
  )

  assert.equal(out[0].reason.failure.code, 'TIMEOUT')
  assert.match(out[0].reason.failure.message, /timed out after/)
  assert.match(out[0].reason.failure.message, /transport limit/)
  assert.match(out[0].reason.failure.message, /not a refused connection/)
  // 归因过程必须留下可查的警告日志。
  assert.ok(logs.warn.some((line) => /refined TRANSPORT -> TIMEOUT/.test(line)))
})

test('适配器自己的 TIMEOUT（空闲看门狗）不得被描述成连接级故障', async () => {
  const { stream } = harness()
  const original = transportFailure('DeepSeek Messages stream idle timeout', 'TIMEOUT')
  const out = await drain(await fastCall(stream, { sessionId: 'session-3' }, chunks([original])))

  assert.equal(out[0].reason.failure.code, 'TIMEOUT')
  assert.match(out[0].reason.failure.message, /idle watchdog/)
  assert.doesNotMatch(out[0].reason.failure.message, /connection-level/)
  assert.match(out[0].reason.failure.message, /DeepSeek Messages stream idle timeout/)
})

test('非网络族失败（如 AUTH）与成功分片都原样透传（同一对象）', async () => {
  const { stream } = harness()
  const auth = { type: 'finish', reason: { kind: 'error', failure: { message: 'bad key', code: 'AUTH', status: 401 } } }
  const stop = { type: 'finish', reason: { kind: 'stop' } }

  const authOut = await drain(await fastCall(stream, { sessionId: 's' }, chunks([auth])))
  assert.equal(authOut[0], auth)

  const stopOut = await drain(await fastCall(stream, { sessionId: 's' }, chunks([stop])))
  assert.equal(stopOut[0], stop)

  // 成功流也照旧把 delta 分片透传。
  const deltas = [{ type: 'text-delta', index: 0, text: 'hi' }]
  const okOut = await drain(await fastCall(stream, { sessionId: 's' }, chunks([...deltas, stop])))
  assert.equal(okOut.length, 2)
  assert.equal(okOut[0], deltas[0])
})

test('额外重试：预算用尽后交回终局决策', async () => {
  const { requestError, logs } = harness({ extraRetries: 2, extraRetryBaseDelayMs: 1, extraRetryMaxDelayMs: 2 })
  const payload = {
    agent: { id: 'session-10' },
    turn: 1,
    step: 2,
    provider: 'deepseek-account',
    failure: { message: 'DeepSeek Messages transport failed', code: 'TRANSPORT' },
    signal: new AbortController().signal,
  }
  const terminal = async () => undefined

  assert.deepEqual(await requestError(payload, terminal), { kind: 'retry' })
  assert.deepEqual(await requestError(payload, terminal), { kind: 'retry' })
  assert.equal(await requestError(payload, terminal), undefined)
  assert.ok(logs.warn.some((line) => /extra retry budget exhausted/.test(line)))
})

test('额外重试：下游已经接手时不重复计数，也不抢决策', async () => {
  const { requestError } = harness({ extraRetries: 1, extraRetryBaseDelayMs: 1 })
  const payload = {
    agent: { id: 'session-11' },
    turn: 3,
    step: 1,
    provider: 'deepseek-account',
    failure: { message: 'x', code: 'TRANSPORT' },
    signal: new AbortController().signal,
  }
  // 下游（dsh-llm-retry）自己重试：原样传回，且不消耗本插件预算。
  assert.deepEqual(await requestError(payload, async () => ({ kind: 'retry' })), { kind: 'retry' })
  // 下游放弃后，本插件仍有一次预算可用。
  assert.deepEqual(await requestError(payload, async () => undefined), { kind: 'retry' })
  assert.equal(await requestError(payload, async () => undefined), undefined)
})

test('额外重试：已经收到过 provider 输出时不重试（避免丢失已生成内容）', async () => {
  const { stream, requestError } = harness({ extraRetries: 3, extraRetryBaseDelayMs: 1 })
  const failure = transportFailure()
  const out = await drain(
    await fastCall(
      stream,
      { sessionId: 'session-12' },
      chunks([{ type: 'text-delta', index: 0, text: 'partial' }, failure]),
    ),
  )
  assert.equal(out.length, 2)

  const payload = {
    agent: { id: 'session-12' },
    turn: 1,
    step: 1,
    provider: 'deepseek-account',
    failure: { message: 'DeepSeek Messages transport failed', code: 'TRANSPORT' },
    signal: new AbortController().signal,
  }
  assert.equal(await requestError(payload, async () => undefined), undefined)
})

test('额外重试：中止信号与不可重试的 code 都被尊重', async () => {
  const { requestError } = harness({ extraRetries: 3, extraRetryBaseDelayMs: 1 })
  const controller = new AbortController()
  controller.abort()
  const base = {
    agent: { id: 'session-13' },
    turn: 1,
    step: 1,
    provider: 'deepseek-account',
    signal: controller.signal,
  }

  assert.equal(
    await requestError({ ...base, failure: { message: 'x', code: 'TRANSPORT' } }, async () => undefined),
    undefined,
  )
  assert.equal(
    await requestError(
      { ...base, signal: new AbortController().signal, failure: { message: 'x', code: 'AUTH' } },
      async () => undefined,
    ),
    undefined,
  )
})

test('配置：非法值回退默认值并告警，enabled=false 时不注册任何监听器', () => {
  const bad = harness({ extraRetries: -3, transportTimeoutFloorMs: 'soon', providers: 'deepseek' })
  assert.ok(bad.logs.warn.some((line) => /ignoring invalid "extraRetries"/.test(line)))
  assert.ok(bad.logs.warn.some((line) => /ignoring invalid "transportTimeoutFloorMs"/.test(line)))
  assert.ok(bad.logs.warn.some((line) => /ignoring invalid "providers"/.test(line)))

  const off = harness({ enabled: false })
  assert.equal(off.stream, undefined)
  assert.equal(off.requestError, undefined)
  assert.ok(off.logs.info.some((line) => /disabled by config/.test(line)))
})

test('配置：providers 白名单只放行列出的 provider', async () => {
  const { stream } = harness({ providers: ['deepseek-account'] })
  const other = transportFailure()
  const out = await drain(await stream({ provider: 'some-other-provider' }, () => chunks([other])))
  assert.equal(out[0], other)
})

test('中文文案：timeout 与连接级故障各自有清晰说法', async () => {
  const { stream } = harness({ transportTimeoutFloorMs: 30, messageLanguage: 'zh' })
  const slow = await drain(
    await fastCall(stream, { sessionId: 'zh-1' }, chunks([transportFailure()], { delayMs: 70 })),
  )
  assert.equal(slow[0].reason.failure.code, 'TIMEOUT')
  assert.match(slow[0].reason.failure.message, /传输层的 ~300s 上限/)

  const fast = await drain(await stream({ provider: 'deepseek-account' }, () => chunks([transportFailure()])))
  assert.match(fast[0].reason.failure.message, /属连接级故障/)
})
