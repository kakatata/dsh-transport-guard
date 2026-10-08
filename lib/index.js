/**
 * dsh-transport-guard —— 让 `DeepSeek Messages transport failed` 不再是一句无从下手的兜底报错
 *
 * ## 为什么需要它（实测证据，不是猜测）
 *
 * 解码 `.dsh/sessions/<id>/session.v4.jsonl.zstd` 后统计全部失败尝试：
 *
 *   - 每一次失败都在 **0.06–0.3 秒**内发生，且 `streamedChunks = 0`（provider 一个字节都没发）；
 *   - 紧随其后的重试往往立刻成功（`delivery-accepted` → `assistant/message`）；
 *   - 没有任何一次尝试真的等待过 ~300 秒；
 *   - 同一批会话里 `web fetch failed: TypeError: fetch failed` 同时出现（连接级故障）。
 *
 * 也就是说 `TRANSPORT` 这一句话同时覆盖了两种性质完全不同的故障：
 *
 *   1. **连接级瞬时故障** —— 复用的 keep-alive 连接已被对端关闭 / 网络抖动，立刻失败；
 *   2. **传输层的 ~300 秒上限** —— undici fetch 的默认 `headersTimeout` / `bodyTimeout`
 *      都是 300_000 ms（Node 内置 dispatcher 与 `dsh-http-proxy` 安装的 `new Agent()` 同值）。
 *
 * 而 `@deepseek-ai/dsh-llm-deepseek` 的兜底分支
 * (`throw new LlmError("DeepSeek Messages transport failed", "TRANSPORT", { cause })`)
 * 会把原始 `cause` 丢掉，`LlmFailure` 只留 `{ message, code }`，于是现场无从区分。
 *
 * 更微妙的是：DSH 自己的空闲看门狗 `streamIdleTimeoutMs` **默认也是 300_000 ms**，
 * 与 undici 的 300 秒处在同一量级，两者是一条**竞态**：
 *
 *   - 看门狗先到 → `TIMEOUT`（正确，消息写明 "stream idle timeout"）；
 *   - undici 先到 → `TRANSPORT`（**误报**，正是用户看到的这句话）。
 *
 * ## 本插件做三件事
 *
 *   A. **分类（根治误报）**：在 `llm/stream` 瀑布里为每次尝试计时。耗时越过阈值
 *      （`transportTimeoutFloorMs`，默认 290 s，刻意贴在 300 s 之下）就说明触发的是
 *      传输/静默上限，于是把 code 改写为 `TIMEOUT` 并在消息里写明真实耗时；否则保留
 *      `TRANSPORT`，但把"0.2 秒内失败、没有任何 provider 输出"这个事实写进消息。
 *      —— 无论 300 秒竞态谁赢，用户看到的信息都是准确的。
 *
 *   B. **归因**：尽力订阅 Node 的 `undici:request:error` 诊断通道，把真正的底层错误
 *      （`ECONNRESET` / `UND_ERR_HEADERS_TIMEOUT` / `other side closed` ...）按**时间窗口**
 *      关联到这次尝试，附在消息上。通道不可用就静默降级，不影响主流程。
 *
 *   C. **自愈**：在 `agent/request-error` 瀑布里，当 provider 自带的重试策略（`dsh-llm-retry`）
 *      已经耗尽预算、而失败属于"尚未收到任何输出"的连接级故障时，额外再给有限次重试。
 *      避免一个几百毫秒的网络抖动直接终结整轮对话。
 *
 * ## 不变量（为什么它是安全的）
 *
 *   - 只**读** `GenerateOptions`（agent loop 传入的是深冻结对象），绝不改写；
 *   - 只在终局失败分片上产出**新的**分片对象，不改写原对象；
 *   - 不改任何其他插件的配置，不替换 undici dispatcher，不 patch 全局 fetch；
 *   - 不引入任何运行时依赖（只用 Node 内建能力），因此不会因为依赖解析失败而拖垮启动；
 *   - 任何辅助能力（诊断通道、日志、计时）出错都被就地吞掉，主流程照旧。
 *
 * @module dsh-transport-guard
 */

/** 插件名（Loader 日志与报错里显示的名字）。 */
const name = 'transport-guard'

/**
 * undici 传输层的默认上限：`headersTimeout` 与 `bodyTimeout` 都是 300 秒。
 * 这里只作为**默认阈值**与文案依据，不做任何断言——真实判断靠实测耗时。
 */
const UNDICI_DEFAULT_TRANSPORT_LIMIT_MS = 300_000

/**
 * 判定"这次失败其实是超时"的默认耗时下限。
 * 刻意贴着 300 秒但略低于它：留出计时误差与 undici 提前一点触发的空间。
 */
const DEFAULT_TIMEOUT_FLOOR_MS = 290_000

/** 会被本插件改写消息的 code（"网络/提供方那边断了"这一族）。 */
const TRANSPORT_FAMILY_CODES = new Set(['TRANSPORT', 'TIMEOUT', 'STREAM_CLOSED'])

/** 允许额外重试的 code：都属于"这次尝试没拿到任何可用输出"。 */
const EXTRA_RETRY_CODES = new Set(['TRANSPORT', 'TIMEOUT', 'STREAM_CLOSED'])

/** 尝试统计的保鲜期：`agent/request-error` 只认刚刚结束的那次尝试。 */
const ATTEMPT_STATS_TTL_MS = 15_000

/** 诊断通道环形缓冲上限。 */
const DIAGNOSTIC_RING_SIZE = 24

/** 额外重试预算表的条目上限（防止长期运行下 Map 无界增长）。 */
const EXTRA_BUDGET_ENTRIES = 512

const CONFIG_DEFAULTS = Object.freeze({
  enabled: true,
  /** 耗时不小于它 → 归因为传输层 ~300 秒上限。 */
  transportTimeoutFloorMs: DEFAULT_TIMEOUT_FLOOR_MS,
  /** 是否把这类 TRANSPORT 的 code 改写成 TIMEOUT。 */
  reclassifyTimeouts: true,
  /** 是否给失败消息补充耗时/阶段/底层原因。 */
  enrichMessages: true,
  /** 失败消息的语言：'en' 与 DSH 自身文案一致，'zh' 便于自己排查。 */
  messageLanguage: 'en',
  /** 在 provider 自带重试策略之外，额外允许的重试次数（0 = 关闭自愈）。 */
  extraRetries: 4,
  extraRetryBaseDelayMs: 750,
  extraRetryMaxDelayMs: 8_000,
  /** 只在"这次尝试尚未收到任何 provider 输出"时额外重试。 */
  onlyBeforeFirstChunk: true,
  /** 只对这些 provider 生效；空数组 = 全部生效。 */
  providers: [],
  /** 是否订阅 undici 诊断通道做归因。 */
  diagnoseTransport: true,
})

/** 读取内建模块，拿不到就返回 null（绝不抛）。 */
function builtinModule(id) {
  try {
    if (typeof process.getBuiltinModule === 'function') return process.getBuiltinModule(id) ?? null
  } catch {
    /* 忽略：拿不到就当能力不可用 */
  }
  return null
}

/** 有限、正数才接受，否则回退默认值。 */
function positiveNumber(value, fallback, { min = Number.MIN_VALUE, max = Number.MAX_SAFE_INTEGER } = {}) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return fallback
  return Math.min(Math.max(value, min), max)
}

/** 非负整数才接受，否则回退默认值。 */
function nonNegativeInt(value, fallback, max = 100) {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) return fallback
  return Math.min(value, max)
}

/**
 * 校验并归一化插件配置。
 * 配置来源可能是 Loader 的 patch `config:`，也可能是手写的 patch —— 一律防御式处理，
 * 任何非法值都回退默认值并留一条 warn，绝不让配置问题阻断插件加载。
 * @param raw - `apply()` 收到的原始配置。
 * @param warn - 记录非法配置的告警函数。
 * @returns 归一化后的完整配置。
 */
function resolveConfig(raw, warn) {
  const input = raw !== null && typeof raw === 'object' && !Array.isArray(raw) ? raw : {}
  const cfg = { ...CONFIG_DEFAULTS }
  const boolKeys = ['enabled', 'reclassifyTimeouts', 'enrichMessages', 'onlyBeforeFirstChunk', 'diagnoseTransport']
  for (const key of boolKeys) {
    if (input[key] === undefined) continue
    if (typeof input[key] === 'boolean') cfg[key] = input[key]
    else warn(`ignoring non-boolean "${key}"`)
  }
  if (input.transportTimeoutFloorMs !== undefined) {
    const value = positiveNumber(input.transportTimeoutFloorMs, Number.NaN, { min: 1, max: 2_147_483_647 })
    if (Number.isFinite(value)) cfg.transportTimeoutFloorMs = value
    else warn('ignoring invalid "transportTimeoutFloorMs"')
  }
  if (input.extraRetries !== undefined) {
    const value = nonNegativeInt(input.extraRetries, -1, 50)
    if (value >= 0) cfg.extraRetries = value
    else warn('ignoring invalid "extraRetries"')
  }
  if (input.extraRetryBaseDelayMs !== undefined) {
    const value = positiveNumber(input.extraRetryBaseDelayMs, Number.NaN, { min: 1, max: 600_000 })
    if (Number.isFinite(value)) cfg.extraRetryBaseDelayMs = value
    else warn('ignoring invalid "extraRetryBaseDelayMs"')
  }
  if (input.extraRetryMaxDelayMs !== undefined) {
    const value = positiveNumber(input.extraRetryMaxDelayMs, Number.NaN, { min: 1, max: 3_600_000 })
    if (Number.isFinite(value)) cfg.extraRetryMaxDelayMs = value
    else warn('ignoring invalid "extraRetryMaxDelayMs"')
  }
  if (cfg.extraRetryMaxDelayMs < cfg.extraRetryBaseDelayMs) cfg.extraRetryMaxDelayMs = cfg.extraRetryBaseDelayMs
  if (input.messageLanguage !== undefined) {
    if (input.messageLanguage === 'en' || input.messageLanguage === 'zh') cfg.messageLanguage = input.messageLanguage
    else warn('ignoring invalid "messageLanguage" (expected "en" or "zh")')
  }
  if (input.providers !== undefined) {
    if (Array.isArray(input.providers) && input.providers.every((entry) => typeof entry === 'string')) {
      cfg.providers = [...input.providers]
    } else warn('ignoring invalid "providers" (expected an array of strings)')
  }
  return cfg
}

/** 把毫秒格式化成人能读的秒（保留 3 位有效数字，避免 300.00000001 这种噪声）。 */
function formatSeconds(ms) {
  const seconds = ms / 1000
  const text = seconds >= 100 ? seconds.toFixed(1) : seconds.toFixed(2)
  return `${text.replace(/\.?0+$/u, '')}s`
}

/**
 * 生成补充到失败消息后面的诊断后缀。
 *
 * 三种情形必须说清楚，不能混为一谈：
 *   - `transportLimit`：耗时贴到传输层上限，触发的是 undici 的 ~300s（不是连接被拒）；
 *   - `timeoutLike` 但没有 `transportLimit`：DSH 自己的空闲看门狗超时（其长度由
 *     `streamIdleTimeoutMs` 决定，未必是 300s）；
 *   - 其余：连接级故障，并说明是否已经收到过 provider 输出。
 *
 * @param info - 本次尝试的耗时、输出量、分类结果、底层原因与语言。
 * @returns 以空格开头的短后缀（可直接拼接到原消息后面）。
 */
function describeSuffix(info) {
  const { elapsedMs, streamedChunks, timeoutLike, transportLimit, cause, language } = info
  const elapsed = formatSeconds(elapsedMs)
  const causeText = cause === undefined ? '' : ` (${cause})`
  const limitSeconds = UNDICI_DEFAULT_TRANSPORT_LIMIT_MS / 1000
  if (language === 'zh') {
    if (timeoutLike) {
      const note = transportLimit
        ? `——这是传输层的 ~${limitSeconds}s 上限，不是连接被拒`
        : '（DSH 空闲看门狗）'
      return ` [transport-guard: 静默 ${elapsed} 后超时${note}]${causeText}`
    }
    const produced = streamedChunks > 0
      ? `已收到 ${streamedChunks} 个分片后中断`
      : '在收到任何 provider 输出之前就失败'
    return ` [transport-guard: ${elapsed} 内失败，${produced}，属连接级故障]${causeText}`
  }
  if (timeoutLike) {
    const note = transportLimit
      ? ` — this is the ~${limitSeconds}s transport limit, not a refused connection`
      : ' (idle watchdog)'
    return ` [transport-guard: timed out after ${elapsed} of provider silence${note}]${causeText}`
  }
  const produced = streamedChunks > 0
    ? `after ${streamedChunks} streamed chunk(s)`
    : 'before any provider output'
  return ` [transport-guard: failed within ${elapsed}, ${produced} — connection-level failure]${causeText}`
}

/**
 * 订阅 undici 的请求错误诊断通道，按时间窗口为后续失败提供底层归因。
 * 通道缺失、订阅失败都属于正常情况（例如 fetch 并非 undici 实现），静默降级。
 * @param enabled - 配置开关。
 * @param warn - 告警函数。
 * @returns 取消订阅函数、以及查询函数。
 */
function createTransportDiagnostics(enabled, warn) {
  const ring = []
  let unsubscribe = null
  if (enabled) {
    try {
      const diagnostics = builtinModule('node:diagnostics_channel')
      if (diagnostics !== null && typeof diagnostics.subscribe === 'function') {
        const listener = (message) => {
          try {
            const error = message?.error
            const entry = {
              at: Date.now(),
              origin: typeof message?.request?.origin === 'string' ? message.request.origin : undefined,
              code: typeof error?.code === 'string' ? error.code : typeof error?.name === 'string' ? error.name : undefined,
              message: typeof error?.message === 'string' ? error.message : undefined,
            }
            ring.push(entry)
            if (ring.length > DIAGNOSTIC_RING_SIZE) ring.shift()
          } catch {
            /* 诊断失败永不影响主流程 */
          }
        }
        diagnostics.subscribe('undici:request:error', listener)
        unsubscribe = () => {
          try {
            diagnostics.unsubscribe('undici:request:error', listener)
          } catch {
            /* 忽略 */
          }
        }
      }
    } catch (error) {
      warn(`undici diagnostics unavailable: ${String(error?.message ?? error)}`)
    }
  }
  /**
   * 在给定时间窗内找最后一条底层错误，用于这次尝试的归因。
   * @param windowStart - 尝试开始的墙钟时间。
   * @param windowEnd - 尝试结束的墙钟时间。
   * @returns 形如 `ECONNRESET: other side closed` 的短描述，或 undefined。
   */
  const lookup = (windowStart, windowEnd) => {
    for (let index = ring.length - 1; index >= 0; index -= 1) {
      const entry = ring[index]
      if (entry.at < windowStart - 1000 || entry.at > windowEnd + 250) continue
      const parts = [entry.code, entry.message].filter((part) => typeof part === 'string' && part !== '')
      if (parts.length === 0) continue
      const text = parts.join(': ')
      return text.length > 160 ? `${text.slice(0, 157)}...` : text
    }
    return undefined
  }
  return { unsubscribe, lookup }
}

/** 可取消的等待；signal 已中止时立刻返回 false。 */
function cancellableDelay(delayMs, signal) {
  if (signal?.aborted === true) return Promise.resolve(false)
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener?.('abort', onAbort)
      resolve(true)
    }, delayMs)
    function onAbort() {
      clearTimeout(timer)
      resolve(false)
    }
    signal?.addEventListener?.('abort', onAbort, { once: true })
  })
}

/** 有界 Map 写入：超上限时按插入顺序淘汰最旧的一半。 */
function boundedSet(map, key, value, limit) {
  if (map.size >= limit && !map.has(key)) {
    let drop = Math.ceil(limit / 2)
    for (const existing of map.keys()) {
      map.delete(existing)
      drop -= 1
      if (drop <= 0) break
    }
  }
  map.set(key, value)
}

/**
 * 判断一次终局失败是否值得改写，并产出新的 `LlmFailure`。
 * @param failure - 适配器给出的 `LlmFailure`（`{ message, code, ... }`）。
 * @param info - 本次尝试的耗时、输出量与归因信息。
 * @param cfg - 归一化配置。
 * @returns 原对象（无需改写时）或新的失败对象。
 */
function refineFailure(failure, info, cfg) {
  if (failure === null || typeof failure !== 'object') return failure
  const code = typeof failure.code === 'string' ? failure.code : ''
  if (!TRANSPORT_FAMILY_CODES.has(code)) return failure

  const elapsedMs = info.elapsedMs
  // 只有"耗时确实贴到传输层上限"时才归因为传输层上限；短失败一律当连接级故障处理。
  // 注意：适配器自己的空闲看门狗会直接给出 TIMEOUT，那条路径不能描述成连接级故障。
  const transportLimit = code !== 'TIMEOUT' && elapsedMs >= cfg.transportTimeoutFloorMs
  const timeoutLike = code === 'TIMEOUT' || transportLimit
  const nextCode = transportLimit && cfg.reclassifyTimeouts ? 'TIMEOUT' : code
  if (!cfg.enrichMessages && nextCode === code) return failure

  const message = typeof failure.message === 'string' && failure.message !== '' ? failure.message : '(no message)'
  const suffix = cfg.enrichMessages
    ? describeSuffix({
        elapsedMs,
        streamedChunks: info.streamedChunks,
        timeoutLike,
        transportLimit,
        cause: info.cause,
        language: cfg.messageLanguage,
      })
    : ''
  return { ...failure, code: nextCode, message: `${message}${suffix}` }
}

/**
 * 把一次模型调用包成"计时 + 分类"的迭代器。
 * @param source - `next()` 给出的适配器流。
 * @param context - provider、sessionId、purpose 等只读上下文。
 * @param cfg - 归一化配置。
 * @param hooks - 记录尝试结果与日志的回调。
 * @returns 与原流等价的异步迭代器（失败的 finish 分片可能被改写）。
 */
function guardStream(source, context, cfg, hooks) {
  const startedAt = Date.now()
  let streamedChunks = 0
  return (async function* guarded() {
    try {
      for await (const chunk of source) {
        if (chunk !== null && typeof chunk === 'object' && chunk.type === 'finish') {
          const finishedAt = Date.now()
          const elapsedMs = finishedAt - startedAt
          const reason = chunk.reason
          const failure = reason !== null && typeof reason === 'object' ? reason.failure : undefined
          if (failure !== null && typeof failure === 'object') {
            const cause = cfg.diagnoseTransport ? hooks.lookupCause(startedAt, finishedAt) : undefined
            const refined = refineFailure(failure, { elapsedMs, streamedChunks, cause }, cfg)
            hooks.recordAttempt(context, {
              at: finishedAt,
              elapsedMs,
              streamedChunks,
              code: refined?.code,
              message: refined?.message,
              purpose: context.purpose,
            })
            if (refined !== failure) {
              hooks.onRefined({ context, before: failure, after: refined, elapsedMs, streamedChunks, cause })
              yield { ...chunk, reason: { ...reason, failure: refined } }
              return
            }
          } else {
            hooks.recordAttempt(context, {
              at: finishedAt,
              elapsedMs,
              streamedChunks,
              code: undefined,
              purpose: context.purpose,
            })
          }
          yield chunk
          return
        }
        streamedChunks += 1
        yield chunk
      }
    } catch (error) {
      // 抛出的失败（中间件/清理/消费方错误）不由本插件接管：记录后原样抛出。
      hooks.recordAttempt(context, {
        at: Date.now(),
        elapsedMs: Date.now() - startedAt,
        streamedChunks,
        code: typeof error?.code === 'string' ? error.code : undefined,
        purpose: context.purpose,
        thrown: true,
      })
      throw error
    }
  })()
}

/**
 * 插件入口。
 * @param ctx - 插件上下文（Cordis）。刻意不声明 `inject`：本插件不等待任何服务，
 *   `apply()` 立即注册监听器，避免"服务永远不就绪导致整个插件不 apply"的老问题。
 * @param rawConfig - Loader 传入的配置（可缺省）。
 */
function apply(ctx, rawConfig = {}) {
  const logger = ctx?.logger ?? {}
  const warn = (message) => {
    try {
      if (typeof logger.warn === 'function') logger.warn(`transport-guard: ${message}`)
    } catch {
      /* 日志失败不影响主流程 */
    }
  }
  const info = (message) => {
    try {
      if (typeof logger.info === 'function') logger.info(`transport-guard: ${message}`)
    } catch {
      /* 忽略 */
    }
  }

  const cfg = resolveConfig(rawConfig, warn)
  if (!cfg.enabled) {
    info('disabled by config')
    return
  }

  const diagnostics = createTransportDiagnostics(cfg.diagnoseTransport, warn)
  /** 最近一次尝试的统计（键：`sessionId|provider`），供 `agent/request-error` 查询。 */
  const attempts = new Map()
  /** 每个 `agent|turn|step` 已经用掉的额外重试次数。 */
  const extraBudget = new Map()

  const providerAllowed = (provider) => cfg.providers.length === 0 || cfg.providers.includes(provider)

  ctx.on('llm/stream', (options, next) => {
    // `next()` 同步返回解析后的适配器流；它若抛错就原样向上抛，交由上层分类。
    const source = next()
    if (source === null || typeof source !== 'object' || typeof source[Symbol.asyncIterator] !== 'function') {
      return source
    }
    const provider = typeof options?.provider === 'string' ? options.provider : ''
    if (!providerAllowed(provider)) return source
    const context = {
      provider,
      sessionId: options?.sessionId === undefined ? undefined : String(options.sessionId),
      purpose: options?.purpose,
    }
    try {
      return guardStream(source, context, cfg, {
        lookupCause: diagnostics.lookup,
        recordAttempt: (attemptContext, stats) => {
          if (attemptContext.purpose === 'session-title') return
          boundedSet(attempts, `${attemptContext.sessionId ?? ''}|${attemptContext.provider}`, stats, EXTRA_BUDGET_ENTRIES)
        },
        onRefined: ({ before, after, elapsedMs, streamedChunks, cause }) => {
          warn(
            `refined ${before.code} -> ${after.code} after ${formatSeconds(elapsedMs)} ` +
              `(streamedChunks=${streamedChunks}${cause === undefined ? '' : `, cause=${cause}`}): ${after.message}`,
          )
        },
      })
    } catch (error) {
      warn(`stream guard unavailable: ${String(error?.message ?? error)}`)
      return source
    }
  })

  ctx.on('agent/request-error', async (payload, next) => {
    // 先让下游（含 dsh-llm-retry 的正常重试策略）表态；它与本插件的注册顺序无关：
    // 若它在更外层，这里拿到的是它回传的决策；若在更内层，则只有它放弃时才轮到这里。
    const decision = await next()
    if (decision !== undefined) return decision
    if (!providerAllowed(payload?.provider ?? '')) return undefined
    if (cfg.extraRetries === 0) return undefined
    if (payload?.signal?.aborted === true) return undefined

    const failure = payload?.failure
    const code = typeof failure?.code === 'string' ? failure.code : ''
    if (!EXTRA_RETRY_CODES.has(code)) return undefined

    const sessionId = payload?.agent?.id === undefined ? '' : String(payload.agent.id)
    const stats = attempts.get(`${sessionId}|${payload?.provider ?? ''}`)
    const fresh = stats !== undefined && Date.now() - stats.at <= ATTEMPT_STATS_TTL_MS
    if (cfg.onlyBeforeFirstChunk) {
      // 没有新鲜统计（例如失败发生在流建立之前）时，按"尚未收到输出"处理：
      // 此时确实没有任何 provider 内容，重试不会丢失已生成的内容。
      if (fresh && stats.streamedChunks > 0) return undefined
      if (fresh && stats.thrown === true) return undefined
    }
    if (fresh && stats.purpose === 'session-title') return undefined

    const budgetKey = `${sessionId}|${payload?.turn ?? -1}|${payload?.step ?? -1}`
    const used = extraBudget.get(budgetKey) ?? 0
    if (used >= cfg.extraRetries) {
      warn(
        `extra retry budget exhausted (${used}/${cfg.extraRetries}) for turn ${payload?.turn} step ${payload?.step}; ` +
          `the failure stays terminal: ${typeof failure?.message === 'string' ? failure.message : code}`,
      )
      return undefined
    }
    const delayMs = Math.min(cfg.extraRetryBaseDelayMs * 2 ** used, cfg.extraRetryMaxDelayMs)
    boundedSet(extraBudget, budgetKey, used + 1, EXTRA_BUDGET_ENTRIES)
    const proceeded = await cancellableDelay(delayMs, payload?.signal)
    if (!proceeded) return undefined
    warn(
      `granting extra retry ${used + 1}/${cfg.extraRetries} for turn ${payload?.turn} step ${payload?.step} ` +
        `after ${code}${fresh ? ` (attempt lasted ${formatSeconds(stats.elapsedMs)}, streamedChunks=${stats.streamedChunks})` : ''}`,
    )
    return { kind: 'retry' }
  })

  // 释放：取消诊断通道订阅。监听器本身随 fiber 一起销毁。
  try {
    if (typeof ctx.effect === 'function') ctx.effect(() => () => diagnostics.unsubscribe?.())
  } catch (error) {
    warn(`dispose hook unavailable: ${String(error?.message ?? error)}`)
  }

  info(
    `active (timeoutFloor=${formatSeconds(cfg.transportTimeoutFloorMs)}, extraRetries=${cfg.extraRetries}, ` +
      `onlyBeforeFirstChunk=${cfg.onlyBeforeFirstChunk}, diagnose=${cfg.diagnoseTransport})`,
  )
}

export { apply, name }
export default { name, apply }
