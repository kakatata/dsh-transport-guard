// Compact event timeline for one decoded session, to see exact per-attempt durations.
// Usage: node timeline.mjs <sessionIdSubstring> [fromSeq] [toSeq] [decodedSessionsDir]
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const dir = process.argv[5] ?? path.join(here, 'sessions');
const match = process.argv[2] ?? '1fd7fa8c';
const fromSeq = Number(process.argv[3] ?? 0);
const toSeq = Number(process.argv[4] ?? 1e9);

const file = fs.readdirSync(dir).find((f) => f.includes(match));
if (!file) {
  console.error(`no session matching ${match}`);
  process.exit(1);
}
const events = fs
  .readFileSync(path.join(dir, file), 'utf8')
  .split('\n')
  .filter((l) => l.trim())
  .map((l) => {
    try {
      return JSON.parse(l);
    } catch {
      return null;
    }
  })
  .filter(Boolean);

console.log(`### ${file}  (${events.length} events)`);
let prevTime = null;
for (const e of events) {
  if (e.seq < fromSeq || e.seq > toSeq) {
    prevTime = e.time ?? prevTime;
    continue;
  }
  const delta = prevTime === null ? '' : `${String(((e.time - prevTime) / 1000).toFixed(2)).padStart(8)}s`;
  let detail = '';
  if (e.type === 'assistant/attempt') {
    const chunks = (e.data?.stream ?? []).map((c) => {
      if (c.chunk?.type === 'finish') {
        const r = c.chunk.reason;
        return r.kind === 'error' ? `finish:error(${r.failure.code})` : `finish:${r.kind}`;
      }
      return c.chunk?.type ?? '?';
    });
    detail = `chunks=[${chunks.join(',')}]`;
  } else if (e.type === 'llm/retry') {
    detail = `retry=${e.data.retry}/${e.data.maxRetries} delay=${Math.round(e.data.delayMs)}ms`;
  } else if (e.type === 'request/header') {
    const c = e.data?.header?.config ?? {};
    detail = `model=${c.model} effort=${c.reasoningEffort} maxTokens=${c.maxTokens}`;
  } else if (e.type === 'assistant/message') {
    const kinds = (e.data?.message?.content ?? []).map((b) => b.type);
    detail = `blocks=[${kinds.join(',')}]`;
  } else if (e.type === 'step/start' || e.type === 'step/end') {
    detail = `turn=${e.data?.turn} step=${e.data?.step}`;
  } else if (e.type === 'turn/end') {
    detail = JSON.stringify(e.data?.reason)?.slice(0, 160) ?? '';
  } else if (e.type === 'tool/call') {
    detail = `name=${e.data?.name}`;
  } else if (e.type === 'tool/result') {
    detail = `isError=${e.data?.message?.isError ?? false}`;
  }
  console.log(`seq=${String(e.seq).padStart(4)} ${delta} ${String(e.type).padEnd(20)} ${detail}`);
  prevTime = e.time ?? prevTime;
}
