// Measure the real duration of every failed model attempt in decoded session logs.
// Usage: node analyze-attempts.mjs [decodedSessionsDir]
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const dir = process.argv[2] ?? path.join(here, 'sessions');
const files = fs.readdirSync(dir).filter((f) => f.endsWith('.jsonl'));

const summary = new Map();
for (const file of files) {
  const lines = fs.readFileSync(path.join(dir, file), 'utf8').split('\n').filter((l) => l.trim());
  const events = [];
  for (const line of lines) {
    try {
      events.push(JSON.parse(line));
    } catch {}
  }
  console.log(`\n########## ${file.replace('.jsonl', '')} (${events.length} events) ##########`);
  let lastHeaderIdx = -1;
  for (let i = 0; i < events.length; i++) {
    const e = events[i];
    if (e.type === 'request/header') {
      lastHeaderIdx = i;
      continue;
    }
    if (e.type !== 'assistant/attempt') continue;
    const chunks = e.data?.stream ?? [];
    const finish = chunks.find((c) => c.chunk?.type === 'finish');
    const failure = finish?.chunk?.reason?.failure;
    if (failure === undefined) continue;
    const prev = events[i - 1];
    const header = events[lastHeaderIdx];
    const start = header?.time ?? undefined;
    const gapPrev = prev?.time !== undefined ? e.time - prev.time : null;
    const gapHeader = start !== undefined ? e.time - start : null;
    const streamed = chunks.filter((c) => c.chunk?.type !== 'finish').length;
    const key = failure.message;
    summary.set(key, (summary.get(key) ?? 0) + 1);
    console.log(
      `  seq=${String(e.seq).padStart(4)} turn=${e.data.turn} step=${e.data.step} ` +
        `sinceRequestHeader=${gapHeader === null ? 'n/a' : `${(gapHeader / 1000).toFixed(1)}s`} ` +
        `sincePrevEvent=${gapPrev === null ? 'n/a' : `${(gapPrev / 1000).toFixed(1)}s`} ` +
        `prevType=${prev?.type} streamedChunks=${streamed} :: ${failure.message} [${failure.code}]`,
    );
  }
}
console.log('\n===== failure message totals =====');
for (const [k, v] of [...summary.entries()].sort((a, b) => b[1] - a[1])) console.log(`  ${v}  ${k}`);
