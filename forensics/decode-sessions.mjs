// Decode DSH session logs (concatenated zstd frames) into plain jsonl files.
// Node's zstd API decodes only the first frame, so frames are located by magic
// and each candidate offset is validated by attempting a real decode.
//
// Usage: node decode-sessions.mjs [sessionsRoot] [outputDir]
// Defaults: $DSH_HOME/sessions  ->  <this script's directory>/sessions
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const dshHome = process.env.DSH_HOME ?? path.join(os.homedir(), '.dsh');
const root = process.argv[2] ?? path.join(dshHome, 'sessions');
const outDir = process.argv[3] ?? path.join(here, 'sessions');
const MAGIC = [0x28, 0xb5, 0x2f, 0xfd];

function* walk(dir) {
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return;
  }
  for (const e of entries) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) yield* walk(p);
    else if (/\.zstd$/i.test(e.name)) yield p;
  }
}

function decodeFrames(file) {
  const buf = fs.readFileSync(file);
  const parts = [];
  let offset = 0;
  let frames = 0;
  let invalid = 0;
  while (offset + 4 <= buf.length) {
    const at = buf.indexOf(Buffer.from(MAGIC), offset);
    if (at === -1) break;
    try {
      const out = zlib.zstdDecompressSync(buf.subarray(at));
      const text = out.toString('utf8');
      if (text.startsWith('{')) {
        parts.push(text.endsWith('\n') ? text : `${text}\n`);
        frames++;
        offset = at + 4;
        continue;
      }
      invalid++;
    } catch {
      invalid++;
    }
    offset = at + 4;
  }
  return { text: parts.join(''), frames, invalid, bytes: buf.length };
}

fs.mkdirSync(outDir, { recursive: true });
for (const file of [...walk(root)]) {
  const sessionId = path.basename(path.dirname(file));
  const { text, frames, invalid, bytes } = decodeFrames(file);
  const dest = path.join(outDir, `${sessionId}.jsonl`);
  fs.writeFileSync(dest, text);
  const lines = text.split('\n').filter((l) => l.trim() !== '');
  console.log(`${sessionId}  frames=${frames} badCandidates=${invalid} in=${bytes}B out=${text.length}B events=${lines.length}`);
}
