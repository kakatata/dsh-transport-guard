// Minimal read-only ASAR inspector.
// Usage:
//   node asar-scan.mjs <archive> list [substr]        -> file listing (filtered by path substring)
//   node asar-scan.mjs <archive> grep <regex> [pathSubstr] -> regex search inside file contents
//   node asar-scan.mjs <archive> cat <exactPath>      -> print one file
//   node asar-scan.mjs <archive> extract <exactPath> <dest> -> write one file (UTF-8, byte-exact)
//   node asar-scan.mjs <archive> range <exactPath> <from:to> -> print a line range
import fs from 'node:fs';

const [, , archive, cmd, arg, arg2] = process.argv;
if (!archive || !cmd) {
  console.error('usage: node asar-scan.mjs <archive> list|grep|cat [arg] [arg2]');
  process.exit(2);
}

const fd = fs.openSync(archive, 'r');
const head = Buffer.alloc(16);
fs.readSync(fd, head, 0, 16, 0);
// asar/pickle layout: uint32 size of the size field payload, then header pickle size
const headerSize = head.readUInt32LE(12);
const headerBuf = Buffer.alloc(headerSize);
fs.readSync(fd, headerBuf, 0, headerSize, 16);
const headerJson = JSON.parse(headerBuf.toString('utf8'));
const dataOffset = 16 + headerSize;

const files = [];
(function walk(node, prefix) {
  for (const [name, entry] of Object.entries(node.files ?? {})) {
    const p = prefix ? `${prefix}/${name}` : name;
    if (entry.files) walk(entry, p);
    else if (typeof entry.offset === 'string') {
      files.push({ path: p, offset: Number(entry.offset), size: entry.size });
    }
  }
})(headerJson, '');

const readFileBuf = (f) => {
  const buf = Buffer.alloc(f.size);
  fs.readSync(fd, buf, 0, f.size, dataOffset + f.offset);
  return buf;
};

if (cmd === 'list') {
  const filtered = arg ? files.filter((f) => f.path.includes(arg)) : files;
  console.log(`total files: ${files.length}, matched: ${filtered.length}`);
  for (const f of filtered) console.log(`${String(f.size).padStart(9)}  ${f.path}`);
} else if (cmd === 'grep') {
  const re = new RegExp(arg, 'gi');
  let hits = 0;
  for (const f of files) {
    if (arg2 && !f.path.includes(arg2)) continue;
    if (f.size > 40 * 1024 * 1024) continue;
    let text;
    try {
      text = readFileBuf(f).toString('utf8');
    } catch {
      continue;
    }
    if (!/[a-zA-Z]/.test(text.slice(0, 400))) continue;
    const lines = text.split(/\r?\n/);
    lines.forEach((line, i) => {
      re.lastIndex = 0;
      if (re.test(line)) {
        hits++;
        console.log(`${f.path}:${i + 1}: ${line.trim().slice(0, 300)}`);
      }
    });
  }
  console.log(`--- hits: ${hits}`);
} else if (cmd === 'extract') {
  const f = files.find((x) => x.path === arg);
  if (!f) {
    console.error(`not found: ${arg}`);
    process.exit(1);
  }
  fs.writeFileSync(arg2, readFileBuf(f));
  console.log(`wrote ${arg2} (${f.size} bytes)`);
} else if (cmd === 'range') {
  const f = files.find((x) => x.path === arg);
  if (!f) {
    console.error(`not found: ${arg}`);
    process.exit(1);
  }
  const [from, to] = arg2.split(':').map(Number);
  const lines = readFileBuf(f).toString('utf8').split(/\r?\n/);
  for (let i = from - 1; i < Math.min(to, lines.length); i++) {
    console.log(`${i + 1}: ${lines[i]}`);
  }
} else if (cmd === 'cat') {
  const f = files.find((x) => x.path === arg || x.path === arg.replace(/^\//, ''));
  if (!f) {
    console.error(`not found: ${arg}`);
    process.exit(1);
  }
  process.stdout.write(readFileBuf(f).toString('utf8'));
} else {
  console.error(`unknown cmd: ${cmd}`);
  process.exit(2);
}
