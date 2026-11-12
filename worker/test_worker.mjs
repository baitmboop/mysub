/**
 * worker.js 的头解析单元测试（node 跑，不联网）
 * ================================================
 * 为什么值得单独测
 * ----------------
 * vless 请求头是定长 + 变长混合结构。解析错一个字节的偏移，在线上的表现是
 * 「TCP 连上了、没报错、就是不通」—— CF 后台日志里什么都看不到。
 * 能在一秒内本地确定的事，不要留到线上去猜。
 *
 * 跑法: node worker/test_worker.mjs
 */
import fs from 'node:fs';
import path from 'node:path';
import url from 'node:url';

const HERE = path.dirname(url.fileURLToPath(import.meta.url));
const SRC = path.join(HERE, 'worker.js');

const UUID = '11111111-2222-3333-4444-555555555555';
const UUID_B = Buffer.from(UUID.replace(/-/g, ''), 'hex');

// 把 ESM 的 import 换成桩，再临时补一个导出钩子 —— 生产文件一个字节都不动
let src = fs.readFileSync(SRC, 'utf8');
src = src.replace(/^import\s*\{[^}]*\}\s*from\s*'cloudflare:sockets';?$/m,
                  'const connect = (...a) => globalThis.__connect(...a);');
src = src.replace("const UUID = '__UUID__';", `const UUID = '${UUID}';`);
src = src.replace("const PATH = '__PATH__';", "const PATH = '/t';");
src += '\nexport { readVlessHeader, ByteReader };\n';

const TMP = path.join(HERE, '.test_worker.mjs');
fs.writeFileSync(TMP, src);
const { readVlessHeader, ByteReader } = await import(url.pathToFileURL(TMP).href);

let pass = 0;
const fails = [];
async function t(name, fn) {
  try {
    await fn();
    pass++;
    console.log(`  ok   ${name}`);
  } catch (e) {
    fails.push(`${name}: ${e.message}`);
    console.log(`  FAIL ${name}: ${e.message}`);
  }
}

/** 拼一个真实的 vless 请求头 */
function buildHeader({ cmd = 1, port = 443, host = 'example.com', atyp = 2, payload = '' }) {
  const parts = [Buffer.from([0]), UUID_B, Buffer.from([0])];   // ver, uuid, optLen
  parts.push(Buffer.from([cmd, port >> 8, port & 0xff]));
  if (atyp === 2) {
    const h = Buffer.from(host, 'utf8');
    parts.push(Buffer.from([2, h.length]), h);
  } else if (atyp === 1) {
    parts.push(Buffer.from([1]), Buffer.from(host.split('.').map(Number)));
  } else {
    const b = Buffer.alloc(16);
    host.split(':').forEach((s, i) => b.writeUInt16BE(parseInt(s, 16), i * 2));
    parts.push(Buffer.from([3]), b);
  }
  if (payload) parts.push(Buffer.from(payload, 'utf8'));
  return Buffer.concat(parts);
}

/** 按固定节奏切块，模拟 ws 消息边界和头解析错位 */
function streamOf(buf, sizes = [7]) {
  let off = 0, i = 0;
  return new ReadableStream({
    pull(ctl) {
      if (off >= buf.length) { ctl.close(); return; }
      const end = Math.min(off + sizes[i++ % sizes.length], buf.length);
      ctl.enqueue(new Uint8Array(buf.subarray(off, end)));
      off = end;
    },
  });
}

await t('域名地址 + 残留正文', async () => {
  const rd = new ByteReader(streamOf(buildHeader({ host: 'a.example.com', port: 8443, payload: 'HELLO' })).getReader());
  const h = await readVlessHeader(rd);
  if (h.host !== 'a.example.com') throw new Error('host=' + h.host);
  if (h.port !== 8443) throw new Error('port=' + h.port);
  if (h.cmd !== 1) throw new Error('cmd=' + h.cmd);
  if (Buffer.from(rd.drain()).toString() !== 'HELLO') throw new Error('残留正文丢了');
});

// 这条是核心：头被切成 1/3/5 字节的碎块，唯一能抓住"按消息边界解析"的写法。
// 正文要按生产代码的顺序取：先 drain 缓冲区，再把流泵干 —— 只 drain 会漏掉
// 还没被 pull 进来的尾部，那是测试写错了，不是实现错。
await t('头跨 1/3/5 字节碎块', async () => {
  const rd = new ByteReader(streamOf(buildHeader({ host: 'x.y.example.org', port: 2053, payload: 'abc' }), [1, 3, 5]).getReader());
  const h = await readVlessHeader(rd);
  if (h.host !== 'x.y.example.org' || h.port !== 2053) throw new Error(JSON.stringify(h));
  const parts = [Buffer.from(rd.drain())];
  for (;;) {
    const { value, done } = await rd.r.read();
    if (done) break;
    parts.push(Buffer.from(value));
  }
  const body = Buffer.concat(parts).toString();
  if (body !== 'abc') throw new Error(`跨块正文错位: ${JSON.stringify(body)}`);
});

await t('IPv4 地址', async () => {
  const rd = new ByteReader(streamOf(buildHeader({ atyp: 1, host: '1.2.3.4', port: 80 }), [2]).getReader());
  const h = await readVlessHeader(rd);
  if (h.host !== '1.2.3.4' || h.port !== 80) throw new Error(JSON.stringify(h));
});

await t('IPv6 地址', async () => {
  const rd = new ByteReader(streamOf(buildHeader({ atyp: 3, host: '2001:db8:0:0:0:0:0:1' }), [5]).getReader());
  const h = await readVlessHeader(rd);
  if (h.host !== '2001:db8:0:0:0:0:0:1') throw new Error('host=' + h.host);
});

// UUID 不对必须拒：否则这个后端等于一台公开的开放代理
await t('uuid 不符就拒', async () => {
  const buf = buildHeader({});
  buf[1] ^= 0xff;
  const rd = new ByteReader(streamOf(buf, [4]).getReader());
  try {
    await readVlessHeader(rd);
  } catch (e) {
    if (!/uuid/.test(e.message)) throw new Error('错误信息不明确: ' + e.message);
    return;
  }
  throw new Error('uuid 错了还放行');
});

await t('客户端半途断开报错而不是静默', async () => {
  const rd = new ByteReader(streamOf(buildHeader({}).subarray(0, 10), [3]).getReader());
  try {
    await readVlessHeader(rd);
  } catch (e) {
    if (!/closed/.test(e.message)) throw new Error('错误信息: ' + e.message);
    return;
  }
  throw new Error('头没读完却成功返回');
});

await t('源码拒绝 UDP', async () => {
  if (!/cmd !== 1/.test(src)) throw new Error('没看到 cmd 检查');
});

fs.unlinkSync(TMP);

console.log(`\n通过 ${pass} / ${pass + fails.length}`);
if (fails.length) {
  console.log('\n失败项:');
  fails.forEach((f) => console.log('  - ' + f));
  process.exit(1);
}
console.log('全部通过');
