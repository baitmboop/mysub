/**
 * vless over websocket —— Cloudflare Worker 后端
 * =================================================
 * 这是「后端」。节点里的 server(IP) 只是「入口」，入口可以随便换，
 * 后端只有这一个。所以这个文件部署一次，之后只改 IP 不用动它。
 *
 * 部署（全网页操作，不需要命令行、不需要装 wrangler）
 * ---------------------------------------------------
 * 1. dash.cloudflare.com 注册（邮箱即可，免费版不绑卡）
 * 2. 左侧 Workers & Pages → Create → Worker → 起个名字 → Deploy
 * 3. Edit code，把整个文件粘进去，覆盖原来的 Hello World
 * 4. 改下面两行常量（UUID / PATH），值从 `python3 cf_sub.py --init` 拿
 * 5. Deploy，然后浏览器打开 https://<名字>.<账号>.workers.dev
 *    看到 ok 就说明活着（不带 ws 头就是诊断页，不会泄露 UUID）
 *
 * 为什么必须有一个域名
 * --------------------
 * TLS 握手时客户端要发 SNI，CF 靠 SNI 找后端；SNI 只能是域名，不能是 IP。
 * workers.dev 就是 CF 白送的域名，不用买。
 *
 * 能力边界
 * --------
 * 只能转 TCP（cmd=1）。UDP（cmd=2/3）直接拒绝 —— 所以节点要写 udp: false，
 * 否则 DNS 查询会卡住直到超时。
 */

import { connect } from 'cloudflare:sockets';

// ==== 只改这两行 ============================================
const UUID = '__UUID__';
const PATH = '__PATH__';   // 必须带前导斜杠，和 cf_self.json 里的 path 一模一样
// ===========================================================

const TE = new TextEncoder();
const TD = new TextDecoder();

function uuidToBytes(s) {
  const h = s.replace(/-/g, '');
  if (h.length !== 32) throw new Error('bad uuid');
  const out = new Uint8Array(16);
  for (let i = 0; i < 16; i++) out[i] = parseInt(h.slice(i * 2, i * 2 + 2), 16);
  return out;
}
const UUID_B = uuidToBytes(UUID);

function ipv6Str(b) {
  const p = [];
  for (let i = 0; i < 16; i += 2) p.push(((b[i] << 8) | b[i + 1]).toString(16));
  return p.join(':');
}

/** 把 ws 的 message 事件转成字节流。CF 的 ws 是消息模式，vless 要的是流。 */
function wsToStream(ws) {
  return new ReadableStream({
    start(ctl) {
      let closed = false;
      const push = (u8) => {
        if (closed) return;
        try { ctl.enqueue(u8); } catch { closed = true; }
      };
      ws.addEventListener('message', (e) => {
        if (typeof e.data === 'string') push(TE.encode(e.data));
        else push(new Uint8Array(e.data));
      });
      ws.addEventListener('close', () => {
        closed = true;
        try { ctl.close(); } catch { /* 已关 */ }
      });
      ws.addEventListener('error', () => {
        closed = true;
        try { ctl.error(new Error('ws error')); } catch { /* 已关 */ }
      });
    },
  });
}

/** 带「按字节读」能力的读取器，头解析要精确读 N 字节，不能按消息边界。 */
class ByteReader {
  constructor(inner) {
    this.r = inner;      // ReadableStreamDefaultReader
    this.q = [];         // 未消费的 chunk
    this.len = 0;
  }
  async _fill(n) {
    while (this.len < n) {
      const { value, done } = await this.r.read();
      if (done) throw new Error('client closed before header done');
      const u8 = value instanceof Uint8Array ? value : new Uint8Array(value);
      this.q.push(u8);
      this.len += u8.length;
    }
  }
  async read(n) {
    if (n === 0) return new Uint8Array(0);
    await this._fill(n);
    const out = new Uint8Array(n);
    let off = 0;
    while (off < n) {
      const head = this.q[0];
      const take = Math.min(head.length, n - off);
      out.set(head.subarray(0, take), off);
      off += take;
      if (take === head.length) this.q.shift();
      else this.q[0] = head.subarray(take);
    }
    this.len -= n;
    return out;
  }
  /** 头解析完之后，缓冲区里剩下的就是正文，先吐出来 */
  drain() {
    if (!this.len) return new Uint8Array(0);
    const out = new Uint8Array(this.len);
    let off = 0;
    for (const c of this.q) { out.set(c, off); off += c.length; }
    this.q = [];
    this.len = 0;
    return out;
  }
}

async function readVlessHeader(rd) {
  const ver = (await rd.read(1))[0];
  const id = await rd.read(16);
  for (let i = 0; i < 16; i++) {
    if (id[i] !== UUID_B[i]) throw new Error('uuid mismatch');
  }
  const optLen = (await rd.read(1))[0];
  if (optLen) await rd.read(optLen);          // 现在全是 0，但协议允许

  const cmd = (await rd.read(1))[0];
  const port = ((await rd.read(1))[0] << 8) | (await rd.read(1))[0];
  const atyp = (await rd.read(1))[0];

  let host;
  if (atyp === 1) {
    host = Array.from(await rd.read(4)).join('.');
  } else if (atyp === 2) {
    const n = (await rd.read(1))[0];
    host = TD.decode(await rd.read(n));
  } else if (atyp === 3) {
    host = ipv6Str(await rd.read(16));
  } else {
    throw new Error('bad atyp ' + atyp);
  }
  return { ver, cmd, port, host };
}

async function handle(ws) {
  const rd = new ByteReader(wsToStream(ws).getReader());
  let sock = null;
  let upstreamClosed = false;
  try {
    const h = await readVlessHeader(rd);
    if (h.cmd !== 1) throw new Error('udp not supported (cmd=' + h.cmd + ')');

    sock = connect({ hostname: h.host, port: h.port });
    await sock.opened;

    const w = sock.writable.getWriter();
    // vless 响应头：version + optLen(0)。不发这 2 字节客户端会一直等。
    await w.write(new Uint8Array([h.ver, 0]));

    // 上行：client -> target。先写缓冲区残留，再持续泵。
    (async () => {
      try {
        const rest = rd.drain();
        if (rest.length) await w.write(rest);
        for (;;) {
          const { value, done } = await rd.r.read();
          if (done) break;
          await w.write(value instanceof Uint8Array ? value : new Uint8Array(value));
        }
      } catch { /* 客户端断开是常态 */ }
      try { await w.close(); } catch { /* noop */ }
    })();

    // 下行：target -> client
    sock.readable.pipeTo(new WritableStream({
      write(chunk) {
        if (!upstreamClosed) {
          try { ws.send(chunk); } catch { upstreamClosed = true; }
        }
      },
    })).catch(() => { /* 上游断开 */ });
  } catch (e) {
    try { ws.close(1011, String(e.message || e).slice(0, 120)); } catch { /* noop */ }
    try { if (sock) sock.close(); } catch { /* noop */ }
  }
}

export default {
  async fetch(request) {
    const url = new URL(request.url);

    // 诊断：不带 Upgrade 的普通请求直接回 ok，方便确认 Worker 活着。
    if ((request.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      return new Response('ok\n', {
        headers: { 'content-type': 'text/plain; charset=utf-8' },
      });
    }

    // path 不对就拒绝，避免被扫。
    if (url.pathname !== PATH) {
      return new Response('not found\n', { status: 404 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    handle(server);
    return new Response(null, { status: 101, webSocket: client });
  },
};
