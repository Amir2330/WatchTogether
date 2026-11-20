const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'public', 'index.html');
const TERMS = path.join(__dirname, 'public', 'terms.html');

// ---- limits (override with environment variables on the host) ----
const MAX_ROOM_USERS = +process.env.MAX_ROOM_USERS || 10;   // people per room
const MAX_ROOMS      = +process.env.MAX_ROOMS      || 20;   // rooms alive at once
const MAX_TOTAL      = +process.env.MAX_TOTAL      || 40;   // connections alive at once
const MAX_PER_IP     = +process.env.MAX_PER_IP     || 10;   // connections per IP
const EMPTY_ROOM_MS = (+process.env.EMPTY_ROOM_MINUTES || 5) * 60000;   // keep an empty room this long
const ipCount = new Map();

// sliding-window rate limiter, one bucket per connection
function over(ws, bucket, max, ms) {
  const now = Date.now();
  const b = ws.rl[bucket] || (ws.rl[bucket] = { t: now, n: 0 });
  if (now - b.t > ms) { b.t = now; b.n = 0; }
  return ++b.n > max;
}
function validSrc(s) {
  return s.length <= 2000 && (s === '' || s.startsWith('file:') || /^https?:\/\//i.test(s));
}
function reject(ws, text) {
  try { ws.send(JSON.stringify({ type: 'error', fatal: true, text })); } catch {}
  ws.close(1008, 'rejected');
}

const HLS = path.join(__dirname, 'node_modules', 'hls.js', 'dist', 'hls.min.js');
const FONTS = path.join(__dirname, 'public', 'fonts');
const TYPES = { '.woff2': 'font/woff2', '.woff': 'font/woff', '.ttf': 'font/ttf' };

function sendFile(res, file, type, cache) {
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': type, 'Cache-Control': cache || 'no-cache' });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');     // don't leak the room code to video hosts
  res.setHeader('X-Frame-Options', 'DENY');
  let p;
  try { p = decodeURIComponent((req.url || '/').split('?')[0]); }
  catch { res.writeHead(400); return res.end('bad request'); }
  if (p === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (p === '/terms') return sendFile(res, TERMS, 'text/html; charset=utf-8');
  if (p === '/vendor/hls.min.js') return sendFile(res, HLS, 'application/javascript; charset=utf-8', 'public, max-age=86400');
  if (p.startsWith('/fonts/')) {
    const name = path.basename(p);                       // no path traversal
    return sendFile(res, path.join(FONTS, name), TYPES[path.extname(name)] || 'application/octet-stream', 'public, max-age=86400');
  }
  sendFile(res, INDEX, 'text/html; charset=utf-8');
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });   // bigger messages are refused by the library
const rooms = new Map();

/*
  Room state is authoritative on the server.
  - state.playing = what people WANT (someone pressed play)
  - running       = playing AND every client has its video ready (buffered)
  The clock only advances while running, so everyone waits for the slowest person.
*/
function getRoom(id) {
  if (!rooms.has(id)) {
    rooms.set(id, { clients: new Set(), state: { src: '', playing: false, time: 0, rate: 1, at: Date.now() } });
  }
  return rooms.get(id);
}

function allReady(room) {
  for (const c of room.clients) if (!c.ready) return false;
  return true;
}
function running(room) { return room.state.playing && allReady(room); }

function nowTime(room) {
  const s = room.state;
  return running(room) ? s.time + ((Date.now() - s.at) / 1000) * s.rate : s.time;
}
// Must be called BEFORE anything that can change "running" (ready flips, join, leave, update)
function commit(room) {
  room.state.time = nowTime(room);
  room.state.at = Date.now();
}

function snapshot(room) {
  const s = room.state;
  return {
    type: 'state',
    src: s.src,
    want: s.playing,
    playing: running(room),
    time: nowTime(room),
    rate: s.rate,
    waiting: [...room.clients].filter(c => !c.ready).map(c => c.name),
  };
}

function toAll(room, obj, except) {
  const out = JSON.stringify(obj);
  for (const c of room.clients) if (c !== except && c.readyState === 1) c.send(out);
}
function sendUsers(room) {
  toAll(room, { type: 'users', users: [...room.clients].map(c => ({ name: c.name, loaded: c.loaded })) });
}

wss.on('connection', (ws, req) => {
  ws.rl = {};
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', () => {});

  // only our own pages may use this server (non-browser clients send no Origin)
  const origin = req.headers.origin;
  if (origin) {
    let ok = false;
    try { ok = new URL(origin).host === req.headers.host; } catch {}
    if (!ok) return ws.close(1008, 'origin');
  }
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?';
  if (wss.clients.size > MAX_TOTAL) return reject(ws, 'سرور الان شلوغه، چند دقیقه دیگه دوباره امتحان کن.');
  if ((ipCount.get(ip) || 0) >= MAX_PER_IP) return reject(ws, 'تعداد اتصال‌های این شبکه زیاده. چند تا از تب‌ها رو ببند.');

  const url = new URL(req.url, 'http://localhost');
  const roomId = (url.searchParams.get('room') || 'default').slice(0, 50);
  ws.name = (url.searchParams.get('name') || 'مهمان').trim().slice(0, 20) || 'مهمان';
  ws.ready = true;          // assume ready; client reports a stall only if it really starves
  ws.forced = false;
  ws.cid = (url.searchParams.get('cid') || '').slice(0, 40);   // same browser tab reconnecting
  ws.loaded = true;
  const existing = rooms.get(roomId);
  // a leftover connection of the same tab (network dropped / phone slept) must not block its own return
  const stale = existing && ws.cid && [...existing.clients].find(c => c.cid === ws.cid);
  if (!existing && rooms.size >= MAX_ROOMS) return reject(ws, 'ظرفیت سرور پره، چند دقیقه دیگه امتحان کن.');
  if (existing && existing.clients.size - (stale ? 1 : 0) >= MAX_ROOM_USERS) return reject(ws, 'این اتاق پره (حداکثر ' + MAX_ROOM_USERS + ' نفر).');
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  const room = getRoom(roomId);

  commit(room);
  room.clients.add(ws);
  clearTimeout(room.emptyTimer);
  if (stale) { stale.silent = true; stale.terminate(); }

  toAll(room, snapshot(room));          // everyone (incl. newcomer) gets state; others pause until newcomer is ready
  sendUsers(room);
  if (!stale) toAll(room, { type: 'event', text: ws.name + ' وارد اتاق شد' }, ws);

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (over(ws, 'all', 100, 5000)) return ws.terminate();      // flooding

    if (m.type === 'sync') {
      return ws.send(JSON.stringify(snapshot(room)));
    }

    if (m.type === 'notice' && typeof m.text === 'string') {
      if (over(ws, 'notice', 5, 5000)) return;
      toAll(room, { type: 'event', text: ws.name + ': ' + m.text.slice(0, 200) }, ws);
      return;
    }

    if (m.type === 'ready') {
      if (typeof m.loaded === 'boolean' && m.loaded !== ws.loaded) { ws.loaded = m.loaded; sendUsers(room); }
      if (typeof m.ready !== 'boolean') return;
      if (m.ready) ws.forced = false;
      if (!m.ready && ws.forced) return;            // already given up waiting for this client
      if (m.ready === ws.ready) return;
      commit(room);
      ws.ready = m.ready;
      ws.notReadySince = m.ready ? 0 : Date.now();
      toAll(room, snapshot(room));
      return;
    }

    if (m.type === 'update') {
      commit(room);
      const s = room.state;
      const prevSrc = s.src;
      if (typeof m.src === 'string' && validSrc(m.src)) s.src = m.src;
      if (typeof m.playing === 'boolean') s.playing = m.playing;
      if (typeof m.time === 'number' && isFinite(m.time)) s.time = Math.max(0, m.time);
      if (typeof m.rate === 'number' && m.rate >= 0.25 && m.rate <= 4) s.rate = m.rate;
      s.at = Date.now();
      const snap = snapshot(room);
      toAll(room, { ...snap, by: ws.name, action: String(m.action || '').slice(0, 20) }, ws);
      ws.send(JSON.stringify(snap));
      return;
    }

    if (m.type === 'chat' && typeof m.text === 'string') {
      if (over(ws, 'chat', 6, 5000)) return ws.send(JSON.stringify({ type: 'event', text: 'آهسته‌تر پیام بده 🙂' }));
      toAll(room, { type: 'chat', name: ws.name, text: m.text.slice(0, 500) }, ws);
    }
  });

  ws.on('close', () => {
    const n = (ipCount.get(ip) || 1) - 1;
    if (n > 0) ipCount.set(ip, n); else ipCount.delete(ip);
    commit(room);
    room.clients.delete(ws);
    if (room.clients.size === 0) {
      // nobody left: pause the clock, but keep the room (video + position) for a few minutes
      room.state.playing = false;
      room.state.at = Date.now();
      clearTimeout(room.emptyTimer);
      room.emptyTimer = setTimeout(() => { if (room.clients.size === 0 && rooms.get(roomId) === room) rooms.delete(roomId); }, EMPTY_ROOM_MS);
      return;
    }
    toAll(room, snapshot(room));
    sendUsers(room);
    if (!ws.silent) toAll(room, { type: 'event', text: ws.name + ' از اتاق رفت' });
  });
});

// Safety net: never let one stuck client block everyone for more than 8 seconds
setInterval(() => {
  for (const room of rooms.values()) {
    for (const c of room.clients) {
      if (!c.ready && !c.forced && c.notReadySince && Date.now() - c.notReadySince > 8000) {
        commit(room);
        c.ready = true;
        c.forced = true;
        toAll(room, snapshot(room));
      }
    }
  }
}, 500);

// Drop dead connections (phone went to sleep, wifi lost...) so they don't pile up
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 10000);   // dead connections are dropped within 10-20 s

process.on('uncaughtException', e => console.error('uncaught:', e));

server.listen(PORT, () => console.log(`Running on http://localhost:${PORT}`));
