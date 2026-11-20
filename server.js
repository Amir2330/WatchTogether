const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'public', 'index.html');

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
  const p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/vendor/hls.min.js') return sendFile(res, HLS, 'application/javascript; charset=utf-8', 'public, max-age=86400');
  if (p.startsWith('/fonts/')) {
    const name = path.basename(p);                       // no path traversal
    return sendFile(res, path.join(FONTS, name), TYPES[path.extname(name)] || 'application/octet-stream', 'public, max-age=86400');
  }
  sendFile(res, INDEX, 'text/html; charset=utf-8');
});

const wss = new WebSocketServer({ server });
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
  const url = new URL(req.url, 'http://localhost');
  const roomId = (url.searchParams.get('room') || 'default').slice(0, 50);
  ws.name = (url.searchParams.get('name') || 'مهمان').trim().slice(0, 20) || 'مهمان';
  ws.ready = true;          // assume ready; client reports a stall only if it really starves
  ws.forced = false;
  ws.loaded = true;
  const room = getRoom(roomId);

  commit(room);
  room.clients.add(ws);

  toAll(room, snapshot(room));          // everyone (incl. newcomer) gets state; others pause until newcomer is ready
  sendUsers(room);
  toAll(room, { type: 'event', text: ws.name + ' وارد اتاق شد' }, ws);

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'sync') {
      return ws.send(JSON.stringify(snapshot(room)));
    }

    if (m.type === 'notice' && typeof m.text === 'string') {
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
      if (typeof m.src === 'string') s.src = m.src.slice(0, 2000);
      if (typeof m.playing === 'boolean') s.playing = m.playing;
      if (typeof m.time === 'number' && isFinite(m.time)) s.time = Math.max(0, m.time);
      if (typeof m.rate === 'number' && m.rate >= 0.25 && m.rate <= 4) s.rate = m.rate;
      s.at = Date.now();
      const snap = snapshot(room);
      toAll(room, { ...snap, by: ws.name, action: String(m.action || '') }, ws);
      ws.send(JSON.stringify(snap));
      return;
    }

    if (m.type === 'chat' && typeof m.text === 'string') {
      toAll(room, { type: 'chat', name: ws.name, text: m.text.slice(0, 500) }, ws);
    }
  });

  ws.on('close', () => {
    commit(room);
    room.clients.delete(ws);
    if (room.clients.size === 0) { rooms.delete(roomId); return; }
    toAll(room, snapshot(room));
    sendUsers(room);
    toAll(room, { type: 'event', text: ws.name + ' از اتاق رفت' });
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

server.listen(PORT, () => console.log(`Running on http://localhost:${PORT}`));
