const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const rooms = new Map();
const mime = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml' };
const server = http.createServer((req, res) => {
  let pathname = decodeURIComponent((req.url || '/').split('?')[0]);
  if (pathname === '/') pathname = '/index.html';
  const file = path.resolve(PUBLIC, '.' + pathname);
  if (!file.startsWith(PUBLIC + path.sep) && file !== path.join(PUBLIC, 'index.html')) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': mime[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-store' });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server });
function send(ws, data) { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); }
function broadcast(room, data) { for (const p of room.players) send(p.ws, data); }
function cleanRoom(room) {
  room.players = room.players.filter(p => p.ws && p.ws.readyState === WebSocket.OPEN);
  if (!room.players.length) rooms.delete(room.code);
  else broadcast(room, { type: 'roomUpdate', code: room.code, players: room.players.map(p => ({ id: p.id, name: p.name, car: p.car })), hostId: room.hostId, started: room.started });
}
function code() { const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let out; do { out = ''; for (let i=0;i<4;i++) out += chars[Math.floor(Math.random()*chars.length)]; } while (rooms.has(out)); return out; }
wss.on('connection', ws => {
  ws.id = Math.random().toString(36).slice(2, 10);
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.type !== 'string') return;
    if (m.type === 'createRoom') {
      if (ws.roomCode) return;
      const c = code(); const player = { id: ws.id, ws, name: safeName(m.name), car: safeCar(m.car), state: null };
      const room = { code: c, hostId: ws.id, players: [player], started: false };
      rooms.set(c, room); ws.roomCode = c; ws.playerId = ws.id;
      send(ws, { type: 'roomCreated', code: c, playerId: ws.id });
      broadcast(room, { type: 'roomUpdate', code: c, players: room.players.map(p => ({ id:p.id, name:p.name, car:p.car })), hostId: room.hostId, started: false });
    } else if (m.type === 'joinRoom') {
      if (ws.roomCode) return;
      const c = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0,4); const room = rooms.get(c);
      if (!room) return send(ws, { type:'error', message:'לא מצאנו חדר כזה. בדקו את הקוד ונסו שוב.' });
      if (room.started) return send(ws, { type:'error', message:'המרוץ בחדר הזה כבר התחיל.' });
      if (room.players.length >= 2) return send(ws, { type:'error', message:'החדר מלא — אפשר לשחק עד שני משתתפים.' });
      const player = { id: ws.id, ws, name: safeName(m.name), car: safeCar(m.car), state: null };
      room.players.push(player); ws.roomCode = c; ws.playerId = ws.id;
      send(ws, { type:'joinedRoom', code:c, playerId:ws.id });
      broadcast(room, { type:'roomUpdate', code:c, players:room.players.map(p=>({id:p.id,name:p.name,car:p.car})), hostId:room.hostId, started:false });
    } else if (m.type === 'startGame') {
      const room = rooms.get(ws.roomCode); if (!room || room.hostId !== ws.id || room.players.length !== 2 || room.started) return;
      room.started = true; room.startAt = Date.now() + 1800;
      broadcast(room, { type:'gameStart', startAt:room.startAt });
    } else if (m.type === 'playerState') {
      const room = rooms.get(ws.roomCode); if (!room || !room.started) return;
      const player = room.players.find(p => p.id === ws.id); if (!player) return;
      player.state = {
        pos: finite(m.state?.pos, 0, 1e9), px: finite(m.state?.px, -1.5, 1.5),
        sp: finite(m.state?.sp, 0, 3), score: finite(m.state?.score, 0, 1e9),
        coins: finite(m.state?.coins, 0, 1e6), hp: finite(m.state?.hp, 0, 3),
        car: player.car, name: player.name, over: !!m.state?.over, at: Date.now()
      };
      for (const other of room.players) if (other.id !== ws.id) send(other.ws, { type:'opponentState', playerId:player.id, state:player.state });
    } else if (m.type === 'leaveRoom') leave(ws);
  });
  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));
});
function safeName(v) { const s = String(v || '').trim().replace(/[<>]/g,'').slice(0,16); return s || 'נהג/ת'; }
function safeCar(v) { const n = Number(v); return Number.isInteger(n) && n >= 0 && n <= 3 ? n : 0; }
function finite(v, min, max) { const n = Number(v); return Number.isFinite(n) ? Math.max(min, Math.min(max,n)) : 0; }
function leave(ws) {
  const c = ws.roomCode; if (!c) return; ws.roomCode = null;
  const room = rooms.get(c); if (!room) return;
  room.players = room.players.filter(p => p.id !== ws.id);
  if (!room.players.length) rooms.delete(c);
  else {
    if (room.hostId === ws.id) room.hostId = room.players[0].id;
    room.started = false;
    broadcast(room, { type:'opponentLeft', message:'המשתתף השני התנתק. אפשר ליצור חדר חדש ולהתחיל שוב.' });
    broadcast(room, { type:'roomUpdate', code:room.code, players:room.players.map(p=>({id:p.id,name:p.name,car:p.car})), hostId:room.hostId, started:false });
  }
}
server.listen(PORT, '0.0.0.0', () => console.log(`Turbo multiplayer listening on ${PORT}`));
