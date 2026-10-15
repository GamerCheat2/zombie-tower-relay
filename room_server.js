// Máy chủ RELAY phòng cho Thủ Thành Zombie.
// Chủ phòng và người vào đều chỉ kết nối RA máy chủ này (WebSocket) -> không cần mở cổng router,
// khác WiFi / 4G / mạng nào cũng vào được bằng ID phòng kiểu ZT-7K9Q2.
//
//   npm install && npm start       (cổng mặc định 8080, hoặc biến môi trường PORT)
//
// Giao thức (đường dẫn /relay):
//   Chủ phòng -> {"t":"host","id":"ZT-XXXXX","token":"..."}   <- {"t":"ok"} | {"t":"err","m":"id_taken"}
//   Người vào -> {"t":"join","id":"ZT-XXXXX"}                 <- {"t":"ok"} | {"t":"err","m":"not_found|full|started"}
//   Chủ phòng -> {"t":"started"}                               (không nhận thêm người)
//   Relay -> chủ phòng: {"t":"open","s":N} / {"t":"close","s":N}  (N = số phiên của người vào, 1..255)
//   Dữ liệu trận là khung nhị phân:
//     người vào -> relay: [datagram]            relay -> chủ phòng: [N][datagram]
//     chủ phòng -> relay: [N][datagram]         relay -> người vào: [datagram]
const http = require("http");
const crypto = require("crypto");

const PORT = parseInt(process.env.PORT || "8080", 10);
const ID_RE = /^ZT-[2-9A-HJKMNP-Z]{5}$/;
const MAX_CLIENTS = 3;              // + chủ phòng = 4 người
const MAX_ROOMS = 3000;
const MAX_FRAME = 4096;             // datagram ENet tối đa ~1400 byte
const PING_MS = 20000;
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const rooms = new Map();            // id -> {host, token, clients: Map(s->conn), started, timer}

// ---------------------------------------------------------------- WebSocket tối giản (RFC 6455, không cần gói ngoài)
class Conn {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.open = true; this.alive = true;
    this.frag = null; this.fragBin = false;
    this.role = null; this.room = null; this.roomId = null; this.session = 0;
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => {});
    socket.setNoDelay(true);
  }
  frame(op, payload) {
    const n = payload.length;
    let h;
    if (n < 126) h = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    return Buffer.concat([h, payload]);
  }
  write(op, payload) { if (this.open && !this.socket.destroyed) this.socket.write(this.frame(op, payload)); }
  sendJson(o) { this.write(1, Buffer.from(JSON.stringify(o))); }
  sendBin(b) { this.write(2, b); }
  ping() { this.write(9, Buffer.alloc(0)); }
  close(code = 1000) {
    if (!this.open) return;
    const p = Buffer.alloc(2); p.writeUInt16BE(code);
    this.write(8, p);
    this.open = false;
    this.socket.end();
    setTimeout(() => this.socket.destroy(), 500).unref();
    this.onClose();
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_FRAME * 8) return this.close(1009);
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_FRAME) return this.close(1009);
      if (!masked) return this.close(1002);
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4);
      const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      this.alive = true;
      if (op === 8) return this.close(1000);
      if (op === 9) { this.write(10, payload); continue; }
      if (op === 10) continue;
      if (op === 1 || op === 2) { this.frag = [payload]; this.fragBin = op === 2; }
      else if (op === 0 && this.frag) this.frag.push(payload);
      else continue;
      if (fin) {
        const msg = Buffer.concat(this.frag); this.frag = null;
        this.onMessage(msg, this.fragBin);
      }
    }
  }
  onClose() {
    if (this.closed) return; this.closed = true; this.open = false;
    const room = this.room;
    if (!room) return;
    if (this.role === "client") {
      if (room.clients.get(this.session) === this) room.clients.delete(this.session);
      if (room.host) room.host.sendJson({ t: "close", s: this.session });
    } else if (this.role === "host" && room.host === this) {
      room.host = null;
      // cho chủ phòng 20s để vào lại cùng ID (rớt mạng thoáng qua), sau đó đóng phòng
      room.timer = setTimeout(() => {
        if (room.host || rooms.get(this.roomId) !== room) return;
        rooms.delete(this.roomId);
        for (const c of room.clients.values()) c.close(1001);
      }, 20000);
    }
  }
  onMessage(msg, isBin) {
    if (!isBin) {
      let m; try { m = JSON.parse(msg.toString()); } catch (e) { return this.close(1003); }
      if (this.role === null && m.t === "host") {
        const id = String(m.id || ""), token = String(m.token || "").slice(0, 64);
        if (!ID_RE.test(id) || !token) return this.sendJson({ t: "err", m: "bad_id" });
        const ex = rooms.get(id);
        if (ex) {
          if (ex.token !== token) return this.sendJson({ t: "err", m: "id_taken" });
          // chủ phòng cũ rớt mạng và vào lại cùng ID: giữ phòng, thay kết nối
          clearTimeout(ex.timer);
          if (ex.host && ex.host !== this) { const old = ex.host; ex.host = null; old.role = null; old.close(1000); }
          ex.host = this; this.role = "host"; this.room = ex; this.roomId = id;
          for (const [s] of ex.clients) this.sendJson({ t: "open", s });
          return this.sendJson({ t: "ok" });
        }
        if (rooms.size >= MAX_ROOMS) return this.sendJson({ t: "err", m: "busy" });
        const room = { host: this, token, clients: new Map(), started: false, timer: null };
        rooms.set(id, room);
        this.role = "host"; this.room = room; this.roomId = id;
        return this.sendJson({ t: "ok" });
      }
      if (this.role === null && m.t === "join") {
        const room = rooms.get(String(m.id || "").toUpperCase());
        if (!room || !room.host) return this.sendJson({ t: "err", m: "not_found" });
        if (room.started) return this.sendJson({ t: "err", m: "started" });
        if (room.clients.size >= MAX_CLIENTS) return this.sendJson({ t: "err", m: "full" });
        let s = 1; while (room.clients.has(s)) s++;
        room.clients.set(s, this);
        this.role = "client"; this.room = room; this.session = s;
        room.host.sendJson({ t: "open", s });
        return this.sendJson({ t: "ok" });
      }
      if (this.role === "host" && m.t === "started") this.room.started = true;
      if (this.role === "host" && m.t === "bye") {      // chủ phòng chủ động đóng phòng
        const room = this.room;
        clearTimeout(room.timer);
        if (rooms.get(this.roomId) === room) rooms.delete(this.roomId);
        room.host = null;
        for (const c of room.clients.values()) c.close(1001);
      }
      return;
    }
    if (this.role === "client") {
      const h = this.room.host;
      if (h) h.sendBin(Buffer.concat([Buffer.from([this.session]), msg]));
    } else if (this.role === "host" && msg.length > 1) {
      const c = this.room.clients.get(msg[0]);
      if (c) c.sendBin(msg.subarray(1));
    }
  }
}

const conns = new Set();
const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
  }
  res.writeHead(404); res.end();
});
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.url || "").split("?")[0] !== "/relay") { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  const c = new Conn(socket);
  conns.add(c);
  socket.on("close", () => conns.delete(c));
});

setInterval(() => {
  for (const c of conns) {
    if (!c.alive) { c.socket.destroy(); continue; }
    c.alive = false; c.ping();
  }
}, PING_MS).unref();

server.listen(PORT, () => console.log(`Relay phòng đang chạy ở cổng ${PORT}`));
