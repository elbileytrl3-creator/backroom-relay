// ============================================================
// server.js — Backrooms Relay WebSocket
// Compatible Render.com (HTTP + WS sur le même port)
// ============================================================
const http      = require('http');
const WebSocket = require('ws');

const PORT  = process.env.PORT || 8080;
const rooms = new Map();

// Render a besoin d'un serveur HTTP pour les health checks
const httpServer = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(`Backrooms Relay OK — ${rooms.size} salon(s) actif(s)`);
});

const wss = new WebSocket.Server({ server: httpServer });

wss.on('connection', ws => {
    ws._room   = null;
    ws._isHost = false;
    ws._id     = null;

    ws.on('message', raw => {
        const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
        if (buf.length < 1) return;
        const type = buf[0];

        if (type === 0x01) {                          // HOST REGISTER
            if (buf.length < 7) return;
            const code = buf.slice(1, 7).toString('ascii');
            if (rooms.has(code)) { ws.send(Buffer.from([0xE1])); return; }
            ws._isHost = true;
            ws._room   = code;
            rooms.set(code, { host: ws, clients: new Map(), nextId: 1 });
            ws.send(Buffer.from([0xA0]));
            console.log(`[+] Salon : ${code}`);
        }

        else if (type === 0x02) {                     // CLIENT JOIN
            if (buf.length < 7) return;
            const code = buf.slice(1, 7).toString('ascii');
            const room = rooms.get(code);
            if (!room) { ws.send(Buffer.from([0xE2])); return; }
            const id = room.nextId++;
            ws._isHost = false;
            ws._room   = code;
            ws._id     = id;
            room.clients.set(id, ws);
            ws.send(Buffer.from([0xA0, id]));
            room.host.send(Buffer.from([0xA1, id]));
            console.log(`[+] Client ${id} → ${code}`);
        }

        else if (type === 0x03) {                     // HOST → CLIENT
            if (!ws._isHost || buf.length < 2) return;
            const room = rooms.get(ws._room);
            if (!room) return;
            const target = room.clients.get(buf[1]);
            if (target?.readyState === WebSocket.OPEN) {
                const fwd = Buffer.allocUnsafe(buf.length - 1);
                fwd[0] = 0xA3;
                buf.copy(fwd, 1, 2);
                target.send(fwd);
            }
        }

        else if (type === 0x04) {                     // CLIENT → HOST
            if (ws._isHost) return;
            const room = rooms.get(ws._room);
            if (!room || room.host.readyState !== WebSocket.OPEN) return;
            const fwd = Buffer.allocUnsafe(buf.length + 1);
            fwd[0] = 0xA3;
            fwd[1] = ws._id;
            buf.copy(fwd, 2, 1);
            room.host.send(fwd);
        }
    });

    ws.on('close', () => {
        if (!ws._room) return;
        const room = rooms.get(ws._room);
        if (!room) return;
        if (ws._isHost) {
            room.clients.forEach(c => {
                if (c.readyState === WebSocket.OPEN) c.send(Buffer.from([0xA2, 0xFF]));
            });
            rooms.delete(ws._room);
            console.log(`[-] Salon fermé : ${ws._room}`);
        } else {
            room.clients.delete(ws._id);
            if (room.host.readyState === WebSocket.OPEN)
                room.host.send(Buffer.from([0xA2, ws._id]));
        }
    });

    ws.on('error', err => console.error('[!]', err.message));
});

httpServer.listen(PORT, () => console.log(`Relay démarré sur le port ${PORT}`));
