// Nebulosa Studio — servidor
// - Sirve index.html
// - Señalización WebSocket (/ws) para reuniones de solo voz con WebRTC
// - GET /ice entrega los servidores STUN/TURN al navegador
// - API de cuadros del Espacio de arte (/api/boards, /api/img, /img, /thumb):
//   los cuadros y sus imágenes se guardan en disco (carpeta ./data, o DATA_DIR)
//   y todos los usuarios pueden verlos y editarlos en vivo.
// - API de Propuesta (/api/stars): pegatinas con un objetivo, guardadas en data/stars.json;
//   las de cada persona forman su constelación y llegan en vivo por el WebSocket.
//
// Uso:  npm install  ->  npm start   (abre http://localhost:3000)
//
// Importante: en hostings con disco temporal (p. ej. el plan gratis de Render) la
// carpeta data se borra al reiniciar. Usa un disco persistente y apunta DATA_DIR a él.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PEERS = 6;               // personas por reunión (malla WebRTC: más de 6 se vuelve pesado)
const CODE_LEN = 6;
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sin I, O, 0, 1 para no confundir
const INDEX = path.join(__dirname, 'index.html');

// Servidores ICE. Por defecto solo STUN público. Si algunas redes no conectan,
// agrega un TURN con la variable ICE_SERVERS (JSON), por ejemplo:
// ICE_SERVERS='[{"urls":"stun:stun.l.google.com:19302"},{"urls":"turn:mi.turn.com:3478","username":"u","credential":"p"}]'
let ICE = [
  { urls: 'stun:stun.l.google.com:19302' },
  { urls: 'stun:stun1.l.google.com:19302' },
];
if (process.env.ICE_SERVERS) {
  try { ICE = JSON.parse(process.env.ICE_SERVERS); }
  catch { console.warn('ICE_SERVERS no es un JSON válido; se usa STUN por defecto.'); }
}

const allowAnyOrigin = process.env.ALLOW_ANY_ORIGIN === '1';

// ---------- Cuadros (Espacio de arte) ----------
const DATA = process.env.DATA_DIR || path.join(__dirname, 'data');
const DIR_BOARDS = path.join(DATA, 'boards');
const DIR_IMG = path.join(DATA, 'img');
const DIR_THUMBS = path.join(DATA, 'thumbs');
for (const d of [DIR_BOARDS, DIR_IMG, DIR_THUMBS]) {
  try { fs.mkdirSync(d, { recursive: true }); }
  catch (e) { console.warn('No se pudo crear ' + d + ': los cuadros solo vivirán en memoria.'); }
}

const MAX_BOARDS = 500;          // cuadros en total
const MAX_ITEMS = 1500;          // elementos por cuadro
const MAX_POINTS = 1000000;      // números de trazos por cuadro
const MAX_IMG = 10 * 1024 * 1024;
const MAX_OPS_BODY = 4 * 1024 * 1024;
const TYPES = new Set(['img', 'rect', 'ellipse', 'text', 'stroke', 'thread']);
const FONTS = new Set(['serif', 'sans', 'mono', 'display']);
const IMG_MIME = { jpg: 'image/jpeg', png: 'image/png', webp: 'image/webp' };

const boards = new Map();        // id -> { id,name,author,owner,created,updated,bg,order:[],items:{},thumb }
const imgExt = new Map();        // hash -> extensión
const saveTimers = new Map();

function loadAll() {
  try {
    for (const f of fs.readdirSync(DIR_BOARDS)) {
      if (!f.endsWith('.json')) continue;
      try {
        const b = JSON.parse(fs.readFileSync(path.join(DIR_BOARDS, f), 'utf8'));
        if (b && /^[a-f0-9]{12}$/.test(b.id) && b.items && Array.isArray(b.order)) boards.set(b.id, b);
      } catch { console.warn('Cuadro dañado omitido: ' + f); }
    }
    for (const f of fs.readdirSync(DIR_IMG)) {
      const m = /^([a-f0-9]{24})\.(jpg|png|webp)$/.exec(f);
      if (m) imgExt.set(m[1], m[2]);
    }
  } catch {}
  console.log('Cuadros cargados: ' + boards.size);
}
loadAll();

function persist(b) {
  clearTimeout(saveTimers.get(b.id));
  saveTimers.set(b.id, setTimeout(() => {
    saveTimers.delete(b.id);
    if (!boards.has(b.id)) return;
    const file = path.join(DIR_BOARDS, b.id + '.json'), tmp = file + '.tmp';
    fs.writeFile(tmp, JSON.stringify(b), (err) => {
      if (err) return console.warn('No se pudo guardar el cuadro ' + b.id);
      fs.rename(tmp, file, () => {});
    });
  }, 800));
}

const clean = (s, max) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
const num = (v, d = 0, lim = 1e7) => (typeof v === 'number' && Number.isFinite(v)) ? Math.max(-lim, Math.min(lim, v)) : d;
const ref = (v) => (typeof v === 'string' && /^[a-z0-9]{6,24}$/.test(v)) ? v : '';   // id de otra pieza (extremo de un hilo)
const gid = (v) => (typeof v === 'string' && /^[a-z0-9]{3,40}$/.test(v)) ? v : '';  // id de grupo
const col = (v) => (typeof v === 'string' && (v === '' || /^#[0-9a-fA-F]{6}$/.test(v))) ? v : '';

function cleanItem(i) {
  if (!i || typeof i !== 'object' || typeof i.id !== 'string' || !/^[a-z0-9]{6,24}$/.test(i.id) || !TYPES.has(i.type)) return null;
  const o = {
    id: i.id, type: i.type,
    x: num(i.x), y: num(i.y),
    w: Math.max(0.5, Math.min(1e6, num(i.w, 100))), h: Math.max(0.5, Math.min(1e6, num(i.h, 100))),
    r: num(i.r, 0, 1000),
    op: Math.max(0, Math.min(1, num(i.op, 1))),
    fill: col(i.fill), stroke: col(i.stroke), sw: Math.max(0, Math.min(500, num(i.sw, 0))),
  };
  // grupos (clasificación de piezas): g = id del grupo, gn = nombre, gc = color
  if (i.type !== 'thread' && gid(i.g)) {
    o.g = i.g; o.gn = String(i.gn == null ? '' : i.gn).slice(0, 40); o.gc = col(i.gc) || '#ffffff';
  }
  if (i.type === 'img') {
    if (typeof i.src !== 'string' || !/^[a-f0-9]{24}$/.test(i.src)) return null;
    o.src = i.src; if (i.flip) o.flip = true;
  } else if (i.type === 'text') {
    o.text = String(i.text == null ? '' : i.text).slice(0, 5000);
    o.size = Math.max(1, Math.min(2000, num(i.size, 32)));
    o.font = FONTS.has(i.font) ? i.font : 'serif';
  } else if (i.type === 'thread') {
    // hilo entre dos piezas (a, b) o con un extremo libre (ax, ay, bx, by son las coordenadas)
    o.a = ref(i.a); o.b = ref(i.b);
    o.ax = num(i.ax); o.ay = num(i.ay); o.bx = num(i.bx); o.by = num(i.by);
  } else if (i.type === 'stroke') {
    if (!Array.isArray(i.pts) || i.pts.length < 2 || i.pts.length % 2 || i.pts.length > 20000) return null;
    o.pts = i.pts.map((v) => num(v, 0, 10));
  }
  return o;
}

function pointsOf(b) {
  let t = 0;
  for (const id of b.order) { const it = b.items[id]; if (it && it.pts) t += it.pts.length; }
  return t;
}

// Aplica cambios (set / del / order / bg) y devuelve lo que realmente quedó, para reenviarlo a los demás.
function applyOps(b, ops) {
  const out = {};
  if (Array.isArray(ops.del)) {
    const del = new Set(ops.del.filter((k) => typeof k === 'string'));
    const gone = [...del].filter((k) => b.items[k]);
    if (gone.length) {
      for (const k of gone) delete b.items[k];
      b.order = b.order.filter((k) => !del.has(k));
      out.del = gone;
    }
  }
  if (Array.isArray(ops.set)) {
    const set = [];
    let pts = pointsOf(b);
    for (const raw of ops.set.slice(0, 400)) {
      const it = cleanItem(raw);
      if (!it) continue;
      const had = b.items[it.id];
      if (!had && b.order.length >= MAX_ITEMS) continue;
      const delta = (it.pts ? it.pts.length : 0) - (had && had.pts ? had.pts.length : 0);
      if (delta > 0 && pts + delta > MAX_POINTS) continue;
      pts += delta;
      if (!had) b.order.push(it.id);
      b.items[it.id] = it;
      set.push(it);
    }
    if (set.length) out.set = set;
  }
  if (Array.isArray(ops.order)) {
    const seen = new Set(), o = [];
    for (const k of ops.order) if (typeof k === 'string' && b.items[k] && !seen.has(k)) { seen.add(k); o.push(k); }
    for (const k of b.order) if (!seen.has(k)) o.push(k);
    b.order = o;
    out.order = o;
  }
  if (typeof ops.bg === 'string' && /^#[0-9a-fA-F]{6}$/.test(ops.bg)) { b.bg = ops.bg.toLowerCase(); out.bg = b.bg; }
  if (out.set && !out.order) { /* los nuevos se anexan al final igual en todos los clientes */ }
  return out;
}

// ---------- Propuesta: pegatinas / constelaciones ----------
// Cada pegatina es una estrella con un objetivo escrito. Las de una misma persona forman su constelación.
const FILE_STARS = path.join(DATA, 'stars.json');
const MAX_STARS = 1500;            // pegatinas en total
const MAX_STARS_OWNER = 40;        // por persona
const MAX_STAR_TEXT = 140;
const STAR_KINDS = new Set(['star', 'moon', 'planet', 'rocket', 'flag', 'heart', 'bolt', 'sun']);
const stars = new Map();           // id -> { id,owner,g,author,color,kind,text,x,y,t }
let starsTimer = null;

const groupOf = (owner) => crypto.createHash('sha256').update('nebulosa-g:' + owner).digest('hex').slice(0, 10);

function loadStars() {
  try {
    const arr = JSON.parse(fs.readFileSync(FILE_STARS, 'utf8'));
    for (const s of Array.isArray(arr) ? arr : []) {
      if (!s || !/^[a-f0-9]{12}$/.test(s.id) || typeof s.owner !== 'string' || s.owner.length < 16 || !STAR_KINDS.has(s.kind)) continue;
      const owner = s.owner.slice(0, 64);
      stars.set(s.id, {
        id: s.id, owner, g: groupOf(owner),
        author: clean(s.author, 40) || 'Anónimo', color: col(s.color) || '#ffffff', kind: s.kind,
        text: clean(s.text, MAX_STAR_TEXT), x: num(s.x, 0, 20000), y: num(s.y, 0, 20000), t: num(s.t, Date.now(), 1e15),
      });
    }
  } catch {}
  console.log('Pegatinas cargadas: ' + stars.size);
}
loadStars();

function persistStars() {
  clearTimeout(starsTimer);
  starsTimer = setTimeout(() => {
    starsTimer = null;
    const tmp = FILE_STARS + '.tmp';
    fs.writeFile(tmp, JSON.stringify([...stars.values()]), (err) => {
      if (err) return console.warn('No se pudieron guardar las pegatinas');
      fs.rename(tmp, FILE_STARS, () => {});
    });
  }, 800);
}

function starOut(s, owner) {
  return { id: s.id, g: s.g, author: s.author, color: s.color, kind: s.kind, text: s.text, x: s.x, y: s.y, t: s.t, mine: !!owner && s.owner === owner };
}

function starsOf(owner) { const out = []; for (const s of stars.values()) if (s.owner === owner) out.push(s); return out; }

// Avisa en vivo a quienes están mirando el cielo de propuestas
function broadcastStars(msg) {
  for (const c of wss.clients) if (c.pwatch) send(c, msg);
}

// ---------- utilidades HTTP ----------
function json(res, code, obj, headers) {
  res.writeHead(code, { ...headers, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = []; let n = 0, over = false;
    req.on('data', (c) => { n += c.length; if (n > limit) { over = true; return; } if (!over) chunks.push(c); });
    req.on('end', () => over ? reject(Object.assign(new Error('grande'), { code: 413 })) : resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

const hits = new Map();
function rate(key, max, ms) {
  const now = Date.now();
  const arr = (hits.get(key) || []).filter((t) => now - t < ms);
  if (arr.length >= max) { hits.set(key, arr); return false; }
  arr.push(now); hits.set(key, arr); return true;
}
setInterval(() => { const now = Date.now(); for (const [k, a] of hits) if (!a.some((t) => now - t < 3600000)) hits.delete(k); }, 600000);

function imgKind(b) {
  if (b.length < 16) return null;
  if (b[0] === 0xFF && b[1] === 0xD8 && b[2] === 0xFF) return 'jpg';
  if (b[0] === 0x89 && b.toString('latin1', 1, 4) === 'PNG') return 'png';
  if (b.toString('latin1', 0, 4) === 'RIFF' && b.toString('latin1', 8, 12) === 'WEBP') return 'webp';
  return null;
}

function sendFile(res, file, type, headers, cache) {
  const s = fs.createReadStream(file);
  s.on('open', () => { res.writeHead(200, { ...headers, 'Content-Type': type, 'Cache-Control': cache }); s.pipe(res); });
  s.on('error', () => json(res, 404, { error: 'No encontrado' }, headers));
}

function listItem(b, owner) {
  return { id: b.id, name: b.name, author: b.author, updated: b.updated, count: b.order.length, thumb: b.thumb || 0, color: b.color || '', mine: !!owner && b.owner === owner };
}

async function handleApi(req, res, url, headers) {
  const m = req.method;
  const origin = req.headers.origin;
  if (m !== 'GET' && m !== 'HEAD' && origin && !allowAnyOrigin) {
    let ok = false;
    try { ok = new URL(origin).host === req.headers.host; } catch {}
    if (!ok) return json(res, 403, { error: 'Origen no permitido' }, headers);
  }
  const owner = String(req.headers['x-owner'] || '').slice(0, 64);
  const ip = (String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '?';
  let r;

  if (url === '/api/owner/profile') {
    if (m !== 'POST') return json(res, 405, { error: 'Método no permitido' }, headers);
    if (owner.length < 16) return json(res, 400, { error: 'Falta identificador' }, headers);
    if (!rate('pf:' + ip, 60, 600000)) return json(res, 429, { error: 'Demasiados cambios seguidos' }, headers);
    let body;
    try { body = JSON.parse((await readBody(req, 4096)).toString('utf8')); } catch { return json(res, 400, { error: 'Datos inválidos' }, headers); }
    const author = clean(body && body.name, 40) || 'Anónimo';
    const color = (body && typeof body.color === 'string' && /^#[0-9a-fA-F]{6}$/.test(body.color)) ? body.color.toLowerCase() : '';
    let n = 0;
    const ids = [], sids = [];
    for (const b of boards.values()) {
      if (b.owner !== owner) continue;
      b.author = author;
      if (color) b.color = color;
      persist(b); n++; ids.push(b.id);
    }
    for (const s of starsOf(owner)) { s.author = author; if (color) s.color = color; sids.push(s.id); }
    if (sids.length) persistStars();
    if (ids.length || sids.length) broadcastProfile(ids, color || '', author, null, sids);
    return json(res, 200, { ok: true, updated: n }, headers);
  }

  if (url === '/api/stars') {
    if (m === 'GET') {
      return json(res, 200, { stars: [...stars.values()].sort((a, b) => a.t - b.t).map((s) => starOut(s, owner)) }, headers);
    }
    if (m === 'POST') {
      if (owner.length < 16) return json(res, 400, { error: 'Falta identificador' }, headers);
      if (!rate('st:' + ip, 120, 600000)) return json(res, 429, { error: 'Demasiadas pegatinas seguidas' }, headers);
      let body;
      try { body = JSON.parse((await readBody(req, 4096)).toString('utf8')); } catch { return json(res, 400, { error: 'Datos inválidos' }, headers); }
      const text = clean(body && body.text, MAX_STAR_TEXT);
      if (!text) return json(res, 400, { error: 'Escribe tu objetivo' }, headers);
      if (!STAR_KINDS.has(body.kind)) return json(res, 400, { error: 'Pegatina no válida' }, headers);
      if (stars.size >= MAX_STARS) return json(res, 507, { error: 'El cielo está lleno' }, headers);
      if (starsOf(owner).length >= MAX_STARS_OWNER) return json(res, 400, { error: 'Llegaste al máximo de ' + MAX_STARS_OWNER + ' pegatinas' }, headers);
      const s = {
        id: crypto.randomBytes(6).toString('hex'), owner, g: groupOf(owner),
        author: clean(body.name, 40) || 'Anónimo', color: col(body.color) || '#ffffff', kind: body.kind,
        text, x: num(body.x, 0, 20000), y: num(body.y, 0, 20000), t: Date.now(),
      };
      stars.set(s.id, s); persistStars();
      broadcastStars({ type: 'p-add', star: starOut(s, '') });
      return json(res, 201, starOut(s, owner), headers);
    }
    return json(res, 405, { error: 'Método no permitido' }, headers);
  }

  if ((r = /^\/api\/stars\/([a-f0-9]{12})$/.exec(url))) {
    const s = stars.get(r[1]);
    if (!s) return json(res, 404, { error: 'Esa pegatina ya no existe' }, headers);
    if (!owner || s.owner !== owner) return json(res, 403, { error: 'Solo quien la pegó puede cambiarla' }, headers);
    if (m === 'DELETE') {
      stars.delete(s.id); persistStars();
      broadcastStars({ type: 'p-del', id: s.id });
      return json(res, 200, { ok: true }, headers);
    }
    if (m === 'POST') {
      if (!rate('stm:' + ip, 300, 60000)) return json(res, 429, { error: 'Demasiados cambios seguidos' }, headers);
      let body;
      try { body = JSON.parse((await readBody(req, 4096)).toString('utf8')); } catch { return json(res, 400, { error: 'Datos inválidos' }, headers); }
      if (!body || typeof body !== 'object') return json(res, 400, { error: 'Datos inválidos' }, headers);
      if (body.text !== undefined) {
        const text = clean(body.text, MAX_STAR_TEXT);
        if (!text) return json(res, 400, { error: 'Escribe tu objetivo' }, headers);
        s.text = text;
      }
      if (typeof body.x === 'number') s.x = num(body.x, s.x, 20000);
      if (typeof body.y === 'number') s.y = num(body.y, s.y, 20000);
      persistStars();
      broadcastStars({ type: 'p-upd', star: starOut(s, '') });
      return json(res, 200, starOut(s, owner), headers);
    }
    return json(res, 405, { error: 'Método no permitido' }, headers);
  }

  if (url === '/api/boards') {
    if (m === 'GET') {
      const list = [...boards.values()].sort((a, b) => b.updated - a.updated).map((b) => listItem(b, owner));
      return json(res, 200, { boards: list }, headers);
    }
    if (m === 'POST') {
      if (owner.length < 16) return json(res, 400, { error: 'Falta identificador' }, headers);
      if (!rate('mk:' + ip, 10, 3600000)) return json(res, 429, { error: 'Demasiados cuadros nuevos' }, headers);
      if (boards.size >= MAX_BOARDS) return json(res, 507, { error: 'Se alcanzó el máximo de cuadros' }, headers);
      let body;
      try { body = JSON.parse((await readBody(req, 4096)).toString('utf8')); } catch { return json(res, 400, { error: 'Datos inválidos' }, headers); }
      const name = clean(body && body.name, 60), author = clean(body && body.author, 40) || 'Anónimo';
      if (!name) return json(res, 400, { error: 'Falta el nombre del cuadro' }, headers);
      const color = (body && typeof body.color === 'string' && /^#[0-9a-fA-F]{6}$/.test(body.color)) ? body.color.toLowerCase() : '';
      const now = Date.now();
      const b = { id: crypto.randomBytes(6).toString('hex'), name, author, color, owner, created: now, updated: now, bg: '#08080a', order: [], items: {}, thumb: 0 };
      boards.set(b.id, b); persist(b); notifyList();
      return json(res, 201, listItem(b, owner), headers);
    }
    return json(res, 405, { error: 'Método no permitido' }, headers);
  }

  if ((r = /^\/api\/boards\/([a-f0-9]{12})$/.exec(url))) {
    const b = boards.get(r[1]);
    if (!b) return json(res, 404, { error: 'No existe ese cuadro' }, headers);
    if (m === 'GET') {
      return json(res, 200, { ...listItem(b, owner), bg: b.bg, items: b.order.map((k) => b.items[k]).filter(Boolean) }, headers);
    }
    if (m === 'DELETE') {
      if (!owner || b.owner !== owner) return json(res, 403, { error: 'Solo quien lo creó puede borrarlo' }, headers);
      boards.delete(b.id);
      clearTimeout(saveTimers.get(b.id)); saveTimers.delete(b.id);
      fs.unlink(path.join(DIR_BOARDS, b.id + '.json'), () => {});
      fs.unlink(path.join(DIR_THUMBS, b.id + '.jpg'), () => {});
      for (const s of [...(boardSubs.get(b.id) || [])]) send(s, { type: 'b-gone', id: b.id });
      notifyList();
      return json(res, 200, { ok: true }, headers);
    }
    return json(res, 405, { error: 'Método no permitido' }, headers);
  }

  if ((r = /^\/api\/boards\/([a-f0-9]{12})\/ops$/.exec(url))) {
    if (m !== 'POST') return json(res, 405, { error: 'Método no permitido' }, headers);
    const b = boards.get(r[1]);
    if (!b) return json(res, 404, { error: 'No existe ese cuadro' }, headers);
    if (!rate('ops:' + ip, 600, 60000)) return json(res, 429, { error: 'Demasiados cambios seguidos' }, headers);
    let body;
    try { body = JSON.parse((await readBody(req, MAX_OPS_BODY)).toString('utf8')); }
    catch (e) { return json(res, e.code === 413 ? 413 : 400, { error: e.code === 413 ? 'Cambio demasiado grande' : 'Datos inválidos' }, headers); }
    if (!body || typeof body.ops !== 'object' || !body.ops) return json(res, 400, { error: 'Datos inválidos' }, headers);
    const out = applyOps(b, body.ops);
    if (Object.keys(out).length) {
      b.updated = Date.now(); persist(b);
      const cid = String(body.cid || '').slice(0, 40);
      for (const s of boardSubs.get(b.id) || []) if (s.cid !== cid) send(s, { type: 'b-ops', id: b.id, ops: out });
    }
    return json(res, 200, { ok: true, v: 2 }, headers);
  }

  if ((r = /^\/api\/boards\/([a-f0-9]{12})\/thumb$/.exec(url))) {
    if (m !== 'POST') return json(res, 405, { error: 'Método no permitido' }, headers);
    const b = boards.get(r[1]);
    if (!b) return json(res, 404, { error: 'No existe ese cuadro' }, headers);
    if (!rate('th:' + ip, 60, 60000)) return json(res, 429, { error: 'Demasiadas miniaturas' }, headers);
    let buf;
    try { buf = await readBody(req, 300 * 1024); } catch { return json(res, 413, { error: 'Miniatura demasiado grande' }, headers); }
    if (imgKind(buf) !== 'jpg') return json(res, 400, { error: 'La miniatura debe ser JPEG' }, headers);
    fs.writeFile(path.join(DIR_THUMBS, b.id + '.jpg'), buf, (err) => {
      if (err) return json(res, 500, { error: 'No se pudo guardar' }, headers);
      b.thumb = Date.now(); persist(b); notifyList();
      json(res, 200, { ok: true }, headers);
    });
    return;
  }

  if (url === '/api/img') {
    if (m !== 'POST') return json(res, 405, { error: 'Método no permitido' }, headers);
    if (!rate('img:' + ip, 80, 600000)) return json(res, 429, { error: 'Demasiadas imágenes seguidas' }, headers);
    let buf;
    try { buf = await readBody(req, MAX_IMG); } catch { return json(res, 413, { error: 'La imagen pesa más de 10 MB' }, headers); }
    const kind = imgKind(buf);
    if (!kind) return json(res, 400, { error: 'Formato de imagen no permitido' }, headers);
    const id = crypto.createHash('sha256').update(buf).digest('hex').slice(0, 24);
    if (imgExt.has(id)) return json(res, 200, { id }, headers);
    fs.writeFile(path.join(DIR_IMG, id + '.' + kind), buf, (err) => {
      if (err) return json(res, 500, { error: 'No se pudo guardar la imagen' }, headers);
      imgExt.set(id, kind);
      json(res, 200, { id }, headers);
    });
    return;
  }

  if ((r = /^\/img\/([a-f0-9]{24})$/.exec(url)) && (m === 'GET' || m === 'HEAD')) {
    const ext = imgExt.get(r[1]);
    if (!ext) return json(res, 404, { error: 'No encontrado' }, headers);
    return sendFile(res, path.join(DIR_IMG, r[1] + '.' + ext), IMG_MIME[ext], headers, 'public, max-age=31536000, immutable');
  }

  if ((r = /^\/thumb\/([a-f0-9]{12})\.jpg$/.exec(url)) && (m === 'GET' || m === 'HEAD')) {
    if (!boards.has(r[1])) return json(res, 404, { error: 'No encontrado' }, headers);
    return sendFile(res, path.join(DIR_THUMBS, r[1] + '.jpg'), 'image/jpeg', headers, 'public, max-age=86400');
  }

  json(res, 404, { error: 'No encontrado' }, headers);
}

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  const headers = {
    'X-Content-Type-Options': 'nosniff',
    'Permissions-Policy': 'camera=(), microphone=(self)',
    'Cache-Control': 'no-cache',
  };

  if (url.startsWith('/api/') || url.startsWith('/img/') || url.startsWith('/thumb/')) {
    handleApi(req, res, url, headers).catch((err) => {
      console.error(err);
      if (!res.headersSent) json(res, 500, { error: 'Error interno' }, headers);
      else res.end();
    });
    return;
  }

  if (url === '/' || url === '/index.html') {
    fs.readFile(INDEX, (err, data) => {
      if (err) {
        res.writeHead(500, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('No se encontró index.html junto a server.js');
        return;
      }
      res.writeHead(200, { ...headers, 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else if (url === '/style.css' || url === '/script.js') {
    const css = url === '/style.css';
    fs.readFile(path.join(__dirname, url.slice(1)), (err, data) => {
      if (err) {
        res.writeHead(404, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('No encontrado');
        return;
      }
      res.writeHead(200, { ...headers, 'Content-Type': css ? 'text/css; charset=utf-8' : 'application/javascript; charset=utf-8' });
      res.end(data);
    });
  } else if (url === '/ice') {
    res.writeHead(200, { ...headers, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ iceServers: ICE }));
  } else if (url === '/health') {
    res.writeHead(200, { ...headers, 'Content-Type': 'text/plain' });
    res.end('ok');
  } else {
    res.writeHead(404, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('No encontrado');
  }
});

// ---------- Salas ----------
const rooms = new Map(); // código -> Map(id -> ws)
const boardSubs = new Map(); // id de cuadro -> Set(ws) que lo tienen abierto

function newCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < CODE_LEN; i++) c += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(c)) return c;
  }
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function joinRoom(ws, code) {
  const room = rooms.get(code);
  send(ws, { type: 'joined', code, id: ws.id, peers: [...room.keys()] });
  for (const peer of room.values()) send(peer, { type: 'peer-joined', id: ws.id });
  room.set(ws.id, ws);
  ws.room = code;
}

function leaveRoom(ws) {
  if (!ws.room) return;
  const code = ws.room;
  ws.room = null;
  const room = rooms.get(code);
  if (!room) return;
  room.delete(ws.id);
  for (const peer of room.values()) send(peer, { type: 'peer-left', id: ws.id });
  if (room.size === 0) rooms.delete(code);
}

function broadcastCount(id) {
  const set = boardSubs.get(id);
  if (!set) return;
  for (const s of set) send(s, { type: 'b-count', id, n: set.size });
}

// La lista de cuadros cambió (nuevo, borrado, miniatura): quienes miran la galería la recargan en vivo
let listT = null;
function notifyList() {
  if (listT) return;
  listT = setTimeout(() => {
    listT = null;
    for (const c of wss.clients) if (c.watch) send(c, { type: 'b-list' });
  }, 300);
}

// Cambio de nombre/color de un dueño: lo reciben en vivo quienes miran la galería o tienen un cuadro abierto
function broadcastProfile(ids, color, author, except, sids) {
  const msg = { type: 'b-profile', ids, color, author, sids: sids || [] };
  for (const c of wss.clients) {
    if (c === except || !(c.watch || c.board)) continue;
    send(c, msg);
  }
}

function leaveBoard(ws) {
  const id = ws.board;
  if (!id) return;
  ws.board = null;
  const set = boardSubs.get(id);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) boardSubs.delete(id); else broadcastCount(id);
}

// ---------- WebSocket ----------
const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: 64 * 1024,
  verifyClient: ({ origin, req }) => {
    if (allowAnyOrigin || !origin) return true;
    try { return new URL(origin).host === req.headers.host; } catch { return false; }
  },
});

wss.on('connection', (ws) => {
  ws.id = crypto.randomBytes(4).toString('hex');
  ws.room = null;
  ws.board = null;
  ws.cid = '';
  ws.alive = true;
  ws.count = 0;

  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', (raw) => {
    if (++ws.count > 400) { ws.close(1008, 'Demasiados mensajes'); return; }
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.type !== 'string') return;

    switch (m.type) {
      case 'create': {
        if (ws.room) return;
        const code = newCode();
        rooms.set(code, new Map());
        const room = rooms.get(code);
        room.set(ws.id, ws);
        ws.room = code;
        send(ws, { type: 'created', code, id: ws.id });
        break;
      }
      case 'join': {
        if (ws.room) return;
        const code = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
        const room = rooms.get(code);
        if (!room) { send(ws, { type: 'error', message: 'No existe una reunión con ese código' }); return; }
        if (room.size >= MAX_PEERS) { send(ws, { type: 'error', message: 'La reunión está llena' }); return; }
        joinRoom(ws, code);
        break;
      }
      case 'signal': {
        // reenvía ofertas/respuestas/candidatos solo a alguien de la misma sala
        if (!ws.room || typeof m.to !== 'string' || typeof m.data !== 'object' || !m.data) return;
        const target = rooms.get(ws.room)?.get(m.to);
        if (target) send(target, { type: 'signal', from: ws.id, data: m.data });
        break;
      }
      case 'b-join': {
        // abre un cuadro: recibirá los cambios que hagan los demás
        const id = String(m.id || '');
        if (!boards.has(id)) { send(ws, { type: 'b-gone', id }); return; }
        leaveBoard(ws);
        const set = boardSubs.get(id) || new Set();
        if (set.size >= 100) return;
        set.add(ws);
        boardSubs.set(id, set);
        ws.board = id;
        ws.cid = String(m.cid || '').slice(0, 40);
        broadcastCount(id);
        break;
      }
      case 'b-leave': {
        leaveBoard(ws);
        break;
      }
      case 'g-watch': {
        // quiere enterarse en vivo de los cambios de nombre/color de otros usuarios
        ws.watch = true;
        break;
      }
      case 'p-watch': {
        // está mirando el cielo de propuestas: recibe pegatinas nuevas, movidas y borradas
        ws.pwatch = true;
        break;
      }
      case 'g-profile': {
        // cambio de color/nombre en vivo (mientras se arrastra el selector); se guarda con POST /api/owner/profile
        const owner = String(m.owner || '').slice(0, 64);
        if (owner.length < 16 || typeof m.color !== 'string' || !/^#[0-9a-fA-F]{6}$/.test(m.color)) return;
        const now = Date.now();
        if (now - (ws.lastProfile || 0) < 60) return;
        ws.lastProfile = now;
        const color = m.color.toLowerCase();
        const author = clean(m.name, 40) || 'Anónimo';
        const ids = [], sids = [];
        for (const b of boards.values()) {
          if (b.owner !== owner) continue;
          b.color = color; b.author = author; ids.push(b.id);
        }
        for (const s of starsOf(owner)) { s.color = color; s.author = author; sids.push(s.id); }
        if (ids.length || sids.length) broadcastProfile(ids, color, author, ws, sids);
        break;
      }
    }
  });

  ws.on('close', () => { leaveBoard(ws); leaveRoom(ws); });
  ws.on('error', () => {});
});

// Limpia conexiones muertas y reinicia el contador de mensajes
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.count = 0;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Nebulosa Studio listo en http://localhost:${PORT}`);
});
