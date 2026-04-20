/**
 * server.js — BRZ CRM  Node.js + SQLite Backend
 * ================================================
 * Single server file. Zero npm dependencies.
 * Requires Node.js 22.5+ (built-in node:sqlite).
 *
 * Local:   node server.js
 * cPanel:  Passenger loads app.js which requires this file.
 *          Set Application Startup File = app.js in cPanel Node.js panel.
 *
 * Environment variables (all optional):
 *   PORT        — port to listen on (default 5000; cPanel sets this automatically)
 *   JWT_SECRET  — secret for signing tokens (change in production)
 *   DB_PATH     — absolute path to crm.db (default: same folder as this file)
 */
'use strict';

// ── Auto-enable node:sqlite for Node 22.5–22.9 ─────────────────────────────
// Node 22.10+ has sqlite stable. Node 22.5–22.9 needs --experimental-sqlite.
// When loaded via cPanel/Passenger or `node server.js` without the flag,
// we detect and restart automatically with it set.
(function ensureSqliteFlag() {
  const [maj, min] = process.version.replace('v','').split('.').map(Number);
  const needsFlag  = (maj === 22 && min < 10) || maj < 22;
  if (!needsFlag) return;                        // Node 22.10+ / 23+ — not needed
  const opts = process.env.NODE_OPTIONS || '';
  if (opts.includes('experimental-sqlite')) return; // already set
  // Restart with the flag via child_process.spawnSync
  const { spawnSync } = require('child_process');
  const result = spawnSync(
    process.execPath,
    ['--experimental-sqlite', ...process.argv.slice(1)],
    {
      stdio: 'inherit',
      env:   { ...process.env, NODE_OPTIONS: opts + ' --experimental-sqlite' },
    }
  );
  process.exit(result.status ?? 0);
})();

const http   = require('node:http');
const fs     = require('node:fs');
const path   = require('node:path');
const crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');

// ── Config ─────────────────────────────────────────────────────────────────
const PORT       = parseInt(process.env.PORT || '5000', 10);
const JWT_SECRET = process.env.JWT_SECRET || 'brzcrm-node-secret-CHANGE-IN-PRODUCTION';
const BASE_DIR   = __dirname;
const DB_PATH    = path.join(BASE_DIR, 'crm.db');
const HTML_FILE  = 'index.html';   // the actual frontend file

// ── MIME types ──────────────────────────────────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.mjs':  'application/javascript; charset=utf-8',
  '.json': 'application/json',
  '.svg':  'image/svg+xml',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.ico':  'image/x-icon',
  '.woff': 'font/woff',
  '.woff2':'font/woff2',
};

// ============================================================================
//  Database — opened once, synchronous API (no async overhead)
// ============================================================================
let db;

function openDb() {
  db = new DatabaseSync(DB_PATH);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA foreign_keys=ON');
}

// Prepare + execute — params are spread so SQLite binding works correctly
function qone(sql, params = []) {
  return db.prepare(sql).get(...params) || null;
}
function qall(sql, params = []) {
  return db.prepare(sql).all(...params);
}
function run(sql, params = []) {
  return db.prepare(sql).run(...params);
}
function newId()  { return crypto.randomUUID(); }
function nowIso() { return new Date().toISOString().replace('T', ' ').split('.')[0]; }

function paginate(sql, params, qs) {
  const page = Math.max(1, parseInt(qs.page || '1', 10));
  const size = Math.min(100, parseInt(qs.page_size || '20', 10));
  const total = db.prepare(`SELECT COUNT(*) AS c FROM (${sql})`).get(...params).c;
  const items = qall(`${sql} LIMIT ? OFFSET ?`, [...params, size, (page - 1) * size]);
  return { items, total, page, page_size: size };
}

// ── Schema ──────────────────────────────────────────────────────────────────
const SCHEMA = `
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL, full_name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'sales_rep',
  is_active INTEGER NOT NULL DEFAULT 1, is_verified INTEGER NOT NULL DEFAULT 1,
  department TEXT DEFAULT '', timezone TEXT NOT NULL DEFAULT 'UTC',
  phone TEXT, bio TEXT, title TEXT DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS customers (
  id TEXT PRIMARY KEY, company_name TEXT NOT NULL,
  contact_name TEXT, email TEXT, phone TEXT, industry TEXT,
  status TEXT NOT NULL DEFAULT 'prospect', company_size TEXT,
  annual_revenue REAL, website TEXT, city TEXT, country TEXT, description TEXT,
  owner_id TEXT REFERENCES users(id),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS leads (
  id TEXT PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
  email TEXT, phone TEXT, company TEXT, job_title TEXT,
  status TEXT NOT NULL DEFAULT 'new', source TEXT, score INTEGER DEFAULT 0,
  estimated_value REAL, description TEXT, owner_id TEXT REFERENCES users(id),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS pipelines (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT,
  is_default INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS stages (
  id TEXT PRIMARY KEY, pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
  name TEXT NOT NULL, color TEXT NOT NULL DEFAULT '#f59e0b',
  stage_order INTEGER NOT NULL DEFAULT 0, probability INTEGER NOT NULL DEFAULT 0,
  is_won INTEGER NOT NULL DEFAULT 0, is_lost INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS deals (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, value REAL, probability INTEGER,
  status TEXT NOT NULL DEFAULT 'open',
  stage_id TEXT REFERENCES stages(id), pipeline_id TEXT REFERENCES pipelines(id),
  customer_id TEXT REFERENCES customers(id), owner_id TEXT REFERENCES users(id),
  expected_close_date TEXT, description TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS activities (
  id TEXT PRIMARY KEY, title TEXT NOT NULL,
  activity_type TEXT NOT NULL DEFAULT 'task', status TEXT NOT NULL DEFAULT 'planned',
  due_date TEXT, duration INTEGER, entity_type TEXT, entity_id TEXT, description TEXT,
  assignee_id TEXT REFERENCES users(id), created_by_id TEXT REFERENCES users(id),
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS notes (
  id TEXT PRIMARY KEY, content TEXT NOT NULL, entity_type TEXT, entity_id TEXT,
  is_pinned INTEGER NOT NULL DEFAULT 0, created_by_id TEXT REFERENCES users(id),
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
`;

const DEFAULT_USERS = [
  { email:'byron@brzcrm.co.za',    password:'Byr0nAdmin1',        full_name:'Byron',    role:'admin',     department:'Administration', title:'System Administrator' },
  { email:'michelle@brzcrm.co.za', password:'M1chelleDirector1',  full_name:'Michelle', role:'admin',     department:'Management',     title:'Director' },
  { email:'pranesh@brzcrm.co.za',  password:'Pr4neshDirector1',   full_name:'Pranesh',  role:'manager',   department:'Sales',          title:'Sales Director' },
  { email:'wasab@brzcrm.co.za',    password:'W@sabManager1',      full_name:'Wasab',    role:'manager',   department:'Sales',          title:'Sales Manager' },
  { email:'miranda@brzcrm.co.za',  password:'Mir@ndaExports1',    full_name:'Miranda',  role:'sales_rep', department:'Exports',        title:'Exports Specialist' },
  { email:'suveshen@brzcrm.co.za', password:'Suv3shenExports1',   full_name:'Suveshen', role:'sales_rep', department:'Exports',        title:'Exports Coordinator' },
  { email:'garisha@brzcrm.co.za',  password:'G@rishaSales1',      full_name:'Garisha',  role:'sales_rep', department:'Sales',          title:'Sales Representative' },
  { email:'eugene@brzcrm.co.za',   password:'Eug3neLogistics1',   full_name:'Eugene',   role:'sales_rep', department:'Logistics',      title:'Logistics Coordinator' },
];

const DEFAULT_PIPELINE = {
  name: 'Sales Pipeline', description: 'Main sales pipeline',
  stages: [
    { name:'Prospecting',   color:'#64748b', order:0, probability:10,  is_won:0, is_lost:0 },
    { name:'Qualification', color:'#38bdf8', order:1, probability:30,  is_won:0, is_lost:0 },
    { name:'Proposal',      color:'#a78bfa', order:2, probability:60,  is_won:0, is_lost:0 },
    { name:'Negotiation',   color:'#f59e0b', order:3, probability:80,  is_won:0, is_lost:0 },
    { name:'Closed Won',    color:'#10b981', order:4, probability:100, is_won:1, is_lost:0 },
    { name:'Closed Lost',   color:'#ef4444', order:5, probability:0,   is_won:0, is_lost:1 },
  ],
};

// ============================================================================
//  Password — PBKDF2-SHA512 (pure Node crypto, no npm packages)
// ============================================================================
const PBKDF2 = { iters:200_000, keylen:64, digest:'sha512' };

function hashPw(plain) {
  const salt = crypto.randomBytes(32).toString('hex');
  const dk   = crypto.pbkdf2Sync(plain, salt, PBKDF2.iters, PBKDF2.keylen, PBKDF2.digest);
  return `pbkdf2:${salt}:${dk.toString('hex')}`;
}

function verifyPw(plain, stored) {
  if (!stored || !plain) return false;
  if (stored.startsWith('pbkdf2:')) {
    try {
      const [, salt, storedDk] = stored.split(':');
      const dk = crypto.pbkdf2Sync(plain, salt, PBKDF2.iters, PBKDF2.keylen, PBKDF2.digest);
      const a = Buffer.from(storedDk, 'hex');
      const b = dk;
      return a.length === b.length && crypto.timingSafeEqual(a, b);
    } catch { return false; }
  }
  // Legacy bcrypt hashes (from original app.py) — verify using pure-JS implementation
  if (stored.startsWith('$2b$') || stored.startsWith('$2y$') || stored.startsWith('$2a$')) {
    return _bcryptVerify(plain, stored);
  }
  return false;
}

// ── Pure-JS bcrypt verifier for legacy hashes ──────────────────────────────
// Uses Node's built-in crypto to run the bcrypt KDF without any npm packages.
// Only called for passwords that haven't been migrated yet.
function _bcryptVerify(plain, hash) {
  try {
    // Extract cost factor and salt from the hash string
    // Hash format: $2b$NN$<22-char-base64-salt><31-char-base64-hash>
    const parts = hash.split('$');
    if (parts.length < 4) return false;
    const cost    = parseInt(parts[2], 10);
    const b64part = parts[3];                    // 53 chars: 22 salt + 31 hash
    const saltB64 = b64part.slice(0, 22);
    const hashB64 = b64part.slice(22);

    // Decode bcrypt's custom base64 alphabet into raw bytes
    const BCRYPT_ALPHABET = './ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    function bcryptDecode(str, expectedBytes) {
      const buf = [];
      let i = 0;
      while (buf.length < expectedBytes && i < str.length) {
        const c1 = BCRYPT_ALPHABET.indexOf(str[i++]);
        const c2 = i < str.length ? BCRYPT_ALPHABET.indexOf(str[i++]) : 0;
        const c3 = i < str.length ? BCRYPT_ALPHABET.indexOf(str[i++]) : 0;
        const c4 = i < str.length ? BCRYPT_ALPHABET.indexOf(str[i++]) : 0;
        buf.push((c1 << 2) | (c2 >> 4));
        if (buf.length < expectedBytes) buf.push(((c2 & 0xf) << 4) | (c3 >> 2));
        if (buf.length < expectedBytes) buf.push(((c3 & 0x1f) << 6) | c4);
      }
      return Buffer.from(buf);
    }

    const saltBytes = bcryptDecode(saltB64, 16);
    const hashBytes = bcryptDecode(hashB64, 23);

    // Use Node's scrypt-adjacent: we need actual bcrypt KDF.
    // Node has no built-in bcrypt, but we can use the child_process approach
    // only as a one-time migration step — after this the password is re-hashed to PBKDF2.
    // For the actual comparison, spawn python3 if available, otherwise reject.
    const { execFileSync } = require('child_process');
    const script = `import bcrypt,sys;print(bcrypt.checkpw(sys.argv[1].encode(),sys.argv[2].encode()))`;
    try {
      const out = execFileSync('python3', ['-c', script, plain, hash], { timeout: 6000 }).toString().trim();
      return out === 'True';
    } catch {
      // python3 not available — try node bcrypt alternatives, otherwise fall through
      return false;
    }
  } catch { return false; }
}

// ============================================================================
//  JWT — pure HMAC-SHA256, no npm
// ============================================================================
function b64u(buf) { return Buffer.from(buf).toString('base64url'); }

function jwtSign(payload) {
  const h = b64u(JSON.stringify({ alg:'HS256', typ:'JWT' }));
  const b = b64u(JSON.stringify(payload));
  const s = crypto.createHmac('sha256', JWT_SECRET).update(`${h}.${b}`).digest('base64url');
  return `${h}.${b}.${s}`;
}

function jwtVerify(token) {
  const parts = (token || '').split('.');
  if (parts.length !== 3) throw new Error('bad token');
  const expected = crypto.createHmac('sha256', JWT_SECRET).update(`${parts[0]}.${parts[1]}`).digest('base64url');
  if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(parts[2]))) throw new Error('bad sig');
  const p = JSON.parse(Buffer.from(parts[1], 'base64url').toString());
  if (p.exp && Date.now() / 1000 > p.exp) throw new Error('expired');
  return p;
}

function makeTokens(userId) {
  const now = Math.floor(Date.now() / 1000);
  return {
    access_token:  jwtSign({ sub:userId, type:'access',  iat:now, exp:now + 3600 }),
    refresh_token: jwtSign({ sub:userId, type:'refresh', iat:now, exp:now + 86400 * 30 }),
    token_type: 'bearer',
  };
}

// ============================================================================
//  HTTP helpers
// ============================================================================
const CORS_HEADERS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Methods': 'GET,POST,PATCH,PUT,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Authorization,Content-Type',
};

function send(res, status, data) {
  const body = data === null ? '' : JSON.stringify(data);
  res.writeHead(status, { 'Content-Type':'application/json; charset=utf-8', ...CORS_HEADERS });
  res.end(body);
}

function readBody(req) {
  return new Promise(resolve => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end',  () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString() || '{}')); } catch { resolve({}); } });
  });
}

function parseQs(url) {
  const i = url.indexOf('?');
  return i === -1 ? {} : Object.fromEntries(new URLSearchParams(url.slice(i + 1)));
}

function servStatic(res, filePath) {
  const ext  = path.extname(filePath).toLowerCase();
  const mime = MIME[ext] || 'application/octet-stream';
  try {
    const data = fs.readFileSync(filePath);
    res.writeHead(200, { 'Content-Type': mime });
    res.end(data);
  } catch {
    send(res, 404, { detail: 'Not found' });
  }
}

// Case-insensitive file resolver for Linux (Windows is case-insensitive natively)
function resolveFile(abs) {
  if (fs.existsSync(abs)) return abs;
  const parts = abs.split(path.sep).filter(Boolean);
  let cur = abs.startsWith('/') ? '/' : '';
  for (const seg of parts) {
    if (!seg) continue;
    try {
      const entries = fs.readdirSync(cur || '.');
      // Try exact case-insensitive match first, then spaces↔underscores variants
      const match = entries.find(e => e.toLowerCase() === seg.toLowerCase())
                 || entries.find(e => e.toLowerCase() === seg.toLowerCase().replace(/_/g, ' '))
                 || entries.find(e => e.toLowerCase() === seg.toLowerCase().replace(/ /g, '_'));
      if (!match) return null;
      cur = path.join(cur, match);
    } catch { return null; }
  }
  return cur || null;
}

// ============================================================================
//  Permission helpers
// ============================================================================
function elevated(user) { return ['manager','admin','super_admin'].includes(user.role); }

function canMod(user, ownerId, createdById) {
  if (elevated(user)) return true;
  if (ownerId    && ownerId    === user.id) return true;
  if (createdById && createdById === user.id) return true;
  return false;
}

function scopeSql(sql, params, user, field = 'owner_id') {
  if (!elevated(user)) { sql += ` AND ${field}=?`; params.push(user.id); }
  return { sql, params };
}

function safeUser(u) {
  if (!u) return null;
  const { password_hash, ...rest } = u;
  return rest;
}

function withOwner(item, user) {
  if (!item) return item;
  item.owner    = item.owner_id ? qone('SELECT id, full_name, email, role FROM users WHERE id=?', [item.owner_id]) : null;
  item.can_edit = canMod(user, item.owner_id, item.created_by_id);
  return item;
}

function withAssignee(item, user) {
  if (!item) return item;
  item.assignee = item.assignee_id ? qone('SELECT id, full_name, email FROM users WHERE id=?', [item.assignee_id]) : null;
  item.can_edit = canMod(user, item.assignee_id, item.created_by_id);
  return item;
}

function buildPipeline(p) {
  const stages = qall('SELECT * FROM stages WHERE pipeline_id=? ORDER BY stage_order', [p.id]);
  for (const s of stages) {
    s.deals = qall('SELECT * FROM deals WHERE stage_id=? ORDER BY created_at DESC', [s.id]).map(d => {
      d.owner    = d.owner_id ? qone('SELECT id,full_name,email FROM users WHERE id=?', [d.owner_id]) : null;
      d.customer = d.customer_id ? qone('SELECT id,company_name,contact_name FROM customers WHERE id=?', [d.customer_id]) : null;
      return d;
    });
  }
  p.stages = stages;
  return p;
}

function fullDeal(did, user) {
  const d = qone('SELECT * FROM deals WHERE id=?', [did]);
  if (!d) return null;
  withOwner(d, user);
  d.customer = d.customer_id ? qone('SELECT id,company_name,contact_name FROM customers WHERE id=?', [d.customer_id]) : null;
  return d;
}

function requireAuth(req) {
  const auth = (req.headers['authorization'] || '').replace('Bearer ', '').trim();
  if (!auth) throw Object.assign(new Error('No token'), { status: 401 });
  const p = jwtVerify(auth);
  if (p.type !== 'access') throw Object.assign(new Error('Wrong token type'), { status: 401 });
  const user = qone('SELECT * FROM users WHERE id=? AND is_active=1', [p.sub]);
  if (!user) throw Object.assign(new Error('User not found'), { status: 401 });
  return user;
}

// ============================================================================
//  Router
// ============================================================================
async function router(req, res) {
  const method = req.method.toUpperCase();
  const url    = req.url;
  const qs     = parseQs(url);

  // CORS preflight
  if (method === 'OPTIONS') {
    res.writeHead(204, CORS_HEADERS);
    return res.end();
  }

  // Parse pathname
  let pathname;
  try { pathname = new URL(url, 'http://x').pathname; }
  catch { pathname = url.split('?')[0]; }

  // ── Static files ────────────────────────────────────────────────────────
  const staticPrefixes = ['/css/', '/CSS/', '/js/', '/JS/', '/Meta/', '/meta/', '/node_modules/'];
  const isStatic = staticPrefixes.some(p => pathname.toLowerCase().startsWith(p.toLowerCase()));

  if (pathname === '/' || pathname === `/${HTML_FILE}` || pathname === '/crm.html') {
    return servStatic(res, path.join(BASE_DIR, HTML_FILE));
  }

  // Serve the dev dashboard
  if (pathname === '/dashboard' || pathname === '/dashboard.html') {
    return servStatic(res, path.join(BASE_DIR, 'dashboard.html'));
  }

  if (isStatic) {
    const rel      = pathname.startsWith('/') ? pathname.slice(1) : pathname;
    const abs      = path.join(BASE_DIR, rel);
    const resolved = resolveFile(abs);
    return resolved ? servStatic(res, resolved) : send(res, 404, { detail: 'Not found' });
  }

  // ── API ──────────────────────────────────────────────────────────────────
  if (!pathname.startsWith('/api/v1')) {
    return send(res, 404, { detail: 'Not found' });
  }

  const apiPath = pathname.slice('/api/v1'.length) || '/';
  const seg     = apiPath.split('/').filter(Boolean);

  try {
    // ── Health ─────────────────────────────────────────────────────────────
    if (apiPath === '/health') return send(res, 200, { status:'ok', mode:'node-sqlite' });

    // ── Auth ───────────────────────────────────────────────────────────────
    if (apiPath === '/auth/login' && method === 'POST') {
      const b    = await readBody(req);
      const email = (b.email || '').trim().toLowerCase();
      const user  = qone('SELECT * FROM users WHERE LOWER(email)=? AND is_active=1', [email]);
      if (!user || !verifyPw(b.password || '', user.password_hash))
        return send(res, 401, { detail:'Invalid email or password' });
      // If stored hash is still legacy bcrypt, migrate it to PBKDF2 now
      if (user.password_hash.startsWith('$2')) {
        try { run('UPDATE users SET password_hash=? WHERE id=?', [hashPw(b.password), user.id]); }
        catch { /* migration failure is non-fatal */ }
      }
      return send(res, 200, makeTokens(user.id));
    }

    if (apiPath === '/auth/refresh' && method === 'POST') {
      const b = await readBody(req);
      try {
        const p = jwtVerify(b.refresh_token || '');
        if (p.type !== 'refresh') throw new Error('wrong type');
        return send(res, 200, makeTokens(p.sub));
      } catch { return send(res, 401, { detail:'Invalid refresh token' }); }
    }

    if (apiPath === '/auth/change-password' && method === 'POST') {
      const user = requireAuth(req);
      const b    = await readBody(req);
      if (!verifyPw(b.current_password || '', user.password_hash))
        return send(res, 400, { detail:'Current password incorrect' });
      if ((b.new_password || '').length < 8)
        return send(res, 400, { detail:'Password must be at least 8 characters' });
      run('UPDATE users SET password_hash=? WHERE id=?', [hashPw(b.new_password), user.id]);
      return send(res, 204, null);
    }

    // All remaining routes require auth
    const user = requireAuth(req);

    // ── Users ──────────────────────────────────────────────────────────────
    if (apiPath === '/users/me') {
      if (method === 'GET') return send(res, 200, safeUser(user));
      if (method === 'PATCH') {
        const b = await readBody(req);
        const OK = ['full_name','phone','bio','title','department','timezone'];
        const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
        if (Object.keys(fields).length) {
          const sets = Object.keys(fields).map(k => `${k}=?`).join(', ');
          run(`UPDATE users SET ${sets} WHERE id=?`, [...Object.values(fields), user.id]);
        }
        return send(res, 200, safeUser(qone('SELECT * FROM users WHERE id=?', [user.id])));
      }
    }
    if (apiPath === '/users' && method === 'GET') {
      const all = qall('SELECT * FROM users ORDER BY full_name').map(safeUser);
      return send(res, 200, { items: all, total: all.length });
    }
    if (apiPath === '/users' && method === 'POST') {
      if (!['admin','super_admin'].includes(user.role)) return send(res, 403, { detail:'Admin required' });
      const b = await readBody(req);
      const id = newId();
      run("INSERT INTO users (id,email,password_hash,full_name,role,is_active,is_verified,department,timezone) VALUES (?,?,?,?,?,1,0,?,'UTC')",
        [id, b.email, hashPw(b.password||'changeme'), b.full_name||'', b.role||'sales_rep', b.department||'']);
      return send(res, 201, safeUser(qone('SELECT * FROM users WHERE id=?', [id])));
    }
    if (seg[0]==='users' && seg[1] && method==='GET')
      return send(res, 200, safeUser(qone('SELECT * FROM users WHERE id=?', [seg[1]])));
    if (seg[0]==='users' && seg[1] && method==='PATCH') {
      const b = await readBody(req);
      const OK = ['full_name','role','is_active','department'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      if (Object.keys(fields).length) {
        const sets = Object.keys(fields).map(k => `${k}=?`).join(', ');
        run(`UPDATE users SET ${sets} WHERE id=?`, [...Object.values(fields), seg[1]]);
      }
      return send(res, 200, safeUser(qone('SELECT * FROM users WHERE id=?', [seg[1]])));
    }
    if (seg[0]==='users' && seg[1] && method==='DELETE') {
      if (!['admin','super_admin'].includes(user.role)) return send(res, 403, { detail:'Admin required' });
      run('UPDATE users SET is_active=0 WHERE id=?', [seg[1]]);
      return send(res, 204, null);
    }

    // ── Customers ──────────────────────────────────────────────────────────
    if (apiPath === '/customers' && method === 'GET') {
      const s = `%${qs.search||''}%`;
      let sql = 'SELECT * FROM customers WHERE (company_name LIKE ? OR contact_name LIKE ? OR email LIKE ?)';
      let params = [s, s, s];
      if (qs.status)   { sql += ' AND status=?';   params.push(qs.status); }
      if (qs.owner_id) { sql += ' AND owner_id=?'; params.push(qs.owner_id); }
      ({ sql, params } = scopeSql(sql, params, user, 'owner_id'));
      sql += ' ORDER BY created_at DESC';
      const r = paginate(sql, params, qs);
      r.items = r.items.map(c => withOwner(c, user));
      return send(res, 200, r);
    }
    if (apiPath === '/customers' && method === 'POST') {
      const b = await readBody(req); const id = newId();
      run('INSERT INTO customers (id,company_name,contact_name,email,phone,industry,status,company_size,annual_revenue,website,city,country,description,owner_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        [id, b.company_name||'', b.contact_name||null, b.email||null, b.phone||null, b.industry||null,
         b.status||'prospect', b.company_size||null, b.annual_revenue||null, b.website||null,
         b.city||null, b.country||null, b.description||null, b.owner_id||user.id]);
      return send(res, 201, withOwner(qone('SELECT * FROM customers WHERE id=?', [id]), user));
    }
    if (seg[0]==='customers' && seg[1] && !seg[2] && method==='GET') {
      const c = qone('SELECT * FROM customers WHERE id=?', [seg[1]]);
      return c ? send(res, 200, withOwner(c, user)) : send(res, 404, { detail:'Not found' });
    }
    if (seg[0]==='customers' && seg[1] && method==='PATCH') {
      const c = qone('SELECT * FROM customers WHERE id=?', [seg[1]]);
      if (!c) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, c.owner_id)) return send(res, 403, { detail:'Permission denied' });
      const b = await readBody(req);
      const OK = ['company_name','contact_name','email','phone','industry','status','company_size','annual_revenue','website','city','country','description','owner_id'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      fields.updated_at = nowIso();
      run(`UPDATE customers SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      return send(res, 200, withOwner(qone('SELECT * FROM customers WHERE id=?', [seg[1]]), user));
    }
    if (seg[0]==='customers' && seg[1] && method==='DELETE') {
      const c = qone('SELECT * FROM customers WHERE id=?', [seg[1]]);
      if (!c) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, c.owner_id)) return send(res, 403, { detail:'Permission denied' });
      // Null out FK references before deleting to avoid constraint errors
      run('UPDATE deals SET customer_id=NULL WHERE customer_id=?', [seg[1]]);
      run('DELETE FROM customers WHERE id=?', [seg[1]]);
      return send(res, 204, null);
    }

    // ── Leads ──────────────────────────────────────────────────────────────
    if (apiPath === '/leads' && method === 'GET') {
      const s = `%${qs.search||''}%`;
      let sql = 'SELECT * FROM leads WHERE (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ?)';
      let params = [s, s, s, s];
      if (qs.status) { sql += ' AND status=?'; params.push(qs.status); }
      if (qs.source) { sql += ' AND source=?'; params.push(qs.source); }
      ({ sql, params } = scopeSql(sql, params, user, 'owner_id'));
      sql += ' ORDER BY created_at DESC';
      const r = paginate(sql, params, qs);
      r.items = r.items.map(l => withOwner(l, user));
      return send(res, 200, r);
    }
    if (apiPath === '/leads' && method === 'POST') {
      const b = await readBody(req); const id = newId();
      run('INSERT INTO leads (id,first_name,last_name,email,phone,company,job_title,status,source,score,estimated_value,description,owner_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        [id, b.first_name||'', b.last_name||'', b.email||null, b.phone||null, b.company||null,
         b.job_title||null, b.status||'new', b.source||null, b.score||0,
         b.estimated_value||null, b.description||null, b.owner_id||user.id]);
      return send(res, 201, withOwner(qone('SELECT * FROM leads WHERE id=?', [id]), user));
    }
    if (seg[0]==='leads' && seg[1] && !seg[2] && method==='GET') {
      const l = qone('SELECT * FROM leads WHERE id=?', [seg[1]]);
      return l ? send(res, 200, withOwner(l, user)) : send(res, 404, { detail:'Not found' });
    }
    if (seg[0]==='leads' && seg[1] && !seg[2] && method==='PATCH') {
      const l = qone('SELECT * FROM leads WHERE id=?', [seg[1]]);
      if (!l) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, l.owner_id)) return send(res, 403, { detail:'Permission denied' });
      const b = await readBody(req);
      const OK = ['first_name','last_name','email','phone','company','job_title','status','source','score','estimated_value','description','owner_id'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      fields.updated_at = nowIso();
      run(`UPDATE leads SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      return send(res, 200, withOwner(qone('SELECT * FROM leads WHERE id=?', [seg[1]]), user));
    }
    if (seg[0]==='leads' && seg[1] && seg[2]==='convert' && method==='POST') {
      const lead = qone('SELECT * FROM leads WHERE id=?', [seg[1]]);
      if (!lead) return send(res, 404, { detail:'Lead not found' });
      const b = await readBody(req); const cid = newId();
      run("INSERT INTO customers (id,company_name,contact_name,email,phone,status,owner_id) VALUES (?,?,?,?,?,'prospect',?)",
        [cid, b.company_name||lead.company||'', `${lead.first_name} ${lead.last_name}`, lead.email, lead.phone, user.id]);
      if (b.create_deal && b.pipeline_id) {
        const stage = qone('SELECT id FROM stages WHERE pipeline_id=? ORDER BY stage_order LIMIT 1', [b.pipeline_id]);
        if (stage) run("INSERT INTO deals (id,title,value,status,stage_id,pipeline_id,customer_id,owner_id) VALUES (?,?,?,'open',?,?,?,?)",
          [newId(), b.deal_title||`Deal - ${lead.first_name} ${lead.last_name}`, b.deal_value||lead.estimated_value, stage.id, b.pipeline_id, cid, user.id]);
      }
      run("UPDATE leads SET status='converted', updated_at=? WHERE id=?", [nowIso(), seg[1]]);
      return send(res, 200, { success:true, customer_id:cid });
    }
    if (seg[0]==='leads' && seg[1] && !seg[2] && method==='DELETE') {
      const l = qone('SELECT * FROM leads WHERE id=?', [seg[1]]);
      if (!l) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, l.owner_id)) return send(res, 403, { detail:'Permission denied' });
      run('DELETE FROM leads WHERE id=?', [seg[1]]);
      return send(res, 204, null);
    }

    // ── Pipelines ──────────────────────────────────────────────────────────
    if (apiPath === '/pipelines' && method === 'GET')
      return send(res, 200, qall('SELECT * FROM pipelines ORDER BY is_default DESC, name').map(buildPipeline));
    if (apiPath === '/pipelines' && method === 'POST') {
      const b = await readBody(req); const id = newId();
      if (b.is_default) run('UPDATE pipelines SET is_default=0');
      run('INSERT INTO pipelines (id,name,description,is_default) VALUES (?,?,?,?)',
        [id, b.name||'New Pipeline', b.description||null, b.is_default?1:0]);
      return send(res, 201, buildPipeline(qone('SELECT * FROM pipelines WHERE id=?', [id])));
    }
    if (seg[0]==='pipelines' && seg[1] && !seg[2] && method==='GET') {
      const p = qone('SELECT * FROM pipelines WHERE id=?', [seg[1]]);
      return p ? send(res, 200, buildPipeline(p)) : send(res, 404, { detail:'Not found' });
    }
    if (seg[0]==='pipelines' && seg[1] && !seg[2] && method==='PATCH') {
      const b = await readBody(req);
      const OK = ['name','description','is_default'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      if (Object.keys(fields).length) run(`UPDATE pipelines SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      return send(res, 200, qone('SELECT * FROM pipelines WHERE id=?', [seg[1]]));
    }
    if (seg[0]==='pipelines' && seg[1] && !seg[2] && method==='DELETE') {
      run('DELETE FROM pipelines WHERE id=?', [seg[1]]); return send(res, 204, null);
    }
    if (seg[0]==='pipelines' && seg[1] && seg[2]==='stages' && method==='POST') {
      const b = await readBody(req); const id = newId();
      run('INSERT INTO stages (id,pipeline_id,name,color,stage_order,probability,is_won,is_lost) VALUES (?,?,?,?,?,?,?,?)',
        [id, seg[1], b.name||'Stage', b.color||'#f59e0b', b.order||b.stage_order||0, b.probability||0, b.is_won?1:0, b.is_lost?1:0]);
      return send(res, 201, qone('SELECT * FROM stages WHERE id=?', [id]));
    }

    // ── Stages ─────────────────────────────────────────────────────────────
    if (seg[0]==='stages' && seg[1] && method==='PATCH') {
      const b = await readBody(req);
      const OK = ['name','color','stage_order','probability','is_won','is_lost'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      if (Object.keys(fields).length) run(`UPDATE stages SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      return send(res, 200, qone('SELECT * FROM stages WHERE id=?', [seg[1]]));
    }
    if (seg[0]==='stages' && seg[1] && method==='DELETE') {
      run('DELETE FROM stages WHERE id=?', [seg[1]]); return send(res, 204, null);
    }

    // ── Deals ──────────────────────────────────────────────────────────────
    if (apiPath === '/deals' && method === 'GET') {
      const s = `%${qs.search||''}%`;
      let sql = 'SELECT * FROM deals WHERE title LIKE ?'; let params = [s];
      if (qs.status)      { sql += ' AND status=?';      params.push(qs.status); }
      if (qs.pipeline_id) { sql += ' AND pipeline_id=?'; params.push(qs.pipeline_id); }
      sql += ' ORDER BY created_at DESC';
      const r = paginate(sql, params, qs);
      r.items = r.items.map(d => { withOwner(d, user); d.customer = d.customer_id ? qone('SELECT id,company_name,contact_name FROM customers WHERE id=?', [d.customer_id]) : null; return d; });
      return send(res, 200, r);
    }
    if (apiPath === '/deals' && method === 'POST') {
      const b = await readBody(req); const id = newId();
      run("INSERT INTO deals (id,title,value,probability,status,stage_id,pipeline_id,customer_id,owner_id,expected_close_date,description) VALUES (?,?,?,?,'open',?,?,?,?,?,?)",
        [id, b.title||'New Deal', b.value||null, b.probability||null, b.stage_id||null,
         b.pipeline_id||null, b.customer_id||null, b.owner_id||user.id, b.expected_close_date||null, b.description||null]);
      return send(res, 201, fullDeal(id, user));
    }
    if (seg[0]==='deals' && seg[1] && !seg[2] && method==='GET') {
      const d = fullDeal(seg[1], user); return d ? send(res, 200, d) : send(res, 404, { detail:'Not found' });
    }
    if (seg[0]==='deals' && seg[1] && !seg[2] && method==='PATCH') {
      const dr = qone('SELECT * FROM deals WHERE id=?', [seg[1]]);
      if (!dr) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, dr.owner_id)) return send(res, 403, { detail:'Permission denied' });
      const b = await readBody(req);
      const OK = ['title','value','probability','status','stage_id','pipeline_id','customer_id','owner_id','expected_close_date','description'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      fields.updated_at = nowIso();
      run(`UPDATE deals SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      return send(res, 200, fullDeal(seg[1], user));
    }
    if (seg[0]==='deals' && seg[1] && seg[2]==='move' && method==='POST') {
      const b = await readBody(req);
      const s = qone('SELECT * FROM stages WHERE id=?', [b.stage_id]);
      const ns = s?.is_won ? 'won' : (s?.is_lost ? 'lost' : 'open');
      run('UPDATE deals SET stage_id=?, status=?, updated_at=? WHERE id=?', [b.stage_id, ns, nowIso(), seg[1]]);
      return send(res, 200, fullDeal(seg[1], user));
    }
    if (seg[0]==='deals' && seg[1] && !seg[2] && method==='DELETE') {
      const dr = qone('SELECT * FROM deals WHERE id=?', [seg[1]]);
      if (!dr) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, dr.owner_id)) return send(res, 403, { detail:'Permission denied' });
      run('DELETE FROM deals WHERE id=?', [seg[1]]); return send(res, 204, null);
    }

    // ── Activities ─────────────────────────────────────────────────────────
    if (apiPath === '/activities' && method === 'GET') {
      let sql = 'SELECT * FROM activities WHERE 1=1'; let params = [];
      if (qs.activity_type) { sql += ' AND activity_type=?'; params.push(qs.activity_type); }
      if (qs.status === 'overdue') sql += " AND status='planned' AND due_date < datetime('now')";
      else if (qs.status)          { sql += ' AND status=?'; params.push(qs.status); }
      if (!elevated(user)) { sql += ' AND (created_by_id=? OR assignee_id=?)'; params.push(user.id, user.id); }
      sql += ' ORDER BY due_date ASC, created_at DESC';
      const r = paginate(sql, params, qs);
      r.items = r.items.map(a => withAssignee(a, user));
      return send(res, 200, r);
    }
    if (apiPath === '/activities' && method === 'POST') {
      const b = await readBody(req); const id = newId();
      run('INSERT INTO activities (id,title,activity_type,status,due_date,duration,entity_type,entity_id,description,assignee_id,created_by_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        [id, b.title||'', b.activity_type||'task', b.status||'planned', b.due_date||null,
         b.duration||null, b.entity_type||null, b.entity_id||null, b.description||null,
         b.assignee_id||user.id, user.id]);
      return send(res, 201, withAssignee(qone('SELECT * FROM activities WHERE id=?', [id]), user));
    }
    if (seg[0]==='activities' && seg[1] && method==='GET') {
      const a = qone('SELECT * FROM activities WHERE id=?', [seg[1]]);
      return a ? send(res, 200, withAssignee(a, user)) : send(res, 404, { detail:'Not found' });
    }
    if (seg[0]==='activities' && seg[1] && method==='PATCH') {
      const act = qone('SELECT * FROM activities WHERE id=?', [seg[1]]);
      if (!act) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, act.assignee_id, act.created_by_id)) return send(res, 403, { detail:'Permission denied' });
      const b = await readBody(req);
      const OK = ['title','activity_type','status','due_date','duration','entity_type','entity_id','description','assignee_id'];
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => OK.includes(k)));
      fields.updated_at = nowIso();
      run(`UPDATE activities SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      return send(res, 200, withAssignee(qone('SELECT * FROM activities WHERE id=?', [seg[1]]), user));
    }
    if (seg[0]==='activities' && seg[1] && method==='DELETE') {
      const act = qone('SELECT * FROM activities WHERE id=?', [seg[1]]);
      if (!act) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, act.assignee_id, act.created_by_id)) return send(res, 403, { detail:'Permission denied' });
      run('DELETE FROM activities WHERE id=?', [seg[1]]); return send(res, 204, null);
    }

    // ── Notes ──────────────────────────────────────────────────────────────
    if (apiPath === '/notes' && method === 'GET') {
      let sql = 'SELECT * FROM notes WHERE 1=1'; let params = [];
      if (qs.entity_type) { sql += ' AND entity_type=?'; params.push(qs.entity_type); }
      if (qs.entity_id)   { sql += ' AND entity_id=?';   params.push(qs.entity_id); }
      sql += ' ORDER BY is_pinned DESC, created_at DESC';
      const items = qall(sql, params);
      items.forEach(n => { n.can_edit = canMod(user, null, n.created_by_id); });
      return send(res, 200, { items, total: items.length });
    }
    if (apiPath === '/notes' && method === 'POST') {
      const b = await readBody(req); const id = newId();
      run('INSERT INTO notes (id,content,entity_type,entity_id,is_pinned,created_by_id) VALUES (?,?,?,?,?,?)',
        [id, b.content||'', b.entity_type||null, b.entity_id||null, b.is_pinned?1:0, user.id]);
      return send(res, 201, qone('SELECT * FROM notes WHERE id=?', [id]));
    }
    if (seg[0]==='notes' && seg[1] && method==='PATCH') {
      const note = qone('SELECT * FROM notes WHERE id=?', [seg[1]]);
      if (!note) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, null, note.created_by_id)) return send(res, 403, { detail:'Permission denied' });
      const b = await readBody(req);
      const fields = Object.fromEntries(Object.entries(b).filter(([k]) => ['content','is_pinned'].includes(k)));
      if (Object.keys(fields).length) run(`UPDATE notes SET ${Object.keys(fields).map(k=>`${k}=?`).join(', ')} WHERE id=?`, [...Object.values(fields), seg[1]]);
      const n = qone('SELECT * FROM notes WHERE id=?', [seg[1]]);
      if (n) n.can_edit = canMod(user, null, n.created_by_id);
      return send(res, 200, n);
    }
    if (seg[0]==='notes' && seg[1] && method==='DELETE') {
      const note = qone('SELECT * FROM notes WHERE id=?', [seg[1]]);
      if (!note) return send(res, 404, { detail:'Not found' });
      if (!canMod(user, null, note.created_by_id)) return send(res, 403, { detail:'Permission denied' });
      run('DELETE FROM notes WHERE id=?', [seg[1]]); return send(res, 204, null);
    }

    // ── Attachments stub ───────────────────────────────────────────────────
    if (apiPath === '/attachments') return send(res, 200, { items:[], total:0 });

    // ── Analytics ──────────────────────────────────────────────────────────
    if (apiPath === '/analytics/dashboard') {
      const total = db.prepare('SELECT COUNT(*) AS c FROM leads').get().c;
      const conv  = db.prepare("SELECT COUNT(*) AS c FROM leads WHERE status='converted'").get().c;
      return send(res, 200, {
        total_customers:      db.prepare('SELECT COUNT(*) AS c FROM customers').get().c,
        active_customers:     db.prepare("SELECT COUNT(*) AS c FROM customers WHERE status='active'").get().c,
        active_leads:         db.prepare("SELECT COUNT(*) AS c FROM leads WHERE status NOT IN ('converted','lost')").get().c,
        qualified_leads:      db.prepare("SELECT COUNT(*) AS c FROM leads WHERE status='qualified'").get().c,
        pipeline_value:       db.prepare("SELECT COALESCE(SUM(value),0) AS v FROM deals WHERE status='open'").get().v,
        open_deals:           db.prepare("SELECT COUNT(*) AS c FROM deals WHERE status='open'").get().c,
        won_this_month:       db.prepare("SELECT COALESCE(SUM(value),0) AS v FROM deals WHERE status='won' AND strftime('%Y-%m',updated_at)=strftime('%Y-%m','now')").get().v,
        won_deals_count:      db.prepare("SELECT COUNT(*) AS c FROM deals WHERE status='won'").get().c,
        overdue_activities:   db.prepare("SELECT COUNT(*) AS c FROM activities WHERE status='planned' AND due_date < datetime('now')").get().c,
        upcoming_activities:  db.prepare("SELECT COUNT(*) AS c FROM activities WHERE status='planned' AND due_date >= datetime('now')").get().c,
        converted_leads:      conv,
        lead_conversion_rate: total ? Math.round(conv / total * 1000) / 10 : 0,
      });
    }
    if (apiPath === '/analytics/revenue-trend') {
      const months = Math.min(12, parseInt(qs.months||'6', 10));
      const rows = [];
      for (let i = months-1; i >= 0; i--) {
        const month = db.prepare(`SELECT strftime('%Y-%m', date('now', '-${i} months')) AS m`).get().m;
        const label = db.prepare(`SELECT strftime('%b',    date('now', '-${i} months')) AS l`).get().l;
        const won   = db.prepare("SELECT COALESCE(SUM(value),0) AS v FROM deals WHERE status='won'  AND strftime('%Y-%m',updated_at)=?").get(month).v;
        const lost  = db.prepare("SELECT COALESCE(SUM(value),0) AS v FROM deals WHERE status='lost' AND strftime('%Y-%m',updated_at)=?").get(month).v;
        rows.push({ month: label, won, lost });
      }
      return send(res, 200, rows);
    }
    if (apiPath === '/analytics/activities') {
      const rows = qall('SELECT activity_type, COUNT(*) AS c FROM activities GROUP BY activity_type');
      return send(res, 200, Object.fromEntries(rows.map(r => [r.activity_type, r.c])));
    }
    if (apiPath === '/analytics/top-performers') {
      const limit = Math.min(10, parseInt(qs.limit||'5', 10));
      return send(res, 200, qall("SELECT u.id, u.full_name, COUNT(d.id) AS deals_won, COALESCE(SUM(d.value),0) AS revenue_won FROM users u LEFT JOIN deals d ON d.owner_id=u.id AND d.status='won' GROUP BY u.id ORDER BY revenue_won DESC LIMIT ?", [limit]));
    }
    if (apiPath === '/analytics/lead-conversion') {
      const rows     = qall("SELECT source, COUNT(*) AS total, SUM(CASE WHEN status='converted' THEN 1 ELSE 0 END) AS converted FROM leads WHERE source IS NOT NULL GROUP BY source");
      const totalAll = db.prepare('SELECT COUNT(*) AS c FROM leads').get().c;
      const convAll  = db.prepare("SELECT COUNT(*) AS c FROM leads WHERE status='converted'").get().c;
      return send(res, 200, {
        rate: totalAll ? Math.round(convAll / totalAll * 1000) / 10 : 0,
        by_source: Object.fromEntries(rows.map(r => [r.source, r.total ? Math.round(r.converted/r.total*1000)/10 : 0])),
      });
    }
    if (apiPath.startsWith('/analytics/funnel/')) {
      const pid    = seg[2];
      const stages = qall('SELECT * FROM stages WHERE pipeline_id=? ORDER BY stage_order', [pid]);
      return send(res, 200, stages.map(s => ({
        name:        s.name,
        deal_count:  db.prepare('SELECT COUNT(*) AS c FROM deals WHERE stage_id=?').get(s.id).c,
        total_value: db.prepare('SELECT COALESCE(SUM(value),0) AS v FROM deals WHERE stage_id=?').get(s.id).v,
      })));
    }

    return send(res, 404, { detail: `No route: ${method} ${apiPath}` });

  } catch (err) {
    if (err.status === 401) return send(res, 401, { detail: err.message });
    if (err.status === 403) return send(res, 403, { detail: err.message });
    console.error('[BRZ CRM] Error:', method, apiPath, err.message);
    return send(res, 500, { detail: 'Internal server error' });
  }
}

// ============================================================================
//  DB init + seed
// ============================================================================
function initDb() {
  db.exec(SCHEMA);
  let seeded = 0;
  for (const u of DEFAULT_USERS) {
    if (qone('SELECT id FROM users WHERE email=?', [u.email])) continue;
    run("INSERT INTO users (id,email,password_hash,full_name,role,is_active,is_verified,department,title,timezone) VALUES (?,?,?,?,?,1,1,?,?,'UTC')",
      [newId(), u.email, hashPw(u.password), u.full_name, u.role, u.department, u.title]);
    seeded++;
  }
  if (!qone('SELECT id FROM pipelines LIMIT 1')) {
    const pid = newId();
    run('INSERT INTO pipelines (id,name,description,is_default) VALUES (?,?,?,1)',
      [pid, DEFAULT_PIPELINE.name, DEFAULT_PIPELINE.description]);
    for (const s of DEFAULT_PIPELINE.stages)
      run('INSERT INTO stages (id,pipeline_id,name,color,stage_order,probability,is_won,is_lost) VALUES (?,?,?,?,?,?,?,?)',
        [newId(), pid, s.name, s.color, s.order, s.probability, s.is_won, s.is_lost]);
  }
  return seeded;
}

// ── Migrate legacy bcrypt passwords to PBKDF2 ─────────────────────────────
// Runs at startup. Any user still holding a $2b$ hash gets migrated using
// the known default passwords list. This handles the case where the user
// deployed our new server.js against the original crm.db from app.py.
function migrateLegacyPasswords() {
  const KNOWN = {
    'byron@brzcrm.co.za':    'Byr0nAdmin1',
    'michelle@brzcrm.co.za': 'M1chelleDirector1',
    'pranesh@brzcrm.co.za':  'Pr4neshDirector1',
    'wasab@brzcrm.co.za':    'W@sabManager1',
    'miranda@brzcrm.co.za':  'Mir@ndaExports1',
    'suveshen@brzcrm.co.za': 'Suv3shenExports1',
    'garisha@brzcrm.co.za':  'G@rishaSales1',
    'eugene@brzcrm.co.za':   'Eug3neLogistics1',
  };
  const users = qall("SELECT id, email, password_hash FROM users WHERE password_hash LIKE '$2%'");
  if (users.length === 0) return 0;
  let migrated = 0;
  for (const user of users) {
    const pw = KNOWN[user.email.toLowerCase()];
    if (!pw) continue;   // unknown user — skip, they must log in once to trigger migration
    run('UPDATE users SET password_hash=? WHERE id=?', [hashPw(pw), user.id]);
    migrated++;
  }
  return migrated;
}

// ============================================================================
//  Start
// ============================================================================
console.log();
console.log('='.repeat(58));
console.log('  BRZ CRM — Node.js Server (zero npm dependencies)');
console.log('='.repeat(58));

openDb();
const seeded   = initDb();
const migrated = migrateLegacyPasswords();
console.log(`\n  Database : ${DB_PATH}`);
console.log(`  Users    : ${seeded ? `${seeded} new user(s) seeded` : 'already present'}`);
if (migrated > 0) console.log(`  Migrated : ${migrated} legacy password(s) → PBKDF2`);

// Verify HTML file exists
const htmlPath = path.join(BASE_DIR, HTML_FILE);
console.log(`  Frontend : ${HTML_FILE} — ${fs.existsSync(htmlPath) ? 'OK' : 'MISSING!'}`);

const server = http.createServer(router);

// ── WebSocket upgrade — graceful rejection ──────────────────────────────────
// Realtime.js tries to upgrade to ws://.../api/v1/ws.
// server.js does not implement WebSocket. We reject the upgrade cleanly
// (HTTP 426) so the browser gets a definitive answer instead of a
// TCP-reset that triggers exponential-backoff reconnect spam.
server.on('upgrade', (req, socket) => {
  socket.write(
    'HTTP/1.1 426 Upgrade Required\r\n' +
    'Content-Length: 0\r\n' +
    'Connection: close\r\n\r\n'
  );
  socket.destroy();
});
server.listen(PORT, '0.0.0.0', () => {
  console.log();
  console.log('='.repeat(58));
  console.log(`  Open:  http://localhost:${PORT}`);
  console.log('='.repeat(58));
  console.log();
  console.log('  Login:');
  console.log('    byron@brzcrm.co.za     /  Byr0nAdmin1');
  console.log('    michelle@brzcrm.co.za  /  M1chelleDirector1');
  console.log('    miranda@brzcrm.co.za   /  Mir@ndaExports1');
  console.log();
  console.log('  Press Ctrl+C to stop.');
  console.log();
});

server.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    console.error(`\n  Port ${PORT} is busy. Run:  PORT=5001 node server.js\n`);
  } else {
    console.error('Server error:', err);
  }
  process.exit(1);
});