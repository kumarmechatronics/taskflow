"""
TaskFlow JSON Server  —  v2 (concurrent-safe)
==============================================
Run:   python server.py
Port:  3000  (all network interfaces — accessible from every PC on LAN)
Data:  data/db.json  (auto-created on first run)
Backups: backups/  (hourly auto-backups — latest.json always up-to-date)

Concurrency model
-----------------
Every write goes through LOCK (threading.Lock).
POST /sync uses per-record timestamp merge for tasks and escalations —
two users pushing different tasks in the same sub-assembly NEVER lose
each other's work.  The rule is simple: whichever copy of a given task
has the newer _localUpdated timestamp wins.
GET /delta?since=<ISO> returns only tasks updated after that timestamp
so clients can do cheap incremental polls without transferring the whole
payload.
POST /activity appends a server-side audit trail entry.
"""

import json, os, glob, time, threading, uuid, re, warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    try:
        import cgi
        _HAS_CGI = True
    except ImportError:
        _HAS_CGI = False
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(BASE_DIR, 'data', 'db.json')
BACKUP_DIR  = os.path.join(BASE_DIR, 'backups')
LATEST_PATH = os.path.join(BACKUP_DIR, 'latest.json')
ACTIVITY_PATH = os.path.join(BASE_DIR, 'data', 'activity.log')
FILES_DIR   = os.path.join(BASE_DIR, 'data', 'files')   # drawing file storage

# File size limits (bytes)
LIMIT_PDF  = 25  * 1024 * 1024   #  25 MB
LIMIT_IGES = 50  * 1024 * 1024   #  50 MB
LIMIT_STEP = 500 * 1024 * 1024   # 500 MB
FILE_LIMITS = {'pdf': LIMIT_PDF, 'iges': LIMIT_IGES, 'step': LIMIT_STEP}

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(FILES_DIR, exist_ok=True)

LOCK = threading.Lock()
_last_backup_time = None
_next_backup_ts   = None

DEFAULT_USERS = [
    {"id":"u1", "first":"kumar",    "last":"","email":"kumar@taskflow.com",    "password":"kumar123",    "role":"admin", "color":"#4f8ef7", "designation":"Design Manager",   "department":"design_management"},
    {"id":"u2", "first":"praveen",  "last":"","email":"praveen@taskflow.com",  "password":"praveen123",  "role":"member","color":"#7c5ef7", "department":"design"},
    {"id":"u3", "first":"siddharth","last":"","email":"siddharth@taskflow.com","password":"siddharth123","role":"member","color":"#34d399", "department":"design"},
    {"id":"u4", "first":"vignesh",  "last":"","email":"vignesh@taskflow.com",  "password":"vignesh123",  "role":"member","color":"#fbbf24", "department":"design"},
    {"id":"u5", "first":"venkat",   "last":"","email":"venkat@taskflow.com",   "password":"venkat123",   "role":"member","color":"#f87171", "department":"design"},
    {"id":"u6", "first":"suresh",   "last":"","email":"suresh@taskflow.com",   "password":"suresh123",   "role":"member","color":"#a78bfa", "department":"design"},
    {"id":"u7", "first":"siva",     "last":"","email":"siva@taskflow.com",     "password":"siva123",     "role":"member","color":"#fb923c", "department":"design"},
    {"id":"u8", "first":"nirenjen", "last":"","email":"nirenjen@taskflow.com", "password":"nirenjen123", "role":"member","color":"#22d3ee", "department":"design"},
    {"id":"u9", "first":"justin",   "last":"","email":"justin@taskflow.com",   "password":"justin123",   "role":"member","color":"#f472b6", "department":"design"},
    {"id":"u10","first":"logesh",   "last":"","email":"logesh@taskflow.com",   "password":"logesh123",   "role":"member","color":"#86efac", "department":"design"},
    {"id":"u11","first":"hakkim",   "last":"","email":"hakkim@taskflow.com",   "password":"hakkim123",   "role":"admin", "color":"#06b6d4", "designation":"Project Manager",  "department":"project_management"},
    {"id":"u12","first":"bharatia", "last":"","email":"bharatia@taskflow.com", "password":"bharatia123", "role":"admin", "color":"#8b5cf6", "designation":"Project Manager",  "department":"project_management"},
    {"id":"u13","first":"sahidha",  "last":"","email":"sahidha@taskflow.com",  "password":"sahidha123",  "role":"admin", "color":"#ec4899", "designation":"Project Manager",  "department":"project_management"},
    {"id":"u14","first":"rohith",   "last":"","email":"rohith@taskflow.com",   "password":"rohith123",   "role":"admin", "color":"#f59e0b", "designation":"General Manager",  "department":"management"},
    {"id":"u15","first":"arjun",    "last":"","email":"arjun@taskflow.com",    "password":"arjun123",    "role":"admin", "color":"#10b981", "designation":"President",        "department":"management"},
]

DEFAULT_DB = {
    # Empty by default — client pushes seed data on first connect.
    # Never hardcode users here; it causes the server to overwrite
    # client localStorage with stale seed data on every fresh start.
    "users": [], "projects": [], "tasks": [], "issues": [],
    "chat_messages": [], "notifications": [],
    "timesheets": {}, "work_logs": [],
    "archived_members": [],
    "project_assemblies": {},
    "sub_assemblies": {},
    "escalations": [],
    # HR / Leave / Payroll
    "leave_policies":    [],
    "leave_requests":    [],
    "holiday_calendar":  [],
    "salary_structures": [],
    "payslips":          [],
    # Procurement and ERP
    "purchase_orders":   [],
    "suppliers":         [],
    "customers":         [],
    "quotations":        [],
    "inventory":         [],
    # Installation
    "installation_logs": [],
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def now_iso():
    # Always UTC with Z suffix — keeps timestamps comparable with JS toISOString()
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.') + \
           f'{datetime.now(timezone.utc).microsecond:06d}Z'

def load_db():
    if not os.path.exists(DB_PATH):
        if os.path.exists(LATEST_PATH):
            try:
                with open(LATEST_PATH, 'r', encoding='utf-8') as f:
                    db = json.load(f)
                for k in ('backed_up_at', 'label', 'version'):
                    db.pop(k, None)
                for k, v in DEFAULT_DB.items():
                    if k not in db:
                        db[k] = v
                save_db(db)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Restored db.json from latest backup")
                return db
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  Could not restore from backup: {e}")
        save_db(DEFAULT_DB)
        return dict(DEFAULT_DB)
    with open(DB_PATH, 'r', encoding='utf-8') as f:
        db = json.load(f)
    for k, v in DEFAULT_DB.items():
        if k not in db:
            db[k] = v
    return db

def save_db(db):
    with open(DB_PATH, 'w', encoding='utf-8') as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

# ─── Per-record merge helpers ─────────────────────────────────────────────────

def parse_ts(ts_str):
    """
    Parse an ISO timestamp string to epoch seconds for reliable comparison.
    Handles both UTC (Z-suffix, from JS toISOString) and naive local-time
    strings (legacy Python datetime.now().isoformat() without timezone).
    Returns 0 for missing/invalid values.
    """
    if not ts_str:
        return 0
    try:
        s = str(ts_str).strip()
        # Normalise: replace trailing Z with +00:00 for fromisoformat
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        # Remove microseconds beyond 6 digits if present
        # Python's fromisoformat handles up to 6 decimal digits
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            # Naive string — assume local server time, attach local tz
            dt = dt.astimezone(timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0

def merge_tasks(server_tasks, incoming_tasks, now_ts, wiped_at=None):
    """
    Merge incoming tasks into server tasks using timestamp-based conflict resolution.
    Winner per task = whichever has the newer _localUpdated timestamp.
    New tasks (not on server yet) are always added.
    Server-only tasks (not in incoming) are preserved as-is.
    """
    wipe_ts = parse_ts(wiped_at) if wiped_at else None
    server_map = {t['id']: t for t in server_tasks}
    for inc in incoming_tasks:
        tid = inc.get('id')
        if not tid:
            continue
        # Drop tasks that predate the last wipe (stale client cache)
        if wipe_ts:
            inc_ts = parse_ts(inc.get('_localUpdated') or inc.get('_updated') or '')
            if inc_ts and inc_ts <= wipe_ts:
                continue
        srv = server_map.get(tid)
        if srv is None:
            # Brand-new task — add it
            inc['_updated'] = now_ts
            server_map[tid] = inc
        else:
            inc_ts = parse_ts(inc.get('_localUpdated') or inc.get('_updated'))
            srv_ts = parse_ts(srv.get('_localUpdated') or srv.get('_updated'))
            if inc_ts >= srv_ts:
                # Incoming is same age or newer — incoming wins
                inc['_updated'] = now_ts
                server_map[tid] = inc
            # else: server is newer — keep server version, discard incoming
    return list(server_map.values())

def merge_escalations(server_escs, incoming_escs):
    """
    Escalations are append-only + status-updatable.
    Incoming always wins (escalations only grow or get closed, never conflict).
    """
    server_map = {e['id']: e for e in server_escs}
    for inc in incoming_escs:
        eid = inc.get('id')
        if eid:
            server_map[eid] = inc
    return list(server_map.values())

# ─── Activity log ─────────────────────────────────────────────────────────────

def get_user(db, uid):
    return next((u for u in db.get("users", []) if u.get("id") == uid), None)

def user_dept(db, uid):
    u = get_user(db, uid)
    return (u.get("department", "") if u else "").lower()

def is_hr(db, uid):
    return user_dept(db, uid) == "hr"

def is_gm(db, uid):
    u = get_user(db, uid)
    if not u: return False
    return (u.get("department", "") == "management" and
            "general manager" in u.get("designation", "").lower())

def is_dept_manager(db, uid):
    return user_dept(db, uid) in ("design_management", "project_management", "management")

def build_approval_chain(db, requestor_id):
    """Return flat list of approver user IDs in order.

    Chain rules:
      - HR dept / dept manager  →  [GM]
      - Regular employees        →  [HR Manager, GM]
      - GM / President           →  [President]  (or empty if no one above)
    """
    users = db.get('users', [])
    requestor = next((u for u in users if u.get('id') == requestor_id), {})
    dept = requestor.get('department', '')
    role = requestor.get('role', 'member')

    # Find key people
    gm = next((u for u in users
               if u.get('department') == 'management'
               and 'General Manager' in (u.get('designation') or '')), None)
    president = next((u for u in users
                      if u.get('department') == 'management'
                      and 'President' in (u.get('designation') or '')), None)
    hr_manager = next((u for u in users
                       if u.get('department') == 'hr'
                       and u.get('role') == 'admin'
                       and u.get('id') != requestor_id), None)

    gm_id        = gm['id']        if gm        else None
    president_id = president['id'] if president else None
    hr_id        = hr_manager['id'] if hr_manager else None

    # GM / President level — approve goes to president (or self-approve if no one above)
    if dept == 'management':
        return [president_id] if president_id and president_id != requestor_id else []

    # HR dept or any dept manager → straight to GM
    if dept == 'hr' or is_dept_manager(db, requestor_id):
        return [gm_id] if gm_id else []

    # Regular employees → HR Manager → GM
    chain = []
    if hr_id:
        chain.append(hr_id)
    if gm_id:
        chain.append(gm_id)
    return chain

def _do_jan1_reset(new_year):
    """Jan 1: calculate unused CL encashment; EL and ML expire."""
    prev_year = new_year - 1
    with LOCK:
        db = load_db()
        leave_requests = db.get("leave_requests", [])
        leave_policies = db.get("leave_policies", [])
        salary_structs = db.get("salary_structures", [])
        payslips       = db.get("payslips", [])
        for user in db.get("users", []):
            uid = user.get("id")
            if not uid: continue
            policy = next(
                (p for p in leave_policies
                 if p.get("userId") == uid and p.get("year") == prev_year), None)
            if not policy: continue
            used_cl = sum(
                r.get("days", 0) for r in leave_requests
                if r.get("userId") == uid and r.get("type") == "cl"
                and r.get("status") == "approved"
                and str(r.get("fromDate", "")).startswith(str(prev_year))
            )
            unused_cl  = max(0, policy.get("cl", 0) - used_cl)
            salary     = next(
                (s for s in salary_structs
                 if s.get("userId") == uid and s.get("active", True)), None)
            daily_rate = cl_enc = 0
            if salary and unused_cl > 0:
                daily_rate = round(salary.get("basic", 0) / 26, 2)
                cl_enc     = round(unused_cl * daily_rate, 2)
            jan_ps = next(
                (ps for ps in payslips
                 if ps.get("userId") == uid
                 and ps.get("month") == 1 and ps.get("year") == new_year), None)
            if jan_ps:
                jan_ps["clEncashment"]     = cl_enc
                jan_ps["clEncashmentDays"] = unused_cl
                jan_ps["clEncashmentRate"] = daily_rate
            elif cl_enc > 0:
                payslips.append({
                    "id": uuid.uuid4().hex[:12], "userId": uid,
                    "month": 1, "year": new_year, "status": "draft",
                    "clEncashment": cl_enc, "clEncashmentDays": unused_cl,
                    "clEncashmentRate": daily_rate,
                    "autoGenerated": True, "createdAt": now_iso(),
                })
        db["payslips"] = payslips
        save_db(db)
    append_activity({"action": "jan1_auto_reset", "year": new_year,
                     "desc": "Jan1: EL+ML expired, CL encashment calculated"})
    print("[{}] Jan1 reset done for {}".format(
        datetime.now().strftime("%H:%M:%S"), new_year))

def _jan1_scheduler_loop():
    """Background thread: fires once on Jan 1 at ~00:05 each year."""
    last_reset = [None]
    while True:
        time.sleep(60)
        n = datetime.now()
        if n.month == 1 and n.day == 1 and n.hour == 0 and n.minute >= 5:
            if last_reset[0] != n.year:
                last_reset[0] = n.year
                try:
                    _do_jan1_reset(n.year)
                except Exception as e:
                    print("[{}] Jan1 error: {}".format(
                        n.strftime("%H:%M:%S"), e))

def append_activity(entry):
    """Append one line of JSON to the activity log file (fire-and-forget)."""
    try:
        entry['_logged_at'] = now_iso()
        with open(ACTIVITY_PATH, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    except Exception:
        pass

# ─── Backup helpers ───────────────────────────────────────────────────────────

def _write_backup(db, label='auto'):
    global _last_backup_time
    now = datetime.now()
    ts  = now.strftime('%Y-%m-%d_%H-%M')
    fname = f'taskflow_backup_{ts}.json'
    path  = os.path.join(BACKUP_DIR, fname)
    payload = dict(db)
    payload['backed_up_at'] = now.isoformat()
    payload['label']  = label
    payload['version'] = 'v8'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    with open(LATEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    _last_backup_time = now.isoformat()
    files = sorted(glob.glob(os.path.join(BACKUP_DIR, 'taskflow_backup_*.json')))
    for old in files[:-48]:
        try: os.remove(old)
        except Exception: pass
    return fname

def save_backup(label='manual'):
    with LOCK:
        db = load_db()
        return _write_backup(db, label)

def update_latest(db):
    global _last_backup_time
    now = datetime.now()
    payload = dict(db)
    payload['backed_up_at'] = now.isoformat()
    payload['label']  = 'sync'
    payload['version'] = 'v8'
    try:
        with open(LATEST_PATH, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        _last_backup_time = now.isoformat()
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  latest.json write failed: {e}")

def list_backups():
    files = sorted(glob.glob(os.path.join(BACKUP_DIR, 'taskflow_backup_*.json')), reverse=True)
    result = []
    for fp in files[:20]:
        try:
            size  = os.path.getsize(fp)
            mtime = datetime.fromtimestamp(os.path.getmtime(fp)).isoformat()
            result.append({'filename': os.path.basename(fp), 'size': size, 'saved_at': mtime})
        except Exception:
            pass
    return result

# ─── Hourly backup thread ─────────────────────────────────────────────────────

def _hourly_backup_loop():
    global _next_backup_ts
    _next_backup_ts = time.time() + 3600
    while True:
        time.sleep(60)
        if time.time() >= _next_backup_ts:
            _next_backup_ts = time.time() + 3600
            try:
                fname = save_backup('hourly')
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Hourly backup → backups/{fname}")
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Hourly backup failed: {e}")

# ─── HTTP Handler ─────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")

    def send_json(self, data, status=200):
        # Mirror success→ok so frontend res.ok checks work alongside res.success
        if isinstance(data, dict) and data.get('success') and 'ok' not in data:
            data = dict(data, ok=True)
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(body)

    def send_err(self, msg, status=400):
        self.send_json({'error': msg}, status)

    def read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def get_qs(self):
        """Return query-string as dict of str→str (first value only)."""
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        return {k: v[0] for k, v in qs.items()}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    # ── GET ──────────────────────────────────────────────────────────────────

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = self.get_qs()

        with LOCK:
            db = load_db()

        # ── /ping ──────────────────────────────────────────────────────────
        if path == '/ping':
            secs_left = max(0, int((_next_backup_ts or time.time()+3600) - time.time()))
            return self.send_json({
                'status': 'ok',
                'time': now_iso(),
                'last_backup': _last_backup_time,
                'next_backup_secs': secs_left,
            })

        # ── /sync — full snapshot ──────────────────────────────────────────
        if path == '/sync':
            return self.send_json(db)

        # ── /delta?since=<ISO> — lightweight incremental pull ──────────────
        # Returns only tasks (and escalations) changed after the given timestamp.
        # The client merges these into its localStorage instead of re-pulling everything.
        if path == '/delta':
            since = qs.get('since', '')
            delta_tasks = [t for t in db.get('tasks', [])
                           if (t.get('_updated') or t.get('_localUpdated') or '') > since]
            delta_escs  = [e for e in db.get('escalations', [])
                           if (e.get('createdAt') or '') > since or
                              (e.get('closedAt') or '') > since]
            return self.send_json({
                'tasks':       delta_tasks,
                'escalations': delta_escs,
                'server_time': now_iso(),
            })

        # ── /activity — read audit log ──────────────────────────────────────
        if path == '/activity':
            limit = int(qs.get('limit', 200))
            entries = []
            if os.path.exists(ACTIVITY_PATH):
                try:
                    with open(ACTIVITY_PATH, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    for line in lines[-limit:]:
                        line = line.strip()
                        if line:
                            entries.append(json.loads(line))
                except Exception:
                    pass
            return self.send_json({'entries': entries})

        # ── backup endpoints ───────────────────────────────────────────────
        if path == '/backups':
            return self.send_json({'backups': list_backups(), 'last_backup': _last_backup_time})
        if path == '/backups/latest':
            if os.path.exists(LATEST_PATH):
                with open(LATEST_PATH, 'r', encoding='utf-8') as f:
                    return self.send_json(json.load(f))
            return self.send_err('No backup found yet', 404)
        if path.startswith('/backups/'):
            fname = os.path.basename(path[len('/backups/'):])
            fpath = os.path.join(BACKUP_DIR, fname)
            if os.path.exists(fpath) and fname.startswith('taskflow_backup_'):
                with open(fpath, 'r', encoding='utf-8') as f:
                    return self.send_json(json.load(f))
            return self.send_err('Backup not found', 404)

        # ── collection endpoints ───────────────────────────────────────────
        for col in ['users','projects','tasks','issues','chat_messages',
                    'notifications','work_logs','archived_members','escalations']:
            if path == f'/{col}':
                return self.send_json(db.get(col, []))
        if path == '/timesheets':
            return self.send_json(db.get('timesheets', {}))
        if path == '/project_assemblies':
            return self.send_json(db.get('project_assemblies', {}))
        if path == '/sub_assemblies':
            return self.send_json(db.get('sub_assemblies', {}))

        # ── /files/:name — serve uploaded drawing files ─────────────────────
        if path.startswith('/files/'):
            fname = os.path.basename(path[len('/files/'):])
            if not fname or '..' in fname:
                return self.send_err('Invalid filename', 400)
            fpath = os.path.join(FILES_DIR, fname)
            if not os.path.isfile(fpath):
                return self.send_err('File not found', 404)
            ext  = os.path.splitext(fname)[1].lower()
            mime = {
                '.pdf':  'application/pdf',
                '.iges': 'model/iges', '.igs': 'model/iges',
                '.dxf':  'image/vnd.dxf',
                '.step': 'model/step', '.stp': 'model/step',
            }.get(ext, 'application/octet-stream')
            fsize = os.path.getsize(fpath)
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', fsize)
            self.send_header('Content-Disposition', f'inline; filename="{fname}"')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=86400')
            self.end_headers()
            with open(fpath, 'rb') as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            return

        # ── static files (.html, .py not served, etc.) ───────────────────────
        # ERP / HR / Leave / Payroll collections
        for _col in ["leave_policies","leave_requests","holiday_calendar",
                     "salary_structures","payslips","purchase_orders",
                     "suppliers","customers","quotations","inventory",
                     "installation_logs",
                     "inquiries","kickoff_meetings","supplier_quotes",
                     "process_rates","time_logs"]:
            if path == "/" + _col:
                return self.send_json(db.get(_col, []))

        # ── /time_logs/export — filtered time log export ─────────────────────
        if path == '/time_logs/export':
            with LOCK:
                db2 = load_db()
            qs2   = self.get_qs()
            logs  = db2.get('time_logs', [])
            uid   = qs2.get('userId', '')
            since = qs2.get('since', '')
            until = qs2.get('until', '')
            page  = qs2.get('page', '')
            if uid:   logs = [l for l in logs if l.get('userId') == uid]
            if since: logs = [l for l in logs if l.get('ts', '') >= since]
            if until: logs = [l for l in logs if l.get('ts', '') <= until]
            if page:  logs = [l for l in logs if l.get('page', '') == page]
            return self.send_json({'logs': logs, 'count': len(logs)})

        # ── /bom_items_list — lightweight list for supplier quote dropdown ─────
        if path == '/kickoff_meetings/get_zip':
            import base64 as _b64
            kid      = qs.get('kickoffId', [''])[0] if isinstance(qs.get('kickoffId'), list) else qs.get('kickoffId', '')
            srv_file = qs.get('serverFile', [''])[0] if isinstance(qs.get('serverFile'), list) else qs.get('serverFile', '')
            if not srv_file:
                with LOCK:
                    db2 = load_db()
                km = next((x for x in db2.get('kickoff_meetings', []) if x.get('id') == kid), None)
                if km:
                    srv_file = (km.get('projectZip') or {}).get('serverFile', '')
            if not srv_file:
                self.send_response(404); self.end_headers()
                return
            file_path = os.path.join(BASE_DIR, 'data', 'files', 'kickoff', srv_file)
            if not os.path.exists(file_path):
                self.send_response(404); self.end_headers()
                return
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Disposition', f'attachment; filename="{srv_file}"')
            self.send_header('Access-Control-Allow-Origin', '*')
            with open(file_path, 'rb') as _f:
                data = _f.read()
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        if path == '/bom_items_list':
            with LOCK:
                db2 = load_db()
            items = db2.get('bom_items', [])
            lite  = [{'id': x.get('id'), 'partNo': x.get('partNo',''),
                      'description': x.get('description',''),
                      'projectId': x.get('projectId','')} for x in items]
            return self.send_json({'items': lite})

        static_path = os.path.join(BASE_DIR, path.lstrip('/'))
        if os.path.isfile(static_path):
            ext  = os.path.splitext(static_path)[1].lower()
            mime = {
                '.html': 'text/html; charset=utf-8',
                '.js':   'application/javascript',
                '.css':  'text/css',
                '.json': 'application/json',
                '.png':  'image/png',
                '.jpg':  'image/jpeg',
                '.ico':  'image/x-icon',
                '.pdf':  'application/pdf',
                '.iges': 'model/iges',
                '.igs':  'model/iges',
                '.dxf':  'image/vnd.dxf',
                '.step': 'model/step',
                '.stp':  'model/step',
            }.get(ext, 'application/octet-stream')
            with open(static_path, 'rb') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', len(content))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(content)
            return

        self.send_err('Not found', 404)

    # ── POST ─────────────────────────────────────────────────────────────────

    def do_POST(self):
        global _next_backup_ts
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')

        # ── /upload — multipart file upload (must come BEFORE read_body) ──
        if path == '/upload':
            return self._handle_upload()

        body   = self.read_body()

        # ── /backup — manual snapshot ──────────────────────────────────────
        if path == '/backup':
            try:
                fname = save_backup('manual')
                _next_backup_ts = time.time() + 3600
                return self.send_json({'success': True, 'filename': fname, 'saved_at': _last_backup_time})
            except Exception as e:
                return self.send_err(str(e), 500)

        # ── /activity — append audit entry ────────────────────────────────
        if path == '/activity':
            append_activity(body)
            return self.send_json({'success': True})

        with LOCK:
            db = load_db()

            # ── /login ────────────────────────────────────────────────────
            if path == '/login':
                u = next((u for u in db.get('users', [])
                          if u['email'] == body.get('email')
                          and u['password'] == body.get('password')), None)
                if u:
                    append_activity({'action': 'login', 'userId': u['id'], 'userName': u.get('first','')})
                return self.send_json(
                    {'success': True, 'user': u} if u
                    else {'success': False, 'error': 'Invalid credentials'},
                    200 if u else 401
                )

            # ── /sync — smart merge push ───────────────────────────────────
            #
            # CONCURRENCY RULE:
            #   tasks      → per-record merge (newer _localUpdated wins)
            #   escalations→ per-record merge (incoming always added/updated)
            #   everything else → full replace (safe — only one admin writes)
            #
            if path == '/sync':
                ts = now_iso()

                # Tasks: per-record, timestamp-based winner
                if 'tasks' in body:
                    db['tasks'] = merge_tasks(db.get('tasks', []), body['tasks'], ts, wiped_at=db.get('wiped_at'))

                # Escalations: append-merge (never lose an escalation)
                if 'escalations' in body:
                    db['escalations'] = merge_escalations(
                        db.get('escalations', []), body['escalations']
                    )

                # Project assemblies: last writer wins (admin-only, rare)
                if 'project_assemblies' in body:
                    db['project_assemblies'] = body['project_assemblies']
                if 'sub_assemblies' in body:
                    db['sub_assemblies'] = body['sub_assemblies']

                # Projects: ID-based merge — empty client array NEVER wipes server data
                if 'projects' in body:
                    incoming = body.get('projects') or []
                    if incoming:
                        server_map = {p['id']: p for p in db.get('projects', []) if 'id' in p}
                        for p in incoming:
                            if p.get('id'):
                                server_map[p['id']] = p  # client edit wins for existing projects
                        db['projects'] = list(server_map.values())
                    # if incoming is empty, keep server projects unchanged

                # Users: ID-based merge — empty client array NEVER wipes server users
                if 'users' in body:
                    incoming = body.get('users') or []
                    if incoming:
                        server_map = {u['id']: u for u in db.get('users', []) if 'id' in u}
                        for u in incoming:
                            if u.get('id'):
                                server_map[u['id']] = u
                        db['users'] = list(server_map.values())
                    # if incoming is empty, keep server users unchanged

                # Simple collections — full replace (non-critical data)
                for key in ['issues', 'chat_messages',
                            'notifications', 'timesheets', 'work_logs', 'archived_members',
                            # CFT / BOM entities
                            'bom_items', 'indents', 'issue_slips', 'change_notes',
                            'audit_logs', 'revision_history', 'material_receipts',
                            'drawing_files', 'archived_drawing_files',
                            'oem_items', 'fastener_items', 'revision_requests',
                            'materials_list',
                            'leave_policies', 'leave_requests', 'holiday_calendar',
                            'salary_structures', 'payslips', 'purchase_orders',
                            'suppliers', 'customers', 'quotations', 'inventory',
                            'installation_logs',
                            'inquiries', 'kickoff_meetings', 'supplier_quotes',
                            'process_rates', 'time_logs']:
                    if key in body:
                        db[key] = body[key]

                save_db(db)
                update_latest(db)

                # Return merged tasks + escalations so the client can update
                # its localStorage to reflect what the server actually stored
                # (important when the server kept a newer version of a task)
                return self.send_json({
                    'success': True,
                    'tasks': db['tasks'],
                    'escalations': db.get('escalations', []),
                    'server_time': ts,
                })

            # ── generic collection append ──────────────────────────────────
            col_map = {'/tasks': 'tasks', '/issues': 'issues',
                       '/chat_messages': 'chat_messages', '/notifications': 'notifications',
                       '/work_logs': 'work_logs', '/projects': 'projects',
                       '/escalations': 'escalations'}
            if path in col_map:
                col = col_map[path]
                if 'id' not in body:
                    body['id'] = str(uuid.uuid4())[:8]
                body['_created'] = now_iso()
                body['_updated'] = body['_created']
                db[col].append(body)
                save_db(db)
                update_latest(db)
                return self.send_json(body, 201)

            if path == '/leave_policies/set':
                uid  = body.get('userId')
                year = body.get('year', datetime.now().year)
                db['leave_policies'] = [p for p in db.get('leave_policies', [])
                    if not (p.get('userId') == uid and p.get('year') == year)]
                pol = {'id': uuid.uuid4().hex[:12], 'userId': uid, 'year': year,
                       'el': body.get('el', 0), 'cl': body.get('cl', 0),
                       'ml': body.get('ml', 0),
                       'joiningDate': body.get('joiningDate', ''),
                       'setBy': body.get('setBy', ''), 'setAt': now_iso()}
                db['leave_policies'].append(pol)
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'policy': pol})

            if path == '/leave_requests/submit':
                uid   = body.get('userId', '')
                chain = build_approval_chain(db, uid)
                req   = {'id': uuid.uuid4().hex[:12],
                         'userId': uid,
                         'year':   body.get('year', str(datetime.now().year)),
                         'type':   body.get('type', 'el'),
                         'fromDate':     body.get('fromDate', ''),
                         'toDate':       body.get('toDate', ''),
                         'halfSession':  body.get('halfSession', ''),
                         'days':         body.get('days', 1),
                         'reason':       body.get('reason', ''),
                         'status':       'approved' if not chain else 'pending',
                         'approvalChain': chain,
                         'currentStage':  0,
                         'submittedAt':   body.get('submittedAt', now_iso()),
                         'createdAt':     now_iso()}
                db.setdefault('leave_requests', []).append(req)
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'request': req}, 201)

            if path == '/leave_requests/approve':
                rid        = body.get('requestId')
                action     = body.get('action', 'approved')   # 'approved' or 'rejected'
                approver   = body.get('approverId', '')
                note       = body.get('note', '')
                reqs       = db.get('leave_requests', [])
                req        = next((r for r in reqs if r.get('id') == rid), None)
                if not req:
                    return self.send_err('Leave request not found', 404)
                chain      = req.get('approvalChain', [])
                cur_stage  = req.get('currentStage', 0)
                # Verify it is this approver's turn
                if cur_stage >= len(chain) or chain[cur_stage] != approver:
                    return self.send_err('Not your turn to approve', 400)
                # Record decision
                if 'approvalLog' not in req:
                    req['approvalLog'] = []
                req['approvalLog'].append({
                    'stage': cur_stage, 'approverId': approver,
                    'action': action, 'note': note, 'at': now_iso()
                })
                if action == 'rejected':
                    req['status'] = 'rejected'
                else:
                    next_stage = cur_stage + 1
                    req['currentStage'] = next_stage
                    req['status'] = 'approved' if next_stage >= len(chain) else 'pending'
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'request': req})

            if path == '/holiday_calendar/save':
                hol = {'id': body.get('id') or uuid.uuid4().hex[:8],
                       'year': body.get('year', datetime.now().year),
                       'date': body.get('date', ''), 'name': body.get('name', ''),
                       'createdBy': body.get('createdBy', ''), 'createdAt': now_iso()}
                db['holiday_calendar'] = [h for h in db.get('holiday_calendar', [])
                                          if h.get('date') != hol['date']]
                db['holiday_calendar'].append(hol)
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'holiday': hol})

            if path == '/holiday_calendar/delete':
                hid = body.get('id')
                db['holiday_calendar'] = [h for h in db.get('holiday_calendar', [])
                                          if h.get('id') != hid]
                save_db(db); update_latest(db)
                return self.send_json({'success': True})

            if path == '/salary_structures/set':
                uid = body.get('userId')
                for s in db.get('salary_structures', []):
                    if s.get('userId') == uid and s.get('active', True):
                        s['active'] = False
                struct = {'id': uuid.uuid4().hex[:12], 'userId': uid,
                          'basic': body.get('basic', 0), 'hra': body.get('hra', 0),
                          'da': body.get('da', 0), 'special': body.get('special', 0),
                          'other': body.get('other', 0),
                          'allowances': body.get('allowances', []),
                          'deductions': body.get('deductions', []),
                          'effectiveFrom': body.get('effectiveFrom',
                              datetime.now().strftime('%Y-%m-%d')),
                          'active': True,
                          'setBy': body.get('setBy', ''), 'createdAt': now_iso()}
                db.setdefault('salary_structures', []).append(struct)
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'structure': struct})

            if path == '/payslips/generate':
                uid   = body.get('userId')
                month = body.get('month')
                year  = body.get('year')
                sal   = next((s for s in db.get('salary_structures', [])
                              if s.get('userId') == uid and s.get('active', True)), None)
                if not sal:
                    return self.send_err('No active salary structure for employee', 404)
                basic      = sal.get('basic', 0)
                hra        = sal.get('hra', 0)
                da         = sal.get('da', 0)
                special    = sal.get('special', 0)
                other      = sal.get('other', 0)
                allows     = sal.get('allowances', [])
                deducts    = sal.get('deductions', [])
                tot_allow  = da + special + other + sum(a.get('amount', 0) for a in allows)
                tot_deduct = sum(d.get('amount', 0) for d in deducts)
                gross      = basic + hra + tot_allow
                pf         = round(basic * 0.12, 2)
                esi        = round(gross * 0.0075, 2) if gross <= 21000 else 0
                tds        = body.get('tds', 0)
                cl_enc     = body.get('clEncashment', 0)
                lop_days   = body.get('lopDays', 0)
                lop_deduct = round((gross / 26) * lop_days, 2) if lop_days else 0
                total_ded  = round(pf + esi + tds + tot_deduct + lop_deduct, 2)
                net        = round(gross + cl_enc - total_ded, 2)
                draft      = next((ps for ps in db.get('payslips', [])
                                   if ps.get('userId') == uid
                                   and ps.get('month') == month
                                   and ps.get('year') == year), None)
                ps_data    = {'id': (draft['id'] if draft else uuid.uuid4().hex[:12]),
                              'userId': uid, 'month': month, 'year': year,
                              'basic': basic, 'hra': hra, 'da': da,
                              'special': special, 'other': other,
                              'allowances': allows, 'deductions': deducts,
                              'pf': pf, 'esi': esi, 'tds': tds,
                              'grossPay': gross, 'totalDeductions': total_ded,
                              'lopDays': lop_days, 'lopDeduction': lop_deduct,
                              'clEncashment': cl_enc,
                              'clEncashmentDays': body.get('clEncashmentDays', 0),
                              'netPay': net, 'status': 'finalized',
                              'generatedAt': now_iso(),
                              'generatedBy': body.get('generatedBy', '')}
                if draft:
                    db['payslips'] = [ps_data if p.get('id') == draft['id'] else p
                                      for p in db['payslips']]
                else:
                    db.setdefault('payslips', []).append(ps_data)
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'payslip': ps_data})

            if path == '/purchase_orders/set_cost':
                po_id = body.get('poId')
                pos   = db.get('purchase_orders', [])
                po    = next((p for p in pos if p.get('id') == po_id), None)
                if not po: return self.send_err('PO not found', 404)
                po['actualCost']    = body.get('actualCost', 0)
                po['costEnteredBy'] = body.get('enteredBy', '')
                po['costEnteredAt'] = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'po': po})

            _erp_upsert = {
                '/suppliers/save':       'suppliers',
                '/customers/save':       'customers',
                '/quotations/save':      'quotations',
                '/purchase_orders/save': 'purchase_orders',
                '/inventory/update':     'inventory',
            }
            if path in _erp_upsert:
                col  = _erp_upsert[path]
                item = dict(body)
                iid  = item.get('id') or uuid.uuid4().hex[:12]
                item['id'] = iid
                item['_updated'] = now_iso()
                lst  = db.get(col, [])
                idx  = next((i for i, x in enumerate(lst) if x.get('id') == iid), None)
                if idx is not None:
                    lst[idx] = item
                else:
                    item['_created'] = now_iso()
                    lst.append(item)
                db[col] = lst
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'item': item})

            if path == '/installation_logs/submit':
                log = {'id': uuid.uuid4().hex[:12],
                       'projectId':    body.get('projectId', ''),
                       'pmId':         body.get('pmId', ''),
                       'description':  body.get('description', ''),
                       'location':     body.get('location', ''),
                       'locationName': body.get('locationName', ''),
                       'workDate':     body.get('workDate', ''),
                       'attachments':  body.get('attachments', []),
                       'status':       'submitted',
                       'submittedBy':  body.get('submittedBy', ''),
                       'createdAt':    now_iso(), 'pmReview': None}
                db.setdefault('installation_logs', []).append(log)
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'log': log}, 201)

            if path == '/installation_logs/review':
                lid    = body.get('logId')
                logs   = db.get('installation_logs', [])
                log    = next((l for l in logs if l.get('id') == lid), None)
                if not log: return self.send_err('Installation log not found', 404)
                action = body.get('action', 'approve')
                log['status']   = 'approved' if action == 'approve' else 'rejected'
                log['pmReview'] = {'action': action,
                                   'reviewedBy': body.get('reviewedBy', ''),
                                   'note': body.get('note', ''),
                                   'at': now_iso()}
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'log': log})


            # ══════════════════════════════════════════════════════════════════
            # ITERATION 2 — NEW WORKFLOW ENDPOINTS
            # ══════════════════════════════════════════════════════════════════

            # ── INQUIRIES ─────────────────────────────────────────────────────
            if path in ('/inquiries/save',):
                col  = 'inquiries'
                item = dict(body)
                iid  = item.get('id') or uuid.uuid4().hex[:12]
                item['id'] = iid
                item['_updated'] = now_iso()
                lst  = db.get(col, [])
                idx  = next((i for i, x in enumerate(lst) if x.get('id') == iid), None)
                if idx is not None:
                    lst[idx] = item
                else:
                    item.setdefault('status', 'open')
                    item.setdefault('documents', [])
                    item.setdefault('siteVisits', [])
                    item.setdefault('conceptDesigns', [])
                    item.setdefault('salesNumber', '')
                    item['_created'] = now_iso()
                    lst.append(item)
                db[col] = lst
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'item': item})

            if path == '/inquiries/add_document':
                iid  = body.get('inquiryId')
                lst  = db.get('inquiries', [])
                inq  = next((x for x in lst if x.get('id') == iid), None)
                if not inq: return self.send_err('Inquiry not found', 404)
                doc  = {'id': uuid.uuid4().hex[:8],
                        'name': body.get('name', ''),
                        'url':  body.get('url', ''),
                        'type': body.get('type', ''),
                        'uploadedBy': body.get('uploadedBy', ''),
                        'uploadedAt': now_iso()}
                inq.setdefault('documents', []).append(doc)
                inq['_updated'] = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'doc': doc})

            if path == '/inquiries/add_site_visit':
                iid  = body.get('inquiryId')
                lst  = db.get('inquiries', [])
                inq  = next((x for x in lst if x.get('id') == iid), None)
                if not inq: return self.send_err('Inquiry not found', 404)
                visit = {'id': uuid.uuid4().hex[:8],
                         'date':           body.get('date', ''),
                         'location':       body.get('location', ''),
                         'machines':       body.get('machines', ''),
                         'notes':          body.get('notes', ''),
                         'nextSteps':      body.get('nextSteps', ''),
                         'team':           body.get('team', ''),
                         'attachment':     body.get('attachment'),
                         'attachmentName': body.get('attachmentName'),
                         'visitedBy':      body.get('visitedBy', ''),
                         'createdAt':      now_iso()}
                inq.setdefault('siteVisits', []).append(visit)
                inq['_updated'] = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'visit': visit})

            if path == '/inquiries/add_concept_design':
                iid  = body.get('inquiryId')
                lst  = db.get('inquiries', [])
                inq  = next((x for x in lst if x.get('id') == iid), None)
                if not inq: return self.send_err('Inquiry not found', 404)
                cd   = {'id': uuid.uuid4().hex[:8],
                        'title':          body.get('title', ''),
                        'description':    body.get('description', ''),
                        'fileRef':        body.get('fileRef', ''),
                        'fileUrl':        body.get('fileUrl', ''),
                        'notes':          body.get('notes', ''),
                        'roughCosting':   body.get('roughCosting', 0),
                        'attachment':     body.get('attachment'),
                        'attachmentName': body.get('attachmentName'),
                        'createdBy':      body.get('createdBy', ''),
                        'createdAt':      now_iso()}
                inq.setdefault('conceptDesigns', []).append(cd)
                inq['_updated'] = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'design': cd})

            if path == '/inquiries/convert_to_project':
                iid  = body.get('inquiryId')
                lst  = db.get('inquiries', [])
                inq  = next((x for x in lst if x.get('id') == iid), None)
                if not inq: return self.send_err('Inquiry not found', 404)
                # Create project from inquiry
                proj_id   = uuid.uuid4().hex[:12]
                proj_num  = body.get('projectNumber', f'PRJ-{proj_id[:6].upper()}')
                new_proj  = {'id': proj_id,
                             'name':        inq.get('customerName', '') + ' — ' + inq.get('title', ''),
                             'projectNumber': proj_num,
                             'customerId':  inq.get('customerId', ''),
                             'inquiryId':   iid,
                             'salesNumber': inq.get('salesNumber', ''),
                             'poValue':     body.get('poValue', 0),
                             'status':      'active',
                             'assignedTo':  body.get('assignedTo', []),
                             'startDate':   now_iso()[:10],
                             '_created':    now_iso(),
                             '_updated':    now_iso()}
                db.setdefault('projects', []).append(new_proj)
                # Mark inquiry converted
                inq['status']     = 'converted'
                inq['projectId']  = proj_id
                inq['convertedAt'] = now_iso()
                inq['_updated']   = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'project': new_proj, 'inquiry': inq})

            # ── KICKOFF MEETINGS ──────────────────────────────────────────────
            if path == '/kickoff_meetings/save':
                col  = 'kickoff_meetings'
                item = dict(body)
                iid  = item.get('id') or uuid.uuid4().hex[:12]
                item['id'] = iid
                item['_updated'] = now_iso()
                lst  = db.get(col, [])
                idx  = next((i for i, x in enumerate(lst) if x.get('id') == iid), None)
                if idx is not None:
                    lst[idx] = item
                else:
                    item.setdefault('status', 'scheduled')
                    item.setdefault('teamEstimates', [])
                    item.setdefault('pmBudgets', {})
                    item['_created'] = now_iso()
                    lst.append(item)
                db[col] = lst
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'item': item})

            if path == '/kickoff_meetings/team_submit':
                kid    = body.get('kickoffId')
                lst    = db.get('kickoff_meetings', [])
                km     = next((x for x in lst if x.get('id') == kid), None)
                if not km: return self.send_err('Kickoff meeting not found', 404)
                dept   = body.get('department', '')
                est    = {'id': uuid.uuid4().hex[:8],
                          'department':   dept,
                          'submittedBy':  body.get('submittedBy', ''),
                          'budget':       body.get('budget', 0),
                          'timeline':     body.get('timeline', ''),
                          'acceptance':   body.get('acceptance', True),
                          'notes':        body.get('notes', ''),
                          'submittedAt':  now_iso()}
                # Replace existing estimate for same dept
                km.setdefault('teamEstimates', [])
                km['teamEstimates'] = [e for e in km['teamEstimates']
                                       if e.get('department') != dept]
                km['teamEstimates'].append(est)
                # Budget alert: check against PM budget if set
                pm_budget = km.get('pmBudgets', {}).get(dept, 0)
                alert     = False
                if pm_budget and est['budget'] > pm_budget:
                    alert = True
                km['_updated'] = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'estimate': est,
                                       'budgetAlert': alert,
                                       'pmBudget': pm_budget})

            if path == '/kickoff_meetings/set_pm_budget':
                kid    = body.get('kickoffId')
                lst    = db.get('kickoff_meetings', [])
                km     = next((x for x in lst if x.get('id') == kid), None)
                if not km: return self.send_err('Kickoff meeting not found', 404)
                km.setdefault('pmBudgets', {})
                km['pmBudgets'].update(body.get('budgets', {}))
                km['pmBudgetSetBy'] = body.get('setBy', '')
                km['pmBudgetSetAt'] = now_iso()
                km['_updated']      = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'pmBudgets': km['pmBudgets']})


            # ── KICKOFF DEPT ALLOCATION ──────────────────────────────────────

            if path == '/kickoff_meetings/dept_submit':
                kid      = body.get('kickoffId')
                dept_key = body.get('deptKey', '')   # design | controls | production | installation
                lst      = db.get('kickoff_meetings', [])
                km       = next((x for x in lst if x.get('id') == kid), None)
                if not km:
                    return self.send_err('Kickoff not found', 404)

                phase = km.get('phase', 'design_pending')

                # Validate phase for v3 workflow
                if dept_key == 'design' and phase not in ('design_pending',):
                    return self.send_err('Design can only allocate after all departments have accepted the project.', 400)
                if dept_key != 'design' and phase not in ('allocation',):
                    return self.send_err('Design sub-assembly allocation must be approved before other departments can submit.', 400)

                sub_assemblies = body.get('subAssemblies', [])
                submitted_budget = sum(float(sa.get('budget', 0)) for sa in sub_assemblies)

                km.setdefault('deptSubmissions', {})
                km['deptSubmissions'].setdefault(dept_key, {})
                ds = km['deptSubmissions'][dept_key]

                allocated = float(ds.get('allocatedBudget', 0))
                overage_pct = round(((submitted_budget - allocated) / allocated * 100), 1) if allocated else 0

                ds['subAssemblies']   = sub_assemblies
                ds['submittedBudget'] = submitted_budget
                ds['overagePct']      = overage_pct
                ds['status']          = 'submitted'
                ds['submittedBy']     = body.get('submittedBy', '')
                ds['submittedAt']     = now_iso()
                ds['tlComment']       = ''
                ds['pmComment']       = ''

                if dept_key == 'design':
                    km['phase'] = 'design_submitted'

                km['_updated'] = now_iso()
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True, 'submittedBudget': submitted_budget, 'overagePct': overage_pct})

            if path == '/kickoff_meetings/pm_review':
                kid      = body.get('kickoffId')
                dept_key = body.get('deptKey', '')
                action   = body.get('action', '')   # 'approve' | 'reject'
                comment  = body.get('comment', '')
                lst      = db.get('kickoff_meetings', [])
                km       = next((x for x in lst if x.get('id') == kid), None)
                if not km:
                    return self.send_err('Kickoff not found', 404)

                km.setdefault('deptSubmissions', {})
                km['deptSubmissions'].setdefault(dept_key, {})
                ds = km['deptSubmissions'][dept_key]

                allocated = float(ds.get('allocatedBudget', 0))
                submitted = float(ds.get('submittedBudget', 0))
                overage_pct = round(((submitted - allocated) / allocated * 100), 1) if allocated else 0

                if action == 'approve':
                    ds['status']     = 'approved'
                    ds['pmComment']  = comment
                    ds['reviewedAt'] = now_iso()
                    ds['reviewedBy'] = body.get('reviewedBy', '')
                    # If Design approved → unlock other depts
                    if dept_key == 'design':
                        km['phase'] = 'allocation'
                        # Seed other dept submissions with allocatedBudget from pmBudgets
                        pm_budgets = km.get('pmBudgets', {})
                        for dk, disp in [('controls','Controls'),('production','Production'),('installation','Installation')]:
                            km['deptSubmissions'].setdefault(dk, {
                                'status': 'pending',
                                'allocatedBudget': float(pm_budgets.get(disp, 0)),
                                'subAssemblies': [],
                                'submittedBudget': 0,
                                'overagePct': 0,
                                'pmComment': '',
                                'tlComment': '',
                                'submittedAt': None,
                                'reviewedAt': None,
                            })
                elif action == 'reject':
                    ds['status']      = 'rejected'
                    ds['overagePct']  = overage_pct
                    overage_msg = f" Budget is {overage_pct}% {'above' if overage_pct>0 else 'below'} the allocated ₹{allocated:,.0f}." if allocated else ""
                    ds['pmComment']   = comment or f"Allocation rejected.{overage_msg} Please revalidate."
                    ds['reviewedAt']  = now_iso()
                    ds['reviewedBy']  = body.get('reviewedBy', '')
                    if dept_key == 'design':
                        km['phase'] = 'design_pending'

                km['_updated'] = now_iso()
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True, 'status': ds['status'], 'pmComment': ds.get('pmComment','')})

            if path == '/kickoff_meetings/tl_justify':
                kid      = body.get('kickoffId')
                dept_key = body.get('deptKey', '')
                action   = body.get('action', '')     # 'revise' | 'justify'
                lst      = db.get('kickoff_meetings', [])
                km       = next((x for x in lst if x.get('id') == kid), None)
                if not km:
                    return self.send_err('Kickoff not found', 404)

                km.setdefault('deptSubmissions', {})
                ds = km['deptSubmissions'].get(dept_key, {})

                if action == 'revise':
                    sub_assemblies   = body.get('subAssemblies', [])
                    submitted_budget = sum(float(sa.get('budget', 0)) for sa in sub_assemblies)
                    allocated        = float(ds.get('allocatedBudget', 0))
                    overage_pct      = round(((submitted_budget - allocated) / allocated * 100), 1) if allocated else 0
                    ds['subAssemblies']   = sub_assemblies
                    ds['submittedBudget'] = submitted_budget
                    ds['overagePct']      = overage_pct
                    ds['status']          = 'submitted'
                    ds['submittedBy']     = body.get('submittedBy', '')
                    ds['submittedAt']     = now_iso()
                    ds['tlComment']       = ''
                    if dept_key == 'design':
                        km['phase'] = 'design_submitted'

                elif action == 'justify':
                    ds['tlComment'] = body.get('justification', '')
                    ds['status']    = 'justification_sent'

                ds['pmComment'] = ''   # clear PM comment so PM sees fresh state
                km['deptSubmissions'][dept_key] = ds
                km['_updated'] = now_iso()
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True, 'status': ds['status']})

            if path == '/kickoff_meetings/pm_final_review':
                # Same logic as pm_review but called after TL justification
                kid      = body.get('kickoffId')
                dept_key = body.get('deptKey', '')
                action   = body.get('action', '')
                comment  = body.get('comment', '')
                lst      = db.get('kickoff_meetings', [])
                km       = next((x for x in lst if x.get('id') == kid), None)
                if not km:
                    return self.send_err('Kickoff not found', 404)

                km.setdefault('deptSubmissions', {})
                ds = km['deptSubmissions'].get(dept_key, {})

                if action == 'approve':
                    ds['status']     = 'approved'
                    ds['pmComment']  = comment or 'Approved after justification review.'
                    ds['reviewedAt'] = now_iso()
                    ds['reviewedBy'] = body.get('reviewedBy', '')
                    if dept_key == 'design':
                        km['phase'] = 'allocation'
                        pm_budgets = km.get('pmBudgets', {})
                        for dk, disp in [('controls','Controls'),('production','Production'),('installation','Installation')]:
                            km['deptSubmissions'].setdefault(dk, {
                                'status': 'pending',
                                'allocatedBudget': float(pm_budgets.get(disp, 0)),
                                'subAssemblies': [],
                                'submittedBudget': 0,
                                'overagePct': 0,
                                'pmComment': '',
                                'tlComment': '',
                                'submittedAt': None,
                                'reviewedAt': None,
                            })
                elif action == 'reject':
                    ds['status']    = 'rejected'
                    ds['pmComment'] = comment or 'Allocation rejected after justification review.'
                    ds['reviewedAt'] = now_iso()
                    ds['reviewedBy'] = body.get('reviewedBy', '')
                    if dept_key == 'design':
                        km['phase'] = 'design_pending'

                km['deptSubmissions'][dept_key] = ds
                km['_updated'] = now_iso()
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True, 'status': ds['status']})


            # ── KICKOFF v3: ZIP UPLOAD / DOWNLOAD / ACCEPTANCE ────────────────

            if path == '/kickoff_meetings/upload_zip':
                import base64 as _b64
                kid       = body.get('kickoffId', 'unknown')
                filename  = body.get('filename', 'project.zip')
                content64 = body.get('content', '')
                if not content64:
                    return self.send_err('No file content provided', 400)
                # Sanitize filename
                safe_name    = re.sub(r'[^\w\-_\.]', '_', filename)
                server_name  = f'{kid}_{safe_name}'
                files_dir    = os.path.join(BASE_DIR, 'data', 'files', 'kickoff')
                os.makedirs(files_dir, exist_ok=True)
                file_path    = os.path.join(files_dir, server_name)
                try:
                    with open(file_path, 'wb') as _f:
                        _f.write(_b64.b64decode(content64))
                except Exception as ex:
                    return self.send_err(f'File save failed: {ex}', 500)
                # Update kickoff record with the filename
                lst = db.get('kickoff_meetings', [])
                km  = next((x for x in lst if x.get('id') == kid), None)
                if km:
                    km.setdefault('projectZip', {})
                    km['projectZip']['type']       = 'upload'
                    km['projectZip']['serverFile']  = server_name
                    km['projectZip']['filename']    = filename
                    km['_updated'] = now_iso()
                    save_db(db)
                    update_latest(db)
                return self.send_json({'success': True, 'serverFile': server_name})

            if path == '/kickoff_meetings/dept_accept':
                kid      = body.get('kickoffId')
                dept_key = body.get('deptKey', '')
                action   = body.get('action', '')   # 'accept' | 'reject'
                notes    = body.get('notes', '')
                lst      = db.get('kickoff_meetings', [])
                km       = next((x for x in lst if x.get('id') == kid), None)
                if not km:
                    return self.send_err('Kickoff not found', 404)

                km.setdefault('deptAcceptance', {})
                km['deptAcceptance'].setdefault(dept_key, {'status': 'pending', 'notes': '', 'respondedAt': None})
                da = km['deptAcceptance'][dept_key]
                da['status']      = 'accepted' if action == 'accept' else 'rejected'
                da['notes']       = notes
                da['respondedAt'] = now_iso()
                da['respondedBy'] = body.get('userId', '')

                # Check if ALL depts have accepted → move to design_pending
                all_depts  = ['design', 'controls', 'production', 'installation']
                all_accepted = all(
                    km['deptAcceptance'].get(dk, {}).get('status') == 'accepted'
                    for dk in all_depts
                )
                if all_accepted:
                    km['phase'] = 'design_pending'
                elif action == 'reject' and km.get('phase') == 'design_pending':
                    # Revert phase if a dept rejects after all had accepted
                    km['phase'] = 'acceptance'
                else:
                    # Keep or set acceptance phase
                    if km.get('phase') not in ('design_pending', 'design_submitted', 'allocation', 'completed'):
                        km['phase'] = 'acceptance'

                km['_updated'] = now_iso()
                save_db(db)
                update_latest(db)
                return self.send_json({
                    'success':     True,
                    'status':      da['status'],
                    'phase':       km.get('phase'),
                    'allAccepted': all_accepted,
                })

            if path == '/kickoff_meetings/pm_resolve_rejection':
                kid      = body.get('kickoffId')
                dept_key = body.get('deptKey', '')
                pm_note  = body.get('pmNote', '')
                lst      = db.get('kickoff_meetings', [])
                km       = next((x for x in lst if x.get('id') == kid), None)
                if not km:
                    return self.send_err('Kickoff not found', 404)

                km.setdefault('deptAcceptance', {})
                km['deptAcceptance'].setdefault(dept_key, {})
                da = km['deptAcceptance'][dept_key]
                # Re-open for this dept to re-submit acceptance
                da['status']         = 'pending'
                da['pmResolutionNote'] = pm_note
                da['resolvedAt']     = now_iso()
                da['resolvedBy']     = body.get('pmId', '')
                da['respondedAt']    = None  # clear previous response
                # Phase reverts to acceptance
                km['phase']      = 'acceptance'
                km['_updated']   = now_iso()
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True, 'phase': km['phase']})

            # ── SUPPLIER QUOTES ───────────────────────────────────────────────
            if path == '/supplier_quotes/save':
                col  = 'supplier_quotes'
                item = dict(body)
                iid  = item.get('id') or uuid.uuid4().hex[:12]
                item['id'] = iid
                item['_updated'] = now_iso()
                lst  = db.get(col, [])
                idx  = next((i for i, x in enumerate(lst) if x.get('id') == iid), None)
                if idx is not None:
                    lst[idx] = item
                else:
                    item.setdefault('status', 'pending')
                    item['_created'] = now_iso()
                    lst.append(item)
                db[col] = lst
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'item': item})

            if path == '/supplier_quotes/accept':
                qid  = body.get('quoteId')
                lst  = db.get('supplier_quotes', [])
                q    = next((x for x in lst if x.get('id') == qid), None)
                if not q: return self.send_err('Quote not found', 404)
                # Mark this one accepted, others for same BOM item as rejected
                bom_id = q.get('bomItemId', '')
                for x in lst:
                    if x.get('bomItemId') == bom_id and x.get('id') != qid:
                        x['status'] = 'rejected'
                q['status']     = 'accepted'
                q['acceptedBy'] = body.get('acceptedBy', '')
                q['acceptedAt'] = now_iso()
                q['_updated']   = now_iso()
                db['supplier_quotes'] = lst
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'quote': q})

            if path == '/supplier_quotes/raise_po':
                qid  = body.get('quoteId')
                lst  = db.get('supplier_quotes', [])
                q    = next((x for x in lst if x.get('id') == qid), None)
                if not q: return self.send_err('Quote not found', 404)
                if q.get('status') != 'accepted':
                    return self.send_err('Quote must be accepted before raising PO', 400)
                # Create a PO from the accepted quote
                po_id  = uuid.uuid4().hex[:12]
                po_num = body.get('poNumber', f'PO-{po_id[:6].upper()}')
                po     = {'id': po_id,
                          'poNumber':    po_num,
                          'supplierId':  q.get('supplierId', ''),
                          'supplierName': q.get('supplierName', ''),
                          'bomItemId':   q.get('bomItemId', ''),
                          'quantity':    q.get('quantity', 0),
                          'unitPrice':   q.get('unitPrice', 0),
                          'totalAmount': q.get('totalAmount', 0),
                          'currency':    q.get('currency', 'INR'),
                          'deliveryDate': body.get('deliveryDate', ''),
                          'status':      'draft',
                          'fromQuoteId': qid,
                          'raisedBy':    body.get('raisedBy', ''),
                          '_created':    now_iso(),
                          '_updated':    now_iso()}
                db.setdefault('purchase_orders', []).append(po)
                q['poId']      = po_id
                q['poRaisedAt'] = now_iso()
                q['_updated']  = now_iso()
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'po': po, 'quote': q})

            # ── PROCESS RATES ─────────────────────────────────────────────────
            if path == '/process_rates/save':
                col  = 'process_rates'
                item = dict(body)
                iid  = item.get('id') or uuid.uuid4().hex[:12]
                item['id'] = iid
                item['_updated'] = now_iso()
                lst  = db.get(col, [])
                idx  = next((i for i, x in enumerate(lst) if x.get('id') == iid), None)
                if idx is not None:
                    lst[idx] = item
                else:
                    item['_created'] = now_iso()
                    lst.append(item)
                db[col] = lst
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'item': item})

            if path == '/process_rates/delete':
                rid  = body.get('id')
                db['process_rates'] = [x for x in db.get('process_rates', [])
                                       if x.get('id') != rid]
                save_db(db); update_latest(db)
                return self.send_json({'success': True})

            # ── TIME LOGS ─────────────────────────────────────────────────────
            if path == '/time_logs/record':
                log = {'id': uuid.uuid4().hex[:12],
                       'userId':    body.get('userId', ''),
                       'action':    body.get('action', ''),
                       'context':   body.get('context', {}),
                       'page':      body.get('page', ''),
                       'ts':        body.get('ts', now_iso())}
                db.setdefault('time_logs', []).append(log)
                # Keep last 5000 entries to cap memory
                if len(db['time_logs']) > 5000:
                    db['time_logs'] = db['time_logs'][-5000:]
                save_db(db); update_latest(db)
                return self.send_json({'success': True, 'id': log['id']})

            # ── END ITERATION 2 ENDPOINTS ─────────────────────────────────────

            self.send_err('Not found', 404)

    # ── File upload handler ───────────────────────────────────────────────────

    def _handle_upload(self):
        """Handle POST /upload — multipart/form-data parser (no cgi module needed).
        Works on all Python versions including 3.13+ and on Windows with binary files."""
        import email as _email
        ctype = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in ctype:
            return self.send_err('Expected multipart/form-data', 400)

        # Extract boundary
        boundary = None
        for part in ctype.split(';'):
            part = part.strip()
            if part.lower().startswith('boundary='):
                boundary = part[9:].strip('"').strip()
                break
        if not boundary:
            return self.send_err('Missing boundary in Content-Type', 400)

        # Read full body (safe for PDF/IGES/STEP sizes)
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 550 * 1024 * 1024:
                return self.send_err('Request body too large (max 550 MB)', 413)
            raw_body = self.rfile.read(content_length)
        except Exception as e:
            return self.send_err(f'Failed to read request body: {e}', 400)

        # Parse multipart using email module (reliable, works on Windows with binary)
        try:
            mime_bytes = (
                f'MIME-Version: 1.0\r\nContent-Type: {ctype}\r\n\r\n'
            ).encode() + raw_body
            msg = _email.message_from_bytes(mime_bytes)
        except Exception as e:
            return self.send_err(f'Multipart parse error: {e}', 400)

        file_data   = None
        orig_name   = 'upload'
        file_type   = 'unknown'
        bom_item_id = ''

        if msg.is_multipart():
            for part in msg.get_payload():
                cd = part.get('Content-Disposition', '')
                # Extract field name
                field_name = None
                for seg in cd.split(';'):
                    seg = seg.strip()
                    if seg.lower().startswith('name='):
                        field_name = seg[5:].strip('"')
                        break
                if field_name is None:
                    continue

                if field_name == 'file':
                    file_data = part.get_payload(decode=True)
                    # Extract filename from Content-Disposition
                    for seg in cd.split(';'):
                        seg = seg.strip()
                        if seg.lower().startswith('filename='):
                            orig_name = seg[9:].strip('"')
                            break
                elif field_name == 'fileType':
                    payload = part.get_payload(decode=True)
                    file_type = (payload or b'').decode('utf-8', errors='replace').strip().lower()
                elif field_name == 'bomItemId':
                    payload = part.get_payload(decode=True)
                    bom_item_id = (payload or b'').decode('utf-8', errors='replace').strip()

        if file_data is None:
            return self.send_err('No file found in request', 400)
        if not orig_name or orig_name == 'upload':
            return self.send_err('No filename provided', 400)

        # Sanitize filename and build save path
        orig_name  = os.path.basename(orig_name.replace('\\', '/'))
        safe_name  = re.sub(r'[^a-zA-Z0-9._()-]', '_', orig_name)
        file_id    = uuid.uuid4().hex[:16]
        saved_name = f"{file_id}_{safe_name}"
        save_path  = os.path.join(FILES_DIR, saved_name)

        # Enforce size limits before writing
        file_size = len(file_data)
        limit = FILE_LIMITS.get(file_type, LIMIT_STEP)
        if file_size > limit:
            limit_mb  = limit // (1024 * 1024)
            actual_mb = file_size / (1024 * 1024)
            return self.send_err(
                f'{file_type.upper()} too large: {actual_mb:.1f} MB exceeds {limit_mb} MB limit', 413
            )

        # Write to disk
        try:
            with open(save_path, 'wb') as out:
                out.write(file_data)
        except Exception as e:
            try: os.remove(save_path)
            except: pass
            return self.send_err(f'File write error: {e}', 500)

        file_url = f'/files/{saved_name}'
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Uploaded {file_type}: {orig_name} ({file_size/1048576:.2f} MB) -> {saved_name}")

        return self.send_json({
            'success':   True,
            'fileId':    file_id,
            'fileName':  orig_name,
            'savedName': saved_name,
            'fileUrl':   file_url,
            'fileSize':  file_size,
            'fileType':  file_type,
        })

    # ── PUT ──────────────────────────────────────────────────────────────────

    def do_PUT(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        body   = self.read_body()
        parts  = path.split('/')

        with LOCK:
            db = load_db()

            if len(parts) == 3 and parts[1] in ('tasks','users','projects','issues','notifications','escalations'):
                col, item_id = parts[1], parts[2]
                items = db.get(col, [])
                idx = next((i for i, x in enumerate(items) if x.get('id') == item_id), None)
                if idx is None:
                    return self.send_err('Not found', 404)
                items[idx].update(body)
                items[idx]['_updated'] = now_iso()
                db[col] = items
                save_db(db)
                update_latest(db)
                return self.send_json(items[idx])

            if path == '/timesheets':
                db['timesheets'] = body
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True})

            if path == '/project_assemblies':
                db['project_assemblies'] = body
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True})

        self.send_err('Not found', 404)

    # ── PATCH — partial update with optimistic concurrency ────────────────────
    #
    # Endpoint: PATCH /tasks/:id
    # Body: { fields: {...}, _v: <expected_version_int> }
    #
    # If _v matches server's current task._v → apply fields, increment _v, 200 OK.
    # If _v mismatches → 409 Conflict with current server task (client re-fetches).
    # If _v omitted → apply unconditionally (backward-compat).
    #
    def do_PATCH(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        body   = self.read_body()
        parts  = path.split('/')

        with LOCK:
            db = load_db()

            if len(parts) == 3 and parts[1] == 'tasks':
                task_id = parts[2]
                tasks   = db.get('tasks', [])
                idx     = next((i for i, t in enumerate(tasks) if t.get('id') == task_id), None)
                if idx is None:
                    return self.send_err('Task not found', 404)

                task        = tasks[idx]
                client_v    = body.get('_v')        # may be None (omitted)
                server_v    = task.get('_v', 0)

                if client_v is not None and client_v != server_v:
                    # Conflict — return current server task so client can merge
                    return self.send_json({'conflict': True, 'task': task}, 409)

                fields = body.get('fields', body)   # support both {fields:{...}} and flat body
                fields.pop('_v', None)
                task.update(fields)
                task['_v']       = server_v + 1
                task['_updated'] = now_iso()
                db['tasks']      = tasks
                save_db(db)
                update_latest(db)
                return self.send_json(task)

        self.send_err('Not found', 404)

    # ── DELETE ────────────────────────────────────────────────────────────────

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        parts  = path.split('/')

        with LOCK:
            db = load_db()
            if len(parts) == 3 and parts[1] in (
                'tasks', 'projects', 'issues', 'users',
                'notifications', 'archived_members', 'escalations',
                'suppliers', 'customers', 'quotations',
                'purchase_orders', 'inventory',
                'leave_requests', 'installation_logs'):
                col, item_id = parts[1], parts[2]
                db[col] = [x for x in db.get(col, []) if x.get('id') != item_id]
                save_db(db)
                update_latest(db)
                return self.send_json({'success': True})

        self.send_err('Not found', 404)

# ─── Entry point ──────────────────────────────────────────────────────────────

def run(host='0.0.0.0', port=3000):
    global _last_backup_time

    db = load_db()

    try:
        fname = _write_backup(db, 'startup')
        print(f"  Startup backup → backups/{fname}")
    except Exception as e:
        print(f"  ⚠️  Startup backup failed: {e}")

    t = threading.Thread(target=_hourly_backup_loop, daemon=True)
    t.start()

    t2 = threading.Thread(target=_jan1_scheduler_loop, daemon=True)
    t2.start()

    print("=" * 60)
    print("  ⚡  TaskFlow JSON Server  v2 — concurrent-safe")
    print("=" * 60)
    print(f"  URL          : http://localhost:{port}")
    print(f"  LAN          : http://<YOUR-IP>:{port}")
    print(f"  Data         : {DB_PATH}")
    print(f"  Activity log : {ACTIVITY_PATH}")
    print(f"  Backups      : {BACKUP_DIR}")
    print(f"  Users        : {len(db.get('users',[]))}")
    print(f"  Tasks        : {len(db.get('tasks',[]))}")
    print(f"  Escalations  : {len(db.get('escalations',[]))}")
    print(f"  Merge mode   : per-record (timestamp wins) ✅")
    print(f"  Auto-backup  : every 60 minutes ✅")
    print(f"  Jan1 reset   : EL+ML expire, CL encashed ✅")
    print(f"  File storage : {FILES_DIR}")
    print(f"  Limits       : PDF 25MB | IGES 50MB | STEP 500MB")
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    HTTPServer((host, port), Handler).serve_forever()

if __name__ == '__main__':
    run()
