from __future__ import annotations
import csv, hashlib, hmac, io, json, math, os, re, secrets, sqlite3, threading, time, uuid
from collections import defaultdict, deque
from pathlib import Path
from flask import Flask, Response, jsonify, request, send_file
from waitress import serve

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN") or secrets.token_urlsafe(32)
API_TOKEN = os.getenv("API_TOKEN") or secrets.token_urlsafe(32)
APP = Flask(__name__)
APP.config["MAX_CONTENT_LENGTH"] = 3 * 1024 * 1024
LOCK = threading.RLock()
SIGNAL = threading.Condition(LOCK)
DB = sqlite3.connect(DATA / "game.sqlite", check_same_thread=False)
DB.row_factory = sqlite3.Row
DB.execute("PRAGMA journal_mode=WAL")
DB.execute("PRAGMA synchronous=FULL")
DB.executescript("""
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS joined(run TEXT,id TEXT,joined_ms INTEGER,
 PRIMARY KEY(run,id));
CREATE TABLE IF NOT EXISTS scores(run TEXT,id TEXT,points INTEGER NOT NULL,
 correct_ms INTEGER NOT NULL,correct INTEGER NOT NULL,PRIMARY KEY(run,id));
CREATE TABLE IF NOT EXISTS answers(run TEXT,qid TEXT,id TEXT,question_id TEXT,
 answer INTEGER,correct INTEGER,elapsed_ms INTEGER,at INTEGER,
 PRIMARY KEY(run,qid,id));
CREATE TABLE IF NOT EXISTS events(run TEXT,kind TEXT,text TEXT,at INTEGER);
""")
DB.commit()
ANCHOR_WALL = time.time_ns() // 1_000_000
ANCHOR_MONO = time.monotonic_ns()
def now_ms():
    return ANCHOR_WALL + (time.monotonic_ns() - ANCHOR_MONO) // 1_000_000
def uid():
    return uuid.uuid4().hex
SETTINGS = dict(time=20, points=1000, reveal_ms=650, auto_next=False,
 result_seconds=6, show_correct=True, allow_late=True, join_locked=False,
 require_qid=False, mask_phones=False, phone_number="", sound=False)
def question(text, options, correct, seconds=None, points=None):
    return dict(id=uid(), text=text, options=options, correct=correct,
                kind="poll" if correct is None else "quiz", seconds=seconds, points=points)
DEFAULT_CONFIG = dict(title="טריוויה על הקו", settings=SETTINGS.copy(), players=[], questions=[
 question("איזה כוכב לכת הוא הגדול ביותר במערכת השמש?",["נוגה","מאדים","צדק","שבתאי"],3),
 question("באיזו שנה הוקמה מדינת ישראל?",["1917","1948","1967","1982"],2,15),
 question("איך אתם מעדיפים את הקפה שלכם?",["אספרסו","הפוך","שחור","בלי קפה"],None,15,0),
 question("כמה עצמות יש בגוף של אדם בוגר?",["106","206","306","406"],2),
 question("איזה יסוד מסומן באותיות Au?",["כסף","זהב","נחושת","ברזל"],2,20,1500),
 question("מהי עיר הבירה של אוסטרליה?",["סידני","מלבורן","קנברה","פרת׳"],3,20,2000)
])
def new_runtime():
    return dict(run_id=uid(),phase="lobby",index=-1,qid=None,revealed=0,
        reveal_at=None,opened_at=None,deadline=None,next_at=None,seconds=20,
        points=1000,join_locked=False)
def load(k, fallback):
    row = DB.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(row["v"]) if row else fallback
CONFIG = load("config", DEFAULT_CONFIG)
RT = load("runtime", new_runtime())
# אחרי אתחול לא ממשיכים חלון תשובות ישן.
if RT["phase"] in ("reveal", "open", "closed"):
    RT.update(phase="closed", next_at=None, deadline=None)
    if 0 <= RT["index"] < len(CONFIG["questions"]):
        RT["revealed"] = len(CONFIG["questions"][RT["index"]]["options"])
VERSION = 0
RATE = defaultdict(deque)

def normalize_id(value):
    s = str(value or "").strip()
    if not s or len(s) > 64 or any(ord(c) < 32 for c in s):
        raise ValueError("מזהה לא תקין")
    if re.fullmatch(r"[+\d\s().-]+", s):
        s = re.sub(r"[\s().-]", "", s)
        if s.startswith("+972"): s = "0" + s[4:]
        elif s.startswith("972") and len(s) == 12: s = "0" + s[3:]
    return s

def number(value, lo, hi, integer=False):
    if isinstance(value, bool): raise ValueError("ערך מספרי לא תקין")
    v = float(value)
    if not math.isfinite(v) or not lo <= v <= hi or (integer and v != int(v)):
        raise ValueError("מספר מחוץ לטווח")
    return int(v) if integer else v

def validate_config(data):
    if not isinstance(data, dict): raise ValueError("מבנה קובץ לא תקין")
    title = str(data.get("title", "")).strip()
    if not 1 <= len(title) <= 120: raise ValueError("נדרשת כותרת עד 120 תווים")
    incoming = data.get("settings", {})
    settings = {k: incoming.get(k,v) for k,v in SETTINGS.items()}
    for k,lo,hi,integer in [("time",.5,3600,False),("points",0,1000000,True),
        ("reveal_ms",0,3000,True),("result_seconds",1,120,False)]:
        settings[k] = number(settings[k],lo,hi,integer)
    for k in ("auto_next","show_correct","allow_late","join_locked","require_qid","mask_phones","sound"):
        if not isinstance(settings[k],bool): raise ValueError("הגדרה בוליאנית לא תקינה")
    settings["phone_number"] = str(settings["phone_number"])[:64]
    questions, ids = [], set()
    if not isinstance(data.get("questions"),list) or len(data["questions"]) > 1000:
        raise ValueError("עד 1,000 שאלות")
    for row in data["questions"]:
        qid = str(row.get("id") or uid())
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}",qid) or qid in ids:
            raise ValueError("מזהה שאלה כפול או לא תקין")
        ids.add(qid)
        text = str(row.get("text","")).strip()
        opts = [str(x).strip() for x in row.get("options",[])]
        if not text or len(text)>1000 or len(opts) not in (3,4) or any(not x or len(x)>300 for x in opts):
            raise ValueError("לכל שאלה נדרשים טקסט ו־3 או 4 תשובות")
        kind = row.get("kind","quiz")
        if kind not in ("quiz","poll"): raise ValueError("סוג שאלה לא תקין")
        correct = None if kind=="poll" else number(row.get("correct"),1,len(opts),True)
        seconds = None if row.get("seconds") in (None,"") else number(row["seconds"],.5,3600)
        points = None if row.get("points") in (None,"") else number(row["points"],0,1000000,True)
        questions.append(dict(id=qid,text=text,options=opts,correct=correct,kind=kind,seconds=seconds,points=points))
    players, seen = [], set()
    for row in data.get("players",[]):
        pid = normalize_id(row.get("id"))
        if pid in seen: raise ValueError("מזהה שחקן כפול")
        seen.add(pid)
        players.append(dict(id=pid,name=str(row.get("name","")).strip()[:120]))
    return dict(title=title,settings=settings,questions=questions,players=players)

def player_name(pid):
    return next((p["name"] for p in CONFIG["players"] if p["id"]==pid and p["name"]),pid)
def event(kind, text):
    DB.execute("INSERT INTO events VALUES(?,?,?,?)",(RT["run_id"],kind,text,now_ms()))
def current():
    return CONFIG["questions"][RT["index"]] if 0 <= RT["index"] < len(CONFIG["questions"]) else None
def persist():
    for k,v in (("config",CONFIG),("runtime",RT)):
        DB.execute("INSERT INTO kv VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                   (k,json.dumps(v,ensure_ascii=False)))
def admin_state():
    run = RT["run_id"]
    participants = [dict(id=r["id"],joined_ms=r["joined_ms"]) for r in
        DB.execute("SELECT * FROM joined WHERE run=? ORDER BY joined_ms",(run,))]
    scores = {p["id"]:dict(points=0,correct_ms=0,correct=0) for p in participants}
    for r in DB.execute("SELECT * FROM scores WHERE run=?",(run,)):
        scores[r["id"]] = dict(points=r["points"],correct_ms=r["correct_ms"],correct=r["correct"])
    answers = []
    for r in DB.execute("SELECT * FROM answers WHERE run=? ORDER BY at",(run,)):
        answers.append(dict(qid=r["qid"],id=r["id"],question_id=r["question_id"],answer=r["answer"],
             correct=None if r["correct"] is None else bool(r["correct"]),elapsed_ms=r["elapsed_ms"],at=r["at"]))
    events = [dict(kind=r["kind"],text=r["text"],at=r["at"]) for r in
        DB.execute("SELECT * FROM events WHERE run=? ORDER BY rowid DESC LIMIT 30",(run,))]
    return dict(**CONFIG,runtime=RT.copy(),participants=participants,scores=scores,
                answers=answers,events=events,server_ms=now_ms(),version=VERSION)
def ranked(state):
    rows = [dict(id=pid,name=player_name(pid),**score) for pid,score in state["scores"].items()]
    rows.sort(key=lambda r:(-r["points"],r["correct_ms"],r["id"]))
    previous, rank = None, 0
    for i,r in enumerate(rows):
        key = (r["points"],r["correct_ms"])
        if key != previous: rank = i+1
        r["rank"], previous = rank, key
    return rows
def public_state():
    s = admin_state()
    def display(pid):
        n = player_name(pid)
        if CONFIG["settings"]["mask_phones"] and n == pid:
            return "••••" + pid[-4:]
        return n
    def public_id(pid):
        return hashlib.sha256((RT["run_id"]+":"+pid).encode()).hexdigest()[:16]
    q = current()
    safe_q = None
    if q and RT["phase"] != "lobby":
        safe_q = dict(text=q["text"],kind=q["kind"],option_count=len(q["options"]),
            options=[v if i<RT["revealed"] else None for i,v in enumerate(q["options"])],
            correct=q["correct"] if RT["phase"] in ("closed","finished") and CONFIG["settings"]["show_correct"] else None)
    ranking = [dict(key=public_id(r["id"]),name=display(r["id"]),points=r["points"],
        correct_ms=r["correct_ms"],rank=r["rank"]) for r in ranked(s)]
    a = [r for r in s["answers"] if r["qid"]==RT["qid"]]
    return dict(title=CONFIG["title"],runtime=RT.copy(),question=safe_q,
        question_count=len(CONFIG["questions"]),
        settings={k:CONFIG["settings"][k] for k in ("phone_number","show_correct","mask_phones")},
        participants=[dict(key=public_id(p["id"]),name=display(p["id"])) for p in s["participants"]],
        ranking=ranking,votes=[sum(r["answer"]==i for r in a) for i in range(1,5)],
        answered=len(a),server_ms=now_ms(),version=VERSION)

def csv_value(v):
    if isinstance(v,str) and v.lstrip().startswith(("=","+","-","@")): return "'"+v
    return v
def csv_text(headers,rows):
    out = io.StringIO(newline="")
    w = csv.writer(out)
    w.writerow(headers)
    w.writerows([[csv_value(v) for v in row] for row in rows])
    return "\ufeff"+out.getvalue()
def export_content(kind):
    s = admin_state()
    if kind=="scores":
        return csv_text(["rank","id","name","points","correct_time_seconds","correct_answers"],
            [[r["rank"],r["id"],r["name"],r["points"],f'{r["correct_ms"]/1000:.3f}',r["correct"]] for r in ranked(s)])
    if kind=="answers":
        return csv_text(["run_id","question_run_id","question_id","id","answer","correct","elapsed_seconds","received_at_ms"],
            [[RT["run_id"],r["qid"],r["question_id"],r["id"],r["answer"],
              "" if r["correct"] is None else int(r["correct"]),f'{r["elapsed_ms"]/1000:.3f}',r["at"]] for r in s["answers"]])
    raise ValueError("סוג ייצוא לא תקין")
def atomic_write(path,text):
    tmp = path.with_suffix(path.suffix+".tmp")
    tmp.write_text(text,encoding="utf-8")
    os.replace(tmp,path)
def export_files():
    folder = DATA / "runs" / RT["run_id"]
    folder.mkdir(parents=True,exist_ok=True)
    s = admin_state()
    score_json = json.dumps({pid:dict(points=v["points"],
        correct_time_seconds=f'{v["correct_ms"]/1000:.3f}',correct_answers=v["correct"])
        for pid,v in s["scores"].items()},ensure_ascii=False,indent=2)
    for target in (DATA,folder):
        atomic_write(target/"scores.json",score_json)
        atomic_write(target/"scores.csv",export_content("scores"))
        atomic_write(target/"answers.csv",export_content("answers"))
    atomic_write(DATA/"questions.json",json.dumps(CONFIG["questions"],ensure_ascii=False,indent=2))
    atomic_write(DATA/"players.json",json.dumps(CONFIG["players"],ensure_ascii=False,indent=2))
def changed(files=False):
    global VERSION
    persist()
    DB.commit()
    VERSION += 1
    SIGNAL.notify_all()
    if files:
        try: export_files()
        except OSError: APP.logger.exception("Export failed; SQLite remains authoritative")

def begin(index):
    q = CONFIG["questions"][index]
    t = now_ms()
    RT.update(phase="reveal",index=index,qid=uid(),revealed=0,
       reveal_at=t+CONFIG["settings"]["reveal_ms"],opened_at=None,deadline=None,next_at=None,
       seconds=q["seconds"] if q["seconds"] is not None else CONFIG["settings"]["time"],
       points=0 if q["kind"]=="poll" else (q["points"] if q["points"] is not None else CONFIG["settings"]["points"]))
    event("question",f'שאלה {index+1} מוצגת')
def close_question():
    RT.update(phase="closed",revealed=len(current()["options"]),
       next_at=now_ms()+int(CONFIG["settings"]["result_seconds"]*1000) if CONFIG["settings"]["auto_next"] else None)
    event("question","חלון התשובות נסגר")
def next_question():
    if RT["index"]+1 >= len(CONFIG["questions"]):
        RT.update(phase="finished",next_at=None)
        event("finish","המשחק הסתיים")
    else: begin(RT["index"]+1)

def scheduler():
    while True:
        time.sleep(.025)
        with LOCK:
            t, dirty = now_ms(), False
            if RT["phase"]=="reveal" and t>=RT["reveal_at"]:
                if CONFIG["settings"]["reveal_ms"]==0:
                    RT["revealed"]=len(current()["options"])
                else:
                    RT["revealed"]+=1
                RT["reveal_at"]=t+CONFIG["settings"]["reveal_ms"]
                if RT["revealed"]>=len(current()["options"]):
                    RT.update(phase="open",opened_at=t,deadline=t+int(RT["seconds"]*1000))
                    event("question","אפשר לענות עכשיו")
                dirty=True
            elif RT["phase"]=="open" and t>=RT["deadline"]:
                close_question(); dirty=True
            elif RT["phase"]=="closed" and RT["next_at"] and t>=RT["next_at"]:
                next_question(); dirty=True
            if dirty: changed()

def token_ok(expected):
    value=request.headers.get("X-API-Key","")
    auth=request.headers.get("Authorization","")
    if auth.startswith("Bearer "): value=auth[7:]
    if not value: value=request.values.get("token","")
    return hmac.compare_digest(str(value),expected)
def admin_required():
    return token_ok(ADMIN_TOKEN)
def plain(value,status=200):
    return Response(value,status=status,content_type="text/plain; charset=utf-8")
def may_join(pid):
    exists=DB.execute("SELECT 1 FROM joined WHERE run=? AND id=?",(RT["run_id"],pid)).fetchone()
    return bool(exists) or (not RT["join_locked"] and RT["phase"]!="finished"
        and (RT["phase"]=="lobby" or CONFIG["settings"]["allow_late"]))
def join(pid):
    found=DB.execute("SELECT 1 FROM joined WHERE run=? AND id=?",(RT["run_id"],pid)).fetchone()
    if not found:
        DB.execute("INSERT INTO joined VALUES(?,?,?)",(RT["run_id"],pid,now_ms()))
        event("join","הצטרפות · "+player_name(pid))

@APP.after_request
def headers(response):
    response.headers["Cache-Control"]="no-store"
    response.headers["X-Content-Type-Options"]="nosniff"
    response.headers["Referrer-Policy"]="no-referrer"
    # אין CORS פתוח: מומלץ להגיש את המסך ואת ה-API מאותו origin.
    return response

@APP.get("/")
def index():
    return send_file(BASE/"index.html")
@APP.get("/api/state")
def state_public():
    with LOCK: return jsonify(public_state())
@APP.get("/api/events")
def stream():
    def generate():
        previous=-1
        while True:
            with SIGNAL:
                if VERSION==previous: SIGNAL.wait(timeout=15)
                v=VERSION
            if v!=previous:
                yield "data: "+json.dumps(dict(version=v))+"\n\n"
                previous=v
            else: yield ": heartbeat\n\n"
    return Response(generate(),mimetype="text/event-stream",
        headers={"X-Accel-Buffering":"no","Cache-Control":"no-cache"})

@APP.get("/api/admin/state")
def state_admin():
    if not admin_required(): return jsonify(error="מפתח מנהל לא תקין"),401
    with LOCK: return jsonify(admin_state())
@APP.put("/api/admin/config")
def update_config():
    global CONFIG
    if not admin_required(): return jsonify(error="אין הרשאה"),401
    try: incoming=validate_config(request.get_json(force=True))
    except (ValueError,TypeError,KeyError,AttributeError) as exc:
        return jsonify(error=str(exc)),400
    with LOCK:
        if RT["phase"] not in ("lobby","finished") and (
            incoming["questions"]!=CONFIG["questions"] or incoming["settings"]!=CONFIG["settings"]):
            return jsonify(error="אפשר לשנות שאלות והגדרות רק לפני המשחק או בסיומו"),409
        CONFIG=incoming
        if RT["phase"]=="lobby": RT["join_locked"]=CONFIG["settings"]["join_locked"]
        event("config","המאגר עודכן")
        changed(True)
        return jsonify(admin_state())
@APP.post("/api/admin/action")
def action():
    global RT
    if not admin_required(): return jsonify(error="אין הרשאה"),401
    name=(request.get_json(silent=True) or {}).get("action")
    with LOCK:
        if name=="start" and RT["phase"]=="lobby" and CONFIG["questions"]:
            begin(0)
        elif name=="next" and RT["phase"]=="closed": next_question()
        elif name=="close" and RT["phase"] in ("open","reveal"): close_question()
        elif name=="toggle_lock":
            RT["join_locked"]=not RT["join_locked"]
            event("config","ההרשמה ננעלה" if RT["join_locked"] else "ההרשמה נפתחה")
        elif name=="reset":
            export_files()
            RT=new_runtime()
            RT["join_locked"]=CONFIG["settings"]["join_locked"]
            event("reset","נפתח משחק חדש")
        else: return jsonify(error="הפעולה אינה זמינה כעת"),409
        changed(True)
        return jsonify(admin_state())
@APP.get("/api/admin/export")
def export():
    if not admin_required(): return jsonify(error="אין הרשאה"),401
    kind=request.args.get("kind","scores")
    with LOCK:
        if kind=="backup":
            return Response(json.dumps(admin_state(),ensure_ascii=False,indent=2),
               content_type="application/json",headers={"Content-Disposition":"attachment; filename=backup.json"})
        if kind not in ("scores","answers"): return jsonify(error="סוג לא תקין"),400
        return Response(export_content(kind),content_type="text/csv; charset=utf-8",
            headers={"Content-Disposition":f"attachment; filename={kind}.csv"})
@APP.get("/api/phone/state")
def phone_state():
    if not token_ok(API_TOKEN): return plain("ERROR",401)
    with LOCK:
        q=current()
        return jsonify(qid=RT["qid"],phase=RT["phase"],question_number=RT["index"]+1,
            option_count=len(q["options"]) if q else 0,
            deadline_ms=RT["deadline"],server_ms=now_ms())

@APP.route("/api/phone",methods=["GET","POST"])
def phone():
    if not token_ok(API_TOKEN): return plain("ERROR",401)
    payload=request.get_json(silent=True) if request.is_json else request.values
    payload=payload or {}
    try: pid=normalize_id(payload.get("ApiPhone"))
    except ValueError: return plain("ERROR")
    with LOCK:
        t=now_ms()
        times=RATE[pid]
        while times and t-times[0]>10000: times.popleft()
        if len(times)>=30: return plain("ERROR",429)
        times.append(t)
        # מגבלה גם על מספר מזהים בזיכרון.
        if len(RATE)>20000:
            for key in list(RATE):
                if not RATE[key] or t-RATE[key][-1]>10000: del RATE[key]
        raw=payload.get("answer")
        if raw is None or str(raw).strip()=="":
            if not may_join(pid): return plain("ERROR")
            join(pid); changed(True)
            return plain("OK")
        if RT["phase"]!="open" or t>=RT["deadline"]: return plain("ERROR")
        qid=str(payload.get("qid") or "")
        if (CONFIG["settings"]["require_qid"] and not qid) or (qid and qid!=RT["qid"]):
            return plain("ERROR")
        q=current()
        if not re.fullmatch(r"[1-4]",str(raw).strip()): return plain("ERROR")
        choice=int(str(raw).strip())
        if choice>len(q["options"]) or not may_join(pid): return plain("ERROR")
        if DB.execute("SELECT 1 FROM answers WHERE run=? AND qid=? AND id=?",
            (RT["run_id"],RT["qid"],pid)).fetchone(): return plain("ERROR")
        elapsed=max(0,t-RT["opened_at"])
        correct=None if q["kind"]=="poll" else choice==q["correct"]
        # UNIQUE + נעילה + טרנזקציה: גם בקשות מקבילות לא מנקדות פעמיים.
        try:
            with DB:
                join(pid)
                DB.execute("INSERT INTO answers VALUES(?,?,?,?,?,?,?,?)",
                    (RT["run_id"],RT["qid"],pid,q["id"],choice,
                     None if correct is None else int(correct),elapsed,t))
                if correct is True:
                    DB.execute("""INSERT INTO scores VALUES(?,?,?,?,1)
                    ON CONFLICT(run,id) DO UPDATE SET
                    points=scores.points+excluded.points,
                    correct_ms=scores.correct_ms+excluded.correct_ms,
                    correct=scores.correct+1""",(RT["run_id"],pid,RT["points"],elapsed))
                event("answer","תשובה נקלטה · "+player_name(pid))
                persist()
        except sqlite3.IntegrityError:
            return plain("ERROR")
        changed(True)
        return plain("ERROR" if correct is False else "OK")

with LOCK:
    persist()
    DB.commit()
    export_files()
threading.Thread(target=scheduler,daemon=True).start()

if __name__=="__main__":
    print("\nQviz — Python 3.10+")
    print("ADMIN_TOKEN =",ADMIN_TOKEN)
    print("API_TOKEN   =",API_TOKEN)
    print("Open http://127.0.0.1:"+os.getenv("PORT","8000"))
    print("Use an HTTPS reverse proxy for external telephony access.\n")
    # תהליך אחד, מספר threads. אין להפעיל כמה workers עם מצב המשחק הזה.
    serve(APP,host=os.getenv("HOST","0.0.0.0"),
          port=int(os.getenv("PORT","8000")),threads=16)
