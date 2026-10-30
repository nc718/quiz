# -*- coding: utf-8 -*-
"""קליק: one-process Flask server, SQLite persistence, SSE and phone API."""
import os, json, time, secrets, sqlite3, threading, copy, csv, io, hashlib, shutil
from pathlib import Path
from flask import Flask, request, jsonify, Response, session, send_file, stream_with_context

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
(DATA / "games").mkdir(exist_ok=True)

def secret_file(name):
    p = DATA / name
    if not p.exists():
        p.write_text(secrets.token_urlsafe(32), encoding="utf-8")
        try: p.chmod(0o600)
        except OSError: pass
    return p.read_text(encoding="utf-8").strip()

ADMIN_TOKEN = os.environ.get("CLICK_ADMIN_TOKEN") or secret_file(".admin-token")
app = Flask(__name__)
app.secret_key = os.environ.get("CLICK_SESSION_SECRET") or secret_file(".session-secret")
app.config.update(MAX_CONTENT_LENGTH=5*1024*1024, SESSION_COOKIE_HTTPONLY=True,
                  SESSION_COOKIE_SAMESITE="Strict",
                  SESSION_COOKIE_SECURE=os.environ.get("COOKIE_SECURE") == "1")
LOCK = threading.RLock()
CV = threading.Condition(LOCK)
db = sqlite3.connect(DATA / "click.db", check_same_thread=False)
db.execute("PRAGMA journal_mode=WAL")
db.execute("CREATE TABLE IF NOT EXISTS live (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
db.execute("CREATE TABLE IF NOT EXISTS history (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
db.commit()
now = lambda: time.time_ns() // 1_000_000

def new_game():
    return dict(id="g_"+secrets.token_hex(6), createdAt=now(), phase="lobby",
                qIndex=0, revealed=0, openedAt=None, closedAt=None, pausedAt=None,
                pauseTotal=0, nextAt=None, revealAt=None, duration=20, points=100)

def sample_questions():
    return [
      dict(id="q1",type="quiz",text="איזה כוכב לכת הוא הקרוב ביותר לשמש?",options=["נוגה","כדור הארץ","כוכב חמה","מאדים"],correct=3,time=20,points=100),
      dict(id="q2",type="quiz",text="כמה שחקנים יש על המגרש בקבוצת כדורסל?",options=["4","5","6","7"],correct=2,time=None,points=150),
      dict(id="q3",type="quiz",text="איזו שפה פותחה על ידי גידו ואן רוסום?",options=["Python","Java","HTML","C++"],correct=1,time=15,points=None),
      dict(id="q4",type="survey",text="מה הכי חשוב במשחק מוצלח?",options=["האתגר","החברים","האווירה","הניצחון"],correct=None,time=None,points=0),
      dict(id="q5",type="quiz",text="מהי העיר הגדולה ביותר בישראל לפי אוכלוסייה?",options=["ירושלים","תל אביב־יפו","חיפה","באר שבע"],correct=1,time=None,points=None),
      dict(id="q6",type="quiz",text="כמה צבעים יש בקשת בענן?",options=["5","6","7"],correct=3,time=15,points=200)
    ]

def fresh():
    return dict(revision=0, settings=dict(title="ערב טריוויה",defaultTime=20,
        defaultPoints=100,transition="manual",transitionDelay=5,autoReveal=True,
        revealDelay=1.2,shuffle=False,maskPhones=False,
        phoneNumber="077-000-0000",phoneKey=secrets.token_urlsafe(20)),
        questions=sample_questions(),players=[],scores={},answers={},
        game=new_game(),history=[])

row = db.execute("SELECT body FROM live WHERE id=1").fetchone()
S = json.loads(row[0]) if row else fresh()

def ranking(s):
    return sorted(s["players"], key=lambda p: (
      -s["scores"].get(p["id"],{}).get("points",0),
       s["scores"].get(p["id"],{}).get("timeMs",0),p.get("name") or p["id"]))

def summary(s):
    ranked=ranking(s)
    top=ranked[0] if ranked else None
    winner=(top.get("name") or top["id"]) if top and s["scores"].get(top["id"],{}).get("points",0)>0 else "ללא ניקוד"
    return dict(id=s["game"]["id"],title=s["settings"]["title"],
        createdAt=s["game"]["createdAt"],endedAt=now(),
        totalPlayers=len(s["players"]),totalQuestions=len(s["questions"]),winner=winner)

def history_summaries():
    items=[]
    for (body,) in db.execute("SELECT body FROM history"):
        item=json.loads(body)
        items.append({k:v for k,v in item.items() if k!="snapshot"})
    return sorted(items,key=lambda x:x["endedAt"],reverse=True)
S["history"]=history_summaries()

def safe_csv(v):
    if isinstance(v,str) and v.startswith(("=","+","-","@","\t","\r")): return "'"+v
    return v

def atomic_text(path,text):
    tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(text,encoding="utf-8")
    tmp.replace(path)

def csv_write(path,rows):
    tmp=path.with_suffix(".tmp")
    with tmp.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.writer(f)
        for row in rows: w.writerow([safe_csv(v) for v in row])
    tmp.replace(path)

def exports(s):
    directory=DATA/"games"/s["game"]["id"]
    directory.mkdir(exist_ok=True)
    rows=[["rank","ApiPhone","name","points","correct_time_ms","correct_count"]]
    for index,p in enumerate(ranking(s),1):
        score=s["scores"].get(p["id"],{})
        rows.append([index,p["id"],p.get("name",""),score.get("points",0),
                     score.get("timeMs",0),score.get("correct",0)])
    csv_write(directory/"scores.csv",rows)
    rows=[["question_id","ApiPhone","answer","correct","elapsed_ms","points","received_at_ms"]]
    for qid,answers in s["answers"].items():
        for pid,a in answers.items():
            rows.append([qid,pid,a["choice"],int(a["correct"]),a["elapsed"],
                         a["points"],a["at"]])
    csv_write(directory/"answers.csv",rows)
    snap={k:v for k,v in s.items() if k!="history"}
    atomic_text(directory/"game.json",json.dumps(snap,ensure_ascii=False,indent=2))
    return directory

def persist():
    # All callers hold LOCK: validation, duplicate prevention and score changes
    # are serialized. SQLite commits the entire live state atomically.
    S["revision"]+=1
    db.execute("INSERT OR REPLACE INTO live VALUES (1,?)",
               (json.dumps(S,ensure_ascii=False),))
    db.commit()
    exports(S)
    csv_write(DATA/"players.csv",[["ApiPhone","name"]]+[
        [p["id"],p.get("name","")] for p in S["players"]])
    atomic_text(DATA/"questions.json",json.dumps(S["questions"],ensure_ascii=False,indent=2))
    CV.notify_all()

def private_view():
    return bool(session.get("admin")) and request.args.get("audience")!="1"

def view(private=False):
    s=copy.deepcopy(S)
    s["serverNow"]=now()
    if not private:
        mapping={p["id"]:"p_"+hashlib.sha256(
            (str(app.secret_key)+p["id"]).encode()).hexdigest()[:16] for p in s["players"]}
        for p in s["players"]:
            original=p["id"]
            shown=original
            if s["settings"].get("maskPhones") and len(original)>5:
                shown=original[:3]+"••••"+original[-3:]
            p["name"]=p.get("name") or shown
            p["id"]=mapping[original]
        s["scores"]={mapping[k]:v for k,v in s["scores"].items() if k in mapping}
        s["answers"]={qid:{mapping[k]:v for k,v in answers.items() if k in mapping}
                      for qid,answers in s["answers"].items()}
        s["settings"].pop("phoneKey",None)
        s["history"]=[]
        # Correct answers are not sent to the audience before results.
        for i,q in enumerate(s["questions"]):
            if i>s["game"]["qIndex"] or (i==s["game"]["qIndex"] and
                    s["game"]["phase"] not in ("results","finished")):
                q["correct"]=None
    return s

def integer(value,low,high,label):
    try:
        number=float(value)
        if not number.is_integer() or not low<=number<=high: raise ValueError()
        return int(number)
    except (TypeError,ValueError,OverflowError):
        raise ValueError(label+" אינו תקין")

def normalize_id(value):
    value=str(value or "").strip()
    if not value or len(value)>64: raise ValueError("נדרש מזהה תקין, עד 64 תווים")
    return value

def validate_question(raw):
    text=str(raw.get("text","")).strip()
    options=[str(x).strip() for x in raw.get("options",[])]
    if not text or len(text)>300: raise ValueError("נדרש נוסח שאלה, עד 300 תווים")
    if len(options) not in (3,4) or any(not x or len(x)>120 for x in options):
        raise ValueError("נדרשות 3 או 4 תשובות, עד 120 תווים לכל תשובה")
    kind="survey" if raw.get("type")=="survey" else "quiz"
    correct=None if kind=="survey" else integer(raw.get("correct"),1,len(options),"מספר תשובה")
    limit=None if raw.get("time") in (None,"") else integer(raw["time"],1,600,"זמן")
    points=0 if kind=="survey" else (None if raw.get("points") in (None,"")
                                  else integer(raw["points"],0,10000,"ניקוד"))
    return dict(id=str(raw.get("id") or "q_"+secrets.token_hex(6)),text=text,
                options=options,type=kind,correct=correct,time=limit,points=points)

def lobby_only():
    if S["game"]["phase"]!="lobby":
        raise ValueError("עריכת שאלות זמינה בלובי. פתחו משחק חדש כדי לשנות את המשחק.")

def question():
    return S["questions"][S["game"]["qIndex"]]

def reveal_question(t):
    g=S["game"]
    g.update(phase="reveal",revealed=0,openedAt=None,closedAt=None,
             pausedAt=None,pauseTotal=0,nextAt=None)
    g["revealAt"]=t+round(S["settings"]["revealDelay"]*1000) if S["settings"]["autoReveal"] else None

def open_question(t):
    q=question();g=S["game"]
    g.update(phase="answering",revealed=len(q["options"]),openedAt=t,
             pausedAt=None,pauseTotal=0,revealAt=None,nextAt=None,
             duration=q["time"] if q.get("time") is not None else S["settings"]["defaultTime"],
             points=(q["points"] if q.get("points") is not None else S["settings"]["defaultPoints"])
             if q["type"]=="quiz" else 0)

def close_question(t):
    g=S["game"]
    g.update(phase="results",closedAt=t,pausedAt=None,revealAt=None)
    g["nextAt"]=t+S["settings"]["transitionDelay"]*1000 if S["settings"]["transition"]=="auto" else None

def step(t):
    g=S["game"]
    if g["phase"]=="lobby":
        if not S["questions"]: raise ValueError("הוסיפו שאלה לפני תחילת המשחק")
        if S["settings"]["shuffle"]:
            import random
            random.shuffle(S["questions"])
        g["qIndex"]=0
        reveal_question(t)
    elif g["phase"]=="reveal":
        if g["revealed"]<len(question()["options"]):
            g["revealed"]+=1
            g["revealAt"]=t+round(S["settings"]["revealDelay"]*1000) if S["settings"]["autoReveal"] else None
        else: open_question(t)
    elif g["phase"]=="answering": close_question(t)
    elif g["phase"]=="results":
        if g["qIndex"]+1>=len(S["questions"]):
            g.update(phase="finished",nextAt=None)
        else:
            g["qIndex"]+=1
            reveal_question(t)

def tick(t):
    g=S["game"]
    if g["phase"]=="answering" and g["pausedAt"] is None:
        if t-g["openedAt"]-g["pauseTotal"]>=g["duration"]*1000:
            close_question(t);return True
    if g["phase"]=="reveal" and g.get("revealAt") is not None and t>=g["revealAt"]:
        step(t);return True
    if g["phase"]=="results" and g.get("nextAt") is not None and t>=g["nextAt"]:
        step(t);return True
    return False

def action(kind,p):
    t=now();g=S["game"]
    if kind in ("start","step"):
        if kind=="start" and g["phase"]!="lobby": raise ValueError("המשחק כבר התחיל")
        step(t)
    elif kind=="open":
        if g["phase"]!="reveal": raise ValueError("המענה אינו ניתן לפתיחה כעת")
        open_question(t)
    elif kind=="back":
        if g["phase"]=="reveal":
            g["revealed"]=max(0,g["revealed"]-1)
            g["revealAt"]=t+round(S["settings"]["revealDelay"]*1000) if S["settings"]["autoReveal"] else None
    elif kind=="pause":
        if g["phase"]!="answering": raise ValueError("אפשר להשהות רק בזמן מענה")
        if g["pausedAt"] is None: g["pausedAt"]=t
        else:
            g["pauseTotal"]+=t-g["pausedAt"]
            g["pausedAt"]=None
    elif kind=="finish":
        g.update(phase="finished",nextAt=None,revealAt=None,pausedAt=None)
    elif kind=="reset":
        snap=copy.deepcopy(S);snap.pop("history",None)
        entry=summary(snap);entry["snapshot"]=snap
        exports(snap)
        db.execute("INSERT OR REPLACE INTO history VALUES (?,?)",
                   (entry["id"],json.dumps(entry,ensure_ascii=False)))
        S.update(game=new_game(),scores={},answers={})
        S["history"]=history_summaries()
    elif kind=="save_question":
        lobby_only();q=validate_question(p["question"])
        index=next((i for i,x in enumerate(S["questions"]) if x["id"]==q["id"]),None)
        if index is None: S["questions"].append(q)
        else: S["questions"][index]=q
    elif kind=="delete_question":
        lobby_only();S["questions"]=[q for q in S["questions"] if q["id"]!=p["id"]]
    elif kind=="reorder":
        lobby_only();ids=p["ids"];existing={q["id"]:q for q in S["questions"]}
        if len(ids)!=len(existing) or set(ids)!=set(existing): raise ValueError("סדר שאלות אינו תקין")
        S["questions"]=[existing[x] for x in ids]
    elif kind=="import_questions":
        lobby_only()
        if not isinstance(p.get("questions"),list) or len(p["questions"])>1000:
            raise ValueError("ניתן לייבא עד 1,000 שאלות")
        questions=[validate_question({**x,"id":"q_"+secrets.token_hex(6)}) for x in p["questions"]]
        S["questions"]=questions if p.get("replace") else S["questions"]+questions
    elif kind in ("save_player","import_players"):
        rows=p["players"] if kind=="import_players" else [p["player"]]
        if len(rows)>20000: raise ValueError("יותר מדי משתתפים")
        rows=[dict(id=normalize_id(x.get("id")),name=str(x.get("name","")).strip()[:80]) for x in rows]
        existing={x["id"]:x for x in S["players"]}
        for row in rows:
            if row["id"] in existing: existing[row["id"]]["name"]=row["name"]
            else:
                player={**row,"connected":False,"joinedAt":None,"lastSeen":None}
                S["players"].append(player);existing[row["id"]]=player
    elif kind=="disconnect":
        for player in S["players"]:
            if player["id"]==p["id"]: player["connected"]=False
    elif kind=="save_settings":
        raw=p["settings"];settings=copy.deepcopy(S["settings"])
        title=str(raw.get("title","")).strip()
        if not title or len(title)>80: raise ValueError("נדרש שם משחק, עד 80 תווים")
        key=str(raw.get("phoneKey","")).strip()
        if len(key)<8 or len(key)>128: raise ValueError("מפתח API חייב להכיל 8–128 תווים")
        settings.update(title=title,phoneKey=key,
            phoneNumber=str(raw.get("phoneNumber",""))[:40],
            defaultTime=integer(raw.get("defaultTime"),1,600,"זמן ברירת מחדל"),
            defaultPoints=integer(raw.get("defaultPoints"),0,10000,"ניקוד ברירת מחדל"),
            transition="auto" if raw.get("transition")=="auto" else "manual",
            transitionDelay=integer(raw.get("transitionDelay"),1,60,"זמן מעבר"))
        try: delay=float(raw.get("revealDelay",1.2))
        except (ValueError,TypeError): raise ValueError("זמן חשיפה אינו תקין")
        if not .4<=delay<=10: raise ValueError("זמן החשיפה הוא 0.4–10 שניות")
        settings["revealDelay"]=delay
        for flag in ("autoReveal","shuffle","maskPhones"): settings[flag]=bool(raw.get(flag))
        S["settings"]=settings
    elif kind in ("delete_history","clear_history"):
        ids=[p["id"]] if kind=="delete_history" else [h["id"] for h in S["history"]]
        allowed={h["id"] for h in S["history"]}
        for game_id in ids:
            if game_id not in allowed: continue
            db.execute("DELETE FROM history WHERE id=?",(game_id,))
            shutil.rmtree(DATA/"games"/game_id,ignore_errors=True)
        S["history"]=history_summaries()
    else: raise ValueError("פעולה לא מוכרת")

@app.get("/")
def index():
    if not (ROOT/"index.html").exists():
        return "Save the downloaded HTML as index.html next to app.py.",500
    return send_file(ROOT/"index.html")

@app.get("/api/meta")
def meta():
    return jsonify(server=True,authenticated=bool(session.get("admin")))

attempts={}
@app.post("/api/login")
def login():
    ip=request.remote_addr or "unknown";t=time.time()
    attempts[ip]=[v for v in attempts.get(ip,[]) if t-v<60]
    if len(attempts[ip])>=8: return jsonify(error="נסו שוב בעוד דקה"),429
    body=request.get_json(silent=True) or {}
    token=body.get("token","") if isinstance(body,dict) else ""
    if not secrets.compare_digest(str(token),ADMIN_TOKEN):
        attempts[ip].append(t)
        return jsonify(error="מפתח הניהול אינו נכון"),401
    session.clear();session["admin"]=True;attempts.pop(ip,None)
    return jsonify(ok=True)

@app.post("/api/logout")
def logout():
    session.clear();return jsonify(ok=True)

@app.get("/api/state")
def get_state():
    with LOCK: return jsonify(view(private_view()))

@app.get("/api/events")
def events():
    private=private_view()
    @stream_with_context
    def generate():
        revision=-1
        while True:
            with CV:
                if revision==S["revision"]: CV.wait(timeout=12)
                changed=revision!=S["revision"]
                if changed:
                    body=json.dumps(view(private),ensure_ascii=False)
                    revision=S["revision"]
            if changed: yield "data: "+body+"\n\n"
            else: yield ": heartbeat\n\n"
    return Response(generate(),mimetype="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.post("/api/action")
def admin_action():
    if not session.get("admin"): return jsonify(error="נדרשת התחברות מנהל"),401
    if not request.is_json: return jsonify(error="JSON required"),415
    body=request.get_json(silent=True) or {}
    if not isinstance(body,dict): return jsonify(error="בקשה אינה תקינה"),400
    try:
        with LOCK:
            tick(now())
            action(body.get("type"),body.get("payload") or {})
            persist()
            return jsonify(ok=True,state=view(True))
    except (ValueError,KeyError,TypeError,IndexError) as e:
        return jsonify(error=str(e)),400

@app.route("/api/phone",methods=["GET","POST"])
def phone():
    params=request.args.to_dict()
    params.update(request.form.to_dict())
    body=request.get_json(silent=True)
    if isinstance(body,dict): params.update(body)
    def reply(text,status=200):
        return Response(text,status=status,content_type="text/plain; charset=utf-8",
                        headers={"Cache-Control":"no-store"})
    with LOCK:
        if not secrets.compare_digest(str(params.get("key","")),S["settings"]["phoneKey"]):
            return reply("ERROR",403)
        try: pid=normalize_id(params.get("ApiPhone"))
        except ValueError: return reply("ERROR")
        t=now();tick(t)
        player=next((x for x in S["players"] if x["id"]==pid),None)
        if str(params.get("hangup","")).lower()=="yes":
            if player: player["connected"]=False;player["lastSeen"]=t
            persist();return reply("בסדר")
        if not player:
            player=dict(id=pid,name="",connected=True,joinedAt=t,lastSeen=t)
            S["players"].append(player)
        else:
            if not player["connected"]: player["joinedAt"]=t
            player["connected"]=True;player["lastSeen"]=t
        if params.get("תשובה") in (None,""):
            persist();return reply("בסדר")
        g=S["game"]
        if g["phase"]!="answering" or g["pausedAt"] is not None:
            persist();return reply("ERROR")
        q=question()
        try: choice=integer(params["תשובה"],1,len(q["options"]),"תשובה")
        except ValueError:
            persist();return reply("ERROR")
        answers=S["answers"].setdefault(q["id"],{})
        if pid in answers:
            persist();return reply("ERROR")
        elapsed=max(0,t-g["openedAt"]-g["pauseTotal"])
        if elapsed>=g["duration"]*1000:
            close_question(t);persist();return reply("ERROR")
        correct=q["type"]=="quiz" and choice==q["correct"]
        earned=g["points"] if correct else 0
        answers[pid]=dict(choice=choice,correct=correct,elapsed=elapsed,points=earned,at=t)
        if correct:
            score=S["scores"].setdefault(pid,dict(points=0,timeMs=0,correct=0))
            score["points"]+=earned;score["timeMs"]+=elapsed;score["correct"]+=1
        persist()
        return reply("בסדר" if correct or q["type"]=="survey" else "ERROR")

@app.get("/api/history/<game_id>")
def history_item(game_id):
    if not session.get("admin"): return jsonify(error="נדרשת התחברות מנהל"),401
    with LOCK:
        row=db.execute("SELECT body FROM history WHERE id=?",(game_id,)).fetchone()
        if not row: return jsonify(error="המשחק אינו קיים"),404
        return jsonify(json.loads(row[0])["snapshot"])

@app.get("/api/export")
def export_file():
    if not session.get("admin"): return jsonify(error="נדרשת התחברות מנהל"),401
    game_id=request.args.get("game")
    kind=request.args.get("kind","scores")
    if kind not in ("scores","answers"): return jsonify(error="Invalid export"),400
    with LOCK:
        if not game_id or game_id==S["game"]["id"]: snap=copy.deepcopy(S)
        else:
            row=db.execute("SELECT body FROM history WHERE id=?",(game_id,)).fetchone()
            if not row: return jsonify(error="המשחק אינו קיים"),404
            snap=json.loads(row[0])["snapshot"]
        path=exports(snap)/(kind+".csv")
        content=path.read_bytes()
    return Response(content,mimetype="text/csv",
        headers={"Content-Disposition":f'attachment; filename="{kind}_{snap["game"]["id"]}.csv"'})

def clock_worker():
    while True:
        time.sleep(.1)
        with LOCK:
            if tick(now()): persist()

with LOCK: persist()
threading.Thread(target=clock_worker,daemon=True).start()

if __name__=="__main__":
    print("\nCLICK admin token / מפתח ניהול:\n"+ADMIN_TOKEN)
    print("\nOpen http://localhost:"+os.environ.get("PORT","5000"))
    print("Phone API requires a public HTTPS address and the separate phone key.")
    print("Data and separate scores/answers files are in: "+str(DATA)+"\n")
    app.run(host=os.environ.get("HOST","127.0.0.1"),
            port=int(os.environ.get("PORT","5000")),threaded=True,
            debug=False,use_reloader=False)
