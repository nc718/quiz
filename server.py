import os
import json
import time
import random
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, UploadFile, File, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import PlainTextResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import uvicorn

app = FastAPI(title="ClickMaster Pro - Telephony & Web Clicker Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def serve_frontend():
    return HTMLResponse((Path(__file__).parent / "index.html").read_text(encoding="utf-8"))

DATA_DIR = Path("./game_data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "clickmaster.sqlite"

# Active WebSockets per game_code for real-time synchronization
ws_connections: Dict[str, list[WebSocket]] = {}

# In-memory high-precision active games state backed by JSON/SQLite files
ACTIVE_GAMES: Dict[str, Dict[str, Any]] = {}


def get_player_files(account_id: str, game_id: str):
    base = DATA_DIR / f"{account_id}_{game_id}"
    base.mkdir(exist_ok=True)
    return {
        "players": base / "players.json",         # קובץ שחקנים
        "scores": base / "scores.json",           # קובץ ניקוד (נקודות + שניות ואלפיות שנייה)
        "answered": base / "answered_log.json",   # קובץ נפרד לתיעוד מענה לשאלה הנוכחית למניעת כפילות
        "history": base / "games_history.json",   # היסטוריית משחקים קודמים
    }


def read_json(path: Path, default):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return default


def write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


async def broadcast_game_state(game_code: str, payload: dict):
    for ws in list(ws_connections.get(game_code, [])):
        try:
            await ws.send_json(payload)
        except Exception:
            ws_connections[game_code].remove(ws)


@app.api_route("/api/phone", methods=["GET", "POST"], response_class=PlainTextResponse)
async def telephony_ivr_endpoint(
    request: Request,
    ApiPhone: Optional[str] = Query(None),
    game_code: Optional[str] = Query(None),
    hangup: Optional[str] = Query(None),
    answer: Optional[str] = Query(None, alias="תשובה"),
):
    """
    נקודת קצה ראשית למערכות טלפוניות (IVR API) ולשלט ה-Web:
    - ApiPhone: מספר הטלפון / מזהה השחקן
    - game_code: קוד משחק בן 4 ספרות (אופציונלי אם השחקן קיים בקובץ השחקנים של משחק פעיל)
    - hangup=yes: מסיר מרשימת המחוברים בזמן אמת אך שומר על הניקוד
    - תשובה: מספר התשובה (1-4)
    """
    params = dict(request.query_params)
    phone = (ApiPhone or params.get("ApiPhone") or "").strip()
    code = (game_code or params.get("game_code") or "").strip()
    is_hangup = (hangup or params.get("hangup") or "").strip().lower() == "yes"
    ans_raw = answer if answer is not None else params.get("תשובה", params.get("answer"))

    if not phone:
        return "ERROR"

    # 1. איתור המשחק הפעיל לפי game_code או לפי קובץ השחקנים של חשבון עם מסך קהל פעיל
    target_game = None
    if code:
        target_game = ACTIVE_GAMES.get(code)
        if not target_game or not target_game.get("is_active"):
            return "UNDEFINED"
    else:
        for g in ACTIVE_GAMES.values():
            if not g.get("is_active"):
                continue
            files = get_player_files(g["account_id"], g["game_id"])
            players_reg = read_json(files["players"], {})
            if phone in g["connected_phones"] or phone in players_reg:
                target_game = g
                break
        if not target_game:
            return "UNDEFINED"

    files = get_player_files(target_game["account_id"], target_game["game_id"])
    players_reg = read_json(files["players"], {})
    scores_file = read_json(files["scores"], {})
    answered_file = read_json(files["answered"], {})

    # בדיקה אם השחקן קיים בקובץ השחקנים (אם לא - מוצג המספר עצמו מבלי להישמר קבוע בקובץ השחקנים)
    display_name = players_reg.get(phone, phone)

    # 2. טיפול בניתוק שיחה: hangup=yes
    if is_hangup:
        target_game["connected_phones"].pop(phone, None)
        await broadcast_game_state(target_game["game_code"], {"event": "PLAYER_HANGUP", "phone": phone})
        return "OK"

    # 3. בקשת התחברות (ללא פרמטר 'תשובה')
    if ans_raw is None or str(ans_raw).strip() == "":
        if target_game.get("locked") and phone not in target_game["connected_phones"]:
            return "ERROR"
        target_game["connected_phones"][phone] = {
            "phone": phone,
            "display_name": display_name,
            "connected_at": time.time(),
        }
        if phone not in scores_file:
            scores_file[phone] = {"points": 0, "total_correct_time_sec": 0.0, "display_name": display_name}
            write_json(files["scores"], scores_file)

        await broadcast_game_state(target_game["game_code"], {
            "event": "PLAYER_CONNECTED",
            "phone": phone,
            "display_name": display_name,
        })
        return "OK"

    # 4. קליטת תשובה בפרמטר 'תשובה' בזמן טיימר אלפיות שנייה פעיל
    if not target_game.get("timer_running") or target_game.get("stage") != "question":
        return "ERROR"

    current_q = target_game.get("current_question")
    if not current_q:
        return "ERROR"

    valid_options_count = len([a for a in current_q["answers"] if a and str(a).strip()])
    try:
        chosen_option = int(str(ans_raw).strip())
    except ValueError:
        return "UNDEFINED"

    # אם ענה מספר תשובה שאינה קיימת בשאלה הנוכחית -> מוחזר UNDEFINED וניתנת לו אפשרות לענות תשובה אחרת
    if chosen_option < 1 or chosen_option > valid_options_count:
        return "UNDEFINED"

    q_id = str(current_q["id"])
    answered_file.setdefault(q_id, {})

    # מניעת כפילות מענה על אותה שאלה בקובץ נפרד
    if phone in answered_file[q_id]:
        return "ERROR"

    elapsed_sec = round(max(0.001, time.perf_counter() - target_game["timer_started_perf"]), 3)
    correct_num = int(current_q.get("correct", 1))
    is_correct = (correct_num == 0) or (chosen_option == correct_num)
    q_points = int(current_q.get("points") or target_game["settings"].get("default_points", 100))

    # תיעוד המענה בקובץ הנפרד של השאלה הנוכחית
    answered_file[q_id][phone] = {
        "phone": phone,
        "display_name": display_name,
        "answer": chosen_option,
        "is_correct": is_correct,
        "elapsed_sec": elapsed_sec,
        "points": q_points if is_correct else 0,
    }
    write_json(files["answered"], answered_file)

    scores_file.setdefault(phone, {"points": 0, "total_correct_time_sec": 0.0, "display_name": display_name})

    # שידור בועה מרחפת בזמן אמת למסך הקהל
    await broadcast_game_state(target_game["game_code"], {
        "event": "PLAYER_ANSWERED",
        "phone": phone,
        "display_name": display_name,
    })

    if is_correct:
        scores_file[phone]["points"] += q_points
        scores_file[phone]["total_correct_time_sec"] = round(
            scores_file[phone]["total_correct_time_sec"] + elapsed_sec, 3
        )
        write_json(files["scores"], scores_file)
        return "בסדר"
    else:
        return "ERROR"


@app.post("/api/upload_excel/{account_id}/{game_id}")
async def upload_questions_from_excel(account_id: str, game_id: str, file: UploadFile = File(...)):
    """העלאת שאלות מקובץ אקסל (.xlsx / .csv)"""
    df = pd.read_excel(file.file) if file.filename.endswith((".xlsx", ".xls")) else pd.read_csv(file.file)
    questions = []
    for idx, row in df.iterrows():
        q_text = str(row.get("שאלה", row.iloc[0])).strip()
        answers = [str(row.iloc[i]).strip() for i in range(1, min(5, len(row))) if pd.notna(row.iloc[i]) and str(row.iloc[i]).strip()]
        correct = int(row.get("תשובה נכונה", 1)) if pd.notna(row.get("תשובה נכונה", 1)) else 1
        time_sec = int(row["זמן"]) if "זמן" in row and pd.notna(row["זמן"]) else None
        pts = int(row["ניקוד"]) if "ניקוד" in row and pd.notna(row["ניקוד"]) else None
        questions.append({
            "id": f"q_{int(time.time()*1000)}_{idx}",
            "text": q_text,
            "answers": answers,
            "correct": correct,
            "time": time_sec,
            "points": pts,
        })
    return JSONResponse({"status": "OK", "imported_count": len(questions), "questions": questions})


@app.websocket("/ws/{game_code}")
async def game_websocket(websocket: WebSocket, game_code: str):
    await websocket.accept()
    ws_connections.setdefault(game_code, []).append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_connections[game_code].remove(websocket)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
