import os
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from flask import Flask, render_template, request, redirect, session, url_for, jsonify
from flask_socketio import SocketIO, emit
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

api = Api(os.getenv("AIRTABLE_TOKEN"))
BASE_ID = "appT97E7YHDPXi6IW"

APPLICANTS_TABLE_ID = "tblktZGcswxO2Ksib"  # Fall '26 Applications
applicants_table = api.table(BASE_ID, APPLICANTS_TABLE_ID)

ESSAY_FIELDS = [
    "Why KTP?",
    "Who were three actives you spoke to during rush, and what did you talk about?",
    "If you were a member of KTP and you were stuck in the airport with your fellow brothers for a couple hours, what would you talk about?",
    "What actives did you already know before rushing? (This doesn't affect your application, we are just trying to gather data on how our members recruited this semester. Thanks!)",
]

SCORE_FIELDS = [
    "Application Score",
    "Cumulative Score (Calculated)",
    "Meet the Chapter Total Score",
    "Speed Networking Total Score",
    "Case Evaluation Total Score",
    "Total Attendance",
]

FORM_TABLES = [
    {"id": "tblcoCLWb3Fv4hS6v", "label": "Meet the Chapter", "match_field": "Rushee Name (First and Last)",
     "show_fields": ["Professionalism", "Ease of Conversation", "Other comments"], "category": "forms"},
    {"id": "tblxO2sn8koNjjrsi", "label": "Speed Networking", "match_field": "Rushee Name (First and Last)",
     "show_fields": ["Professionalism", "Ease of conversation", "Other comments"], "category": "forms"},
    {"id": "tbl06MCPYcd9XVIzu", "label": "Case Evaluation (Open)", "match_field": "Rushee Name (First and Last)",
     "show_fields": ["What portion of this event did you fill this out?", "Professionalism",
                     "Ease of conversation",
                     "How well do they work with others? (Only fill out if you are filling out during case prep portion)",
                     "Speaking skills (Only fill out if you are in case presentations portion)", "Other comments"], "category": "forms"},
    {"id": "tblTwyuNlNYpj2rk5", "label": "Interview", "match_field": "Rushee Name",
     "show_fields": ["Interviewer Name", "Tell Me About Yourself", "Behavioral",
                     "What thinking question did you choose?", "Thinking Question", "On the Spot", "Notes",
                     "Other Comments?"], "category": "forms"},
    {"id": "tbl618w7VFf9mWu71", "label": "Dinner Night", "match_field": "Rushee Name",
     "show_fields": ["Professionalism", "Ease of Conversation",
                     "Other comments (Ex. Rushee talked over others and interrupted, Rushee was very arrogant)"],
     "category": "forms"},
    {"id": "tblsDYSH6pwJ5GmU4", "label": "Passion Pitch", "match_field": "Rushee Name (First and Last)",
     "show_fields": ["Professionalism", "How well did they present?",
                     "How well did they listen to the other presenters?",
                     "Did they seem to care about their passion?", "Other comments"], "category": "forms"},
    {"id": "tblgEBzTXxJUbCoXr", "label": "Case Evaluation (Closed)", "match_field": "Rushee Name (First and Last)",
     "show_fields": ["What portion of this event did you fill this out?", "Professionalism",
                     "How well do they work with others? (Only fill out if you are filling out during case prep portion)",
                     "Speaking skills (Only fill out if you are in case presentations portion)",
                     "How well did they answer questions? (Only fill out if you are in case presentation portion)",
                     "Other comments"], "category": "forms"},
    {"id": "tblNLc33axauq0KaX", "label": "Conflict",
     "match_field": "PMN Name FIRST and LAST you have conflict with (You know prior to them rushing)",
     "show_fields": ["Active Name"], "category": "forms"},
    {"id": "tblkOoYy9BCSb5zon", "label": "Red Flag", "match_field": "PNM Full Name",
     "show_fields": ["Why are you filling out this red flag form?", "Active Name"], "category": "forms"},
    {"id": "tbl49XYiXic469hdk", "label": "Standout", "match_field": "PNM Full Name",
     "show_fields": ["Why are you filling out this standout form?", "Active Name"], "category": "forms"},
    {"id": "tblDQrFspmSsaEhMj", "label": "Event Attendance", "match_field": "Full Name (first and last)",
     "show_fields": ["Event Name", "Event Date", "Date"], "category": "attendance"},
]

# ---------- Shared live state ----------
state = {
    "applicants": [],
    "current_index": 0,
    "voting_open": False,
    "votes": {"yes": 0, "no": 0, "maybe": 0},
    "voted_this_round": set(),
}

# Cached profile data. Airtable is loaded when the app starts and when the
# host presses Refresh List, so profile buttons do not make repeated requests.
profile_cache = {}
profile_cache_lock = threading.Lock()
profile_cache_ready = threading.Event()


def normalize_name(value):
    """Normalize first + last name, case-insensitive."""
    if not value:
        return ""
    parts = str(value).strip().split()
    if len(parts) < 2:
        return ""
    return f"{parts[0].casefold()} {parts[-1].casefold()}"


def value_means_closed(value):
    """Handle common Airtable Yes/No representations."""
    if value is True:
        return True
    if isinstance(value, (int, float)):
        return value == 1
    if isinstance(value, str):
        return value.strip().casefold() in {"yes", "true", "1", "checked", "on"}
    return False


def is_closed_rush_record(record):
    """Closed is a field in Fall '26 Applications."""
    fields = record.get("fields", {})

    if "Closed" in fields:
        return value_means_closed(fields.get("Closed"))

    for field_name, value in fields.items():
        if "closed" in str(field_name).strip().casefold():
            if value_means_closed(value):
                return True

    return False


def extract_headshot_url(value):
    if not isinstance(value, list) or not value:
        return None
    first = value[0]
    return first.get("url") if isinstance(first, dict) else None


def build_base_profiles(records):
    cache = {}
    for record in records:
        fields = record.get("fields", {})
        name = fields.get("Full Name")
        key = normalize_name(name)
        if not name or not key:
            continue

        essays = []
        scores = []
        for field in ESSAY_FIELDS:
            value = fields.get(field)
            if value:
                essays.append({"question": field, "answer": value})
        for field in SCORE_FIELDS:
            value = fields.get(field)
            if value not in (None, ""):
                scores.append({"question": field, "answer": str(value)})

        cache[key] = {
            "name": name,
            "essays": essays,
            "scores": scores,
            "forms": [],
            "attendance": [],
        }
    return cache


def fetch_form_table(cfg):
    """Fetch a supporting table with its own Api client."""
    try:
        table = Api(os.getenv("AIRTABLE_TOKEN")).table(BASE_ID, cfg["id"])
        return cfg, table.all()
    except Exception as exc:
        print(f"Could not load {cfg['label']}: {exc}")
        return cfg, []


def build_profile_cache_async(closed_records):
    """Load the supporting tables in parallel after the app is available."""
    global profile_cache

    cache = build_base_profiles(closed_records)
    with profile_cache_lock:
        profile_cache = cache

    with ThreadPoolExecutor(max_workers=min(10, len(FORM_TABLES))) as executor:
        futures = [executor.submit(fetch_form_table, cfg) for cfg in FORM_TABLES]

        for future in as_completed(futures):
            cfg, rows = future.result()
            for row in rows:
                fields = row.get("fields", {})
                key = normalize_name(fields.get(cfg["match_field"]))
                if not key or key not in cache:
                    continue

                for field in cfg["show_fields"]:
                    value = fields.get(field)
                    if value in (None, ""):
                        continue
                    cache[key][cfg["category"]].append({
                        "question": f"{cfg['label']} — {field}",
                        "answer": value,
                    })

    with profile_cache_lock:
        profile_cache = cache
        profile_cache_ready.set()

    print(f"Profile cache ready for {len(cache)} applicants.")


def load_applicants_and_profiles(background=True):
    """Load the Closed applicant list immediately and profiles in background."""
    global profile_cache

    records = applicants_table.all()
    closed_records = [record for record in records if is_closed_rush_record(record)]
    closed_records.sort(
        key=lambda r: r.get("fields", {}).get("Full Name", "").casefold()
    )

    state["applicants"] = []
    for i, record in enumerate(closed_records):
        fields = record.get("fields", {})
        state["applicants"].append({
            "index": i,
            "name": fields.get("Full Name", "Unknown"),
            "headshot": extract_headshot_url(fields.get("Headshot")),
        })

    state["current_index"] = min(
        state["current_index"],
        len(state["applicants"]) - 1,
    ) if state["applicants"] else 0

    profile_cache_ready.clear()
    with profile_cache_lock:
        profile_cache = build_base_profiles(closed_records)

    print(f"Loaded {len(state['applicants'])} closed applicants.")

    if background and closed_records:
        threading.Thread(
            target=build_profile_cache_async,
            args=(closed_records,),
            daemon=True,
        ).start()

    return len(state["applicants"])


def public_state():
    return {
        "applicants": state["applicants"],
        "current_index": state["current_index"],
        "voting_open": state["voting_open"],
        "votes": state["votes"],
    }


# Only load the main applicant table before serving traffic.
load_applicants_and_profiles(background=True)


# ---------- Auth ----------
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("is_admin"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == os.getenv("ADMIN_PASSWORD"):
            session["is_admin"] = True
            return redirect(request.args.get("next") or url_for("panel"))
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.pop("is_admin", None)
    return redirect(url_for("login"))


# ---------- Pages ----------
@app.route("/")
def home():
    if "voter_id" not in session:
        session["voter_id"] = str(uuid.uuid4())
    return render_template("index.html")


@app.route("/panel")
@login_required
def panel():
    return render_template("panel.html")


# ---------- API ----------
@app.route("/api/applicant/<path:name>/profile")
def applicant_profile(name):
    key = normalize_name(name)
    with profile_cache_lock:
        profile = profile_cache.get(key)

    if profile:
        return jsonify(profile)

    return jsonify({
        "name": name,
        "essays": [],
        "scores": [],
        "forms": [],
        "attendance": [],
        "loading": not profile_cache_ready.is_set(),
    })


# ---------- Socket events ----------
@socketio.on("connect")
def handle_connect():
    emit("state_update", public_state())
    if session.get("voter_id") in state["voted_this_round"]:
        emit("already_voted")


@socketio.on("host_go_to_applicant")
def handle_go_to_applicant(data):
    if not session.get("is_admin"):
        return
    index = data.get("index")
    if isinstance(index, int) and 0 <= index < len(state["applicants"]):
        state["current_index"] = index
        state["voting_open"] = False
        state["votes"] = {"yes": 0, "no": 0, "maybe": 0}
        state["voted_this_round"] = set()
        emit("state_update", public_state(), broadcast=True)


@socketio.on("host_start_vote")
def handle_start_vote():
    if not session.get("is_admin"):
        return
    state["voting_open"] = True
    state["votes"] = {"yes": 0, "no": 0, "maybe": 0}
    state["voted_this_round"] = set()
    emit("state_update", public_state(), broadcast=True)


@socketio.on("host_end_vote")
def handle_end_vote():
    if not session.get("is_admin"):
        return
    state["voting_open"] = False
    emit("state_update", public_state(), broadcast=True)


@socketio.on("host_refresh_applicants")
def handle_refresh_applicants():
    if not session.get("is_admin"):
        return

    try:
        load_applicants_and_profiles()
        state["voting_open"] = False
        state["votes"] = {"yes": 0, "no": 0, "maybe": 0}
        state["voted_this_round"] = set()
        emit("state_update", public_state(), broadcast=True)
        emit("applicants_refreshed")
    except Exception as exc:
        emit("applicants_refresh_error", {"message": str(exc)})


@socketio.on("submit_vote")
def handle_submit_vote(data):
    if not state["voting_open"]:
        return
    voter_id = session.get("voter_id")
    if not voter_id or voter_id in state["voted_this_round"]:
        emit("already_voted")
        return
    choice = data.get("choice")
    if choice not in state["votes"]:
        return
    state["votes"][choice] += 1
    state["voted_this_round"].add(voter_id)
    emit("vote_recorded")
    emit("state_update", public_state(), broadcast=True)


if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5001, debug=True)
