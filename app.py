import os
import uuid
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, render_template, request, redirect, session, url_for, jsonify
from flask_socketio import SocketIO, emit
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY")

# Threading is intentional. The browser uses polling so the app does not
# depend on an eventlet/gevent WebSocket stack.
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

FORM_TABLES = [
    {"id": "tblcoCLWb3Fv4hS6v", "label": "Meet the Chapter", "match_field": "Rushee Name (First and Last)", "show_fields": ["Professionalism", "Ease of Conversation", "Other comments"], "category": "forms"},
    {"id": "tblxO2sn8koNjjrsi", "label": "Speed Networking", "match_field": "Rushee Name (First and Last)", "show_fields": ["Professionalism", "Ease of conversation", "Other comments"], "category": "forms"},
    {"id": "tbl06MCPYcd9XVIzu", "label": "Case Evaluation (Open)", "match_field": "Rushee Name (First and Last)", "show_fields": [
        "What portion of this event did you fill this out?",
        "Professionalism",
        "Ease of conversation",
        "How well do they work with others? (Only fill out if you are filling out during case prep portion)",
        "Speaking skills (Only fill out if you are in case presentations portion)",
        "Other comments",
    ], "category": "forms"},
    {"id": "tblTwyuNlNYpj2rk5", "label": "Interview", "match_field": "Rushee Name", "show_fields": ["Interviewer Name", "Tell Me About Yourself", "Behavioral", "What thinking question did you choose?", "Thinking Question", "On the Spot", "Notes", "Other Comments?"], "category": "forms"},
    {"id": "tbl618w7VFf9mWu71", "label": "Dinner Night", "match_field": "Rushee Name", "show_fields": ["Professionalism", "Ease of Conversation", "Other comments (Ex. Rushee talked over others and interrupted, Rushee was very arrogant)"], "category": "forms"},
    {"id": "tblsDYSH6pwJ5GmU4", "label": "Passion Pitch", "match_field": "Rushee Name (First and Last)", "show_fields": ["Professionalism", "How well did they present?", "How well did they listen to the other presenters?", "Did they seem to care about their passion?", "Other comments"], "category": "forms"},
    {"id": "tblgEBzTXxJUbCoXr", "label": "Case Evaluation (Closed)", "match_field": "Rushee Name (First and Last)", "show_fields": [
        "What portion of this event did you fill this out?",
        "Professionalism",
        "How well do they work with others? (Only fill out if you are filling out during case prep portion)",
        "Speaking skills (Only fill out if you are in case presentations portion)",
        "How well did they answer questions? (Only fill out if you are in case presentation portion)",
        "Other comments",
    ], "category": "forms"},
    {"id": "tblNLc33axauq0KaX", "label": "Conflict", "match_field": "PMN Name FIRST and LAST you have conflict with (You know prior to them rushing)", "show_fields": ["Active Name"], "category": "forms"},
    {"id": "tblkOoYy9BCSb5zon", "label": "Red Flag", "match_field": "PNM Full Name", "show_fields": ["Why are you filling out this red flag form?", "Active Name"], "category": "forms"},
    {"id": "tbl49XYiXic469hdk", "label": "Standout", "match_field": "PNM Full Name", "show_fields": ["Why are you filling out this standout form?", "Active Name"], "category": "forms"},
    {"id": "tblDQrFspmSsaEhMj", "label": "Event Attendance", "match_field": "Full Name (first and last)", "show_fields": ["Event Name", "Event Date", "Date"], "category": "attendance"},
]

state = {
    "applicants": [],
    "current_index": 0,
    "voting_open": False,
    "votes": {"yes": 0, "no": 0, "maybe": 0},
    "voted_this_round": set(),
}

profile_cache = {}


def normalize_space(value):
    return " ".join(str(value).strip().split()) if value is not None else ""


def normalize_name(value):
    """Normalize a full name to first + last, case-insensitive."""
    value = normalize_space(value)
    parts = value.split()
    if len(parts) < 2:
        return ""
    return f"{parts[0].casefold()} {parts[-1].casefold()}"


def normalize_first_name(value):
    value = normalize_space(value)
    if not value:
        return ""
    return value.split()[0].casefold()


def normalize_field_name(value):
    return " ".join(str(value).strip().casefold().split())


def extract_headshot_url(value):
    if not isinstance(value, list) or not value:
        return None
    first = value[0]
    if not isinstance(first, dict):
        return None
    return first.get("url")


def value_is_yes(value):
    if value is True:
        return True

    if isinstance(value, (int, float)):
        return value == 1

    if isinstance(value, str):
        return value.strip().casefold() in {
            "yes",
            "true",
            "1",
            "checked",
            "on",
        }

    return False

def is_closed(record):
    fields = record.get("fields", {})

    # Airtable may return the field with slightly different naming,
    # such as "Closed", "Closed?", "Is Closed", or "Closed Status".
    # Find a field whose name contains the word "closed".
    closed_values = []

    for field_name, field_value in fields.items():
        normalized = " ".join(
            str(field_name).strip().casefold().split()
        )

        if "closed" in normalized:
            closed_values.append((field_name, field_value))

    # Nothing that looks like a Closed field was returned.
    if not closed_values:
        return False

    # Any matching Closed field with a Yes/true value counts.
    for field_name, closed_value in closed_values:
        if value_is_yes(closed_value):
            return True

    return False


def find_actual_score_field(fields, preferred_name):
    """Return (actual_field_name, value) for a preferred score field."""
    normalized_fields = {
        normalize_field_name(name): (name, value)
        for name, value in fields.items()
    }

    # First use explicit aliases.
    aliases = SCORE_FIELD_ALIASES.get(preferred_name, [preferred_name])
    for alias in aliases:
        hit = normalized_fields.get(normalize_field_name(alias))
        if hit:
            return hit

    # Then tolerate small differences by looking for a very close key.
    target = normalize_field_name(preferred_name)
    target_words = set(target.replace("(", " ").replace(")", " ").split())

    best = None
    best_score = 0

    for normalized, original in normalized_fields.items():
        if "score" not in normalized and "attendance" not in normalized:
            continue

        words = set(normalized.replace("(", " ").replace(")", " ").split())
        score = len(target_words & words)

        if score > best_score:
            best_score = score
            best = original

    if best and best_score >= max(2, len(target_words) - 2):
        return best

    return None, None


def build_main_profile(record):
    fields = record.get("fields", {})
    name = fields.get("Full Name")
    key = normalize_name(name)

    if not name or not key:
        return None, None

    essays = []

    for field in ESSAY_FIELDS:
        value = fields.get(field)

        if value not in (None, ""):
            essays.append({
                "question": field,
                "answer": value,
            })

    # Airtable may omit blank attachment fields. Accept the exact Resume field
    # and, as a fallback, any field whose name contains "resume".
    resume_field = fields.get("Resume")
    if resume_field is None:
        for field_name, value in fields.items():
            if "resume" in normalize_field_name(field_name):
                resume_field = value
                break

    resume = []

    if isinstance(resume_field, list):
        for attachment in resume_field:
            if not isinstance(attachment, dict):
                continue

            url = attachment.get("url") or attachment.get("thumbnails", {}).get("large", {}).get("url")
            if not url:
                continue

            resume.append({
                "name": attachment.get("filename", "Resume"),
                "url": url,
            })
    elif isinstance(resume_field, str) and resume_field.strip():
        # Covers URL/formula-style Resume fields.
        resume.append({
            "name": "Resume",
            "url": resume_field.strip(),
        })
    elif isinstance(resume_field, dict):
        url = resume_field.get("url")
        if url:
            resume.append({
                "name": resume_field.get("filename", "Resume"),
                "url": url,
            })

    return key, {
        "name": name,
        "essays": essays,
        "resume": resume,
        "forms": [],
        "attendance": [],
    }

def build_profile_cache(closed_records):
    """
    Build profile data for Closed applicants.

    Matching rules for supporting forms:
      - Full name in form: exact first + last match.
      - First name only in form: attach only if exactly ONE Closed applicant
        has that first name. If multiple Closed applicants share the first name,
        the form is ignored to avoid assigning it to the wrong person.
    """
    global profile_cache

    cache = {}
    first_name_counts = {}

    for record in closed_records:
        key, profile = build_main_profile(record)
        if not key:
            continue
        cache[key] = profile

        first = normalize_first_name(profile["name"])
        if first:
            first_name_counts[first] = first_name_counts.get(first, 0) + 1


    def load_table(cfg):
        table = api.table(BASE_ID, cfg["id"])
        try:
            return cfg, table.all(), None
        except Exception as exc:
            return cfg, [], exc

    # Load supporting tables in parallel so startup/refresh is faster.
    print("===== RESUME DEBUG =====")
    for record in closed_records[:10]:
        fields = record.get("fields", {})
        resume_like = {
            k: v for k, v in fields.items()
            if "resume" in normalize_field_name(k)
        }
        print(f"NAME={fields.get('Full Name')!r} RESUME_FIELDS={resume_like!r}")
    print("========================")

    with ThreadPoolExecutor(max_workers=min(10, len(FORM_TABLES))) as executor:
        futures = [executor.submit(load_table, cfg) for cfg in FORM_TABLES]
        for future in as_completed(futures):
            cfg, rows, exc = future.result()

            if exc:
                print(f"Could not load {cfg['label']}: {exc}")
                continue

            for row in rows:
                fields = row.get("fields", {})
                match_value = fields.get(cfg["match_field"])

                raw_match = normalize_space(match_value)
                if not raw_match:
                    continue

                parts = raw_match.split()

                # Full first + last name -> exact match.
                if len(parts) >= 2:
                    key = normalize_name(raw_match)

                    if key not in cache:
                        continue

                    matched_key = key

                # First name only -> only use it if unique among Closed applicants.
                else:
                    first = normalize_first_name(raw_match)

                    if not first:
                        continue

                    if first_name_counts.get(first, 0) != 1:
                        # Ambiguous first name: deliberately ignore the form.
                        continue

                    matched_key = next(
                        (
                            applicant_key
                            for applicant_key, profile in cache.items()
                            if normalize_first_name(profile["name"]) == first
                        ),
                        None,
                    )

                    if not matched_key:
                        continue

                for field in cfg["show_fields"]:
                    value = fields.get(field)
                    if value in (None, ""):
                        continue

                    cache[matched_key][cfg["category"]].append({
                        "question": f"{cfg['label']} — {field}",
                        "answer": value,
                    })

    profile_cache = cache
    print(f"Profile cache ready for {len(profile_cache)} applicants.")


def load_applicants_and_profiles():
    print("Loading Fall '26 Applications...")

    records = applicants_table.all()
    print(f"Airtable returned {len(records)} application records.")

    closed_records = [record for record in records if is_closed(record)]

    closed_records.sort(
        key=lambda record: str(
            record.get("fields", {}).get("Full Name", "")
        ).casefold()
    )

    applicants = []

    for index, record in enumerate(closed_records):
        fields = record.get("fields", {})
        applicants.append({
            "index": index,
            "name": fields.get("Full Name", "Unknown"),
            "headshot": extract_headshot_url(fields.get("Headshot")),
        })

    state["applicants"] = applicants
    state["current_index"] = min(
        state["current_index"],
        len(applicants) - 1,
    ) if applicants else 0

    state["voting_open"] = False
    state["votes"] = {"yes": 0, "no": 0, "maybe": 0}
    state["voted_this_round"] = set()

    print(f"Loaded {len(applicants)} closed applicants.")
    build_profile_cache(closed_records)


def public_state():
    return {
        "applicants": state["applicants"],
        "current_index": state["current_index"],
        "voting_open": state["voting_open"],
        "votes": state["votes"],
    }


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
    profile = profile_cache.get(normalize_name(name))

    if not profile:
        return jsonify({
            "name": name,
            "essays": [],
            "resume": [],
            "forms": [],
            "attendance": [],
        })

    return jsonify(profile)


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

    try:
        index = int(data.get("index"))
    except (TypeError, ValueError):
        return

    if 0 <= index < len(state["applicants"]):
        state["current_index"] = index
        state["voting_open"] = False
        state["votes"] = {"yes": 0, "no": 0, "maybe": 0}
        state["voted_this_round"] = set()
        emit("state_update", public_state(), broadcast=True)


@socketio.on("host_start_vote")
def handle_start_vote():
    if not session.get("is_admin"):
        return
    if not state["applicants"]:
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
        emit("state_update", public_state(), broadcast=True)
        emit("applicants_refreshed", {"count": len(state["applicants"])})
    except Exception as exc:
        print(f"Refresh failed: {exc}")
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
    load_applicants_and_profiles()
    socketio.run(app, host="0.0.0.0", port=os.getenv("PORT"), debug=True)
