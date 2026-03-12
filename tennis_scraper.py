import os
import redis
import json
import time
import requests
from datetime import datetime
from playwright.sync_api import sync_playwright

# ----------------------------
# Redis setup
# ----------------------------
REDIS_URL = os.environ.get("REDIS_URL")
if not REDIS_URL:
    raise ValueError("REDIS_URL not set")

r = redis.from_url(REDIS_URL, decode_responses=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.sofascore.com/"
}

# ----------------------------
# SofaScore endpoints
# ----------------------------
STAT_API = "https://api.sofascore.com/api/v1/event/{match_id}/statistics"
POINT_API = "https://api.sofascore.com/api/v1/event/{match_id}/point-by-point"

# ----------------------------
# Helpers
# ----------------------------
def safe_int(v):
    try:
        return int(v)
    except:
        return None


def safe_score(score_obj):
    if not score_obj:
        return 0
    return score_obj.get("current") or score_obj.get("period1") or 0


# ----------------------------
# Fetch statistics API
# ----------------------------
def fetch_match_statistics(match_id):

    url = STAT_API.format(match_id=match_id)

    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        data = res.json()
    except:
        return {}

    stats = {}

    for block in data.get("statistics", []):

        if block.get("period") != "ALL":
            continue

        for group in block.get("groups", []):

            for item in group.get("statisticsItems", []):

                name = item.get("name")
                stats[name] = {
                    "home": item.get("home"),
                    "away": item.get("away")
                }

    return stats


# ----------------------------
# Fetch point-by-point fallback
# ----------------------------
def fetch_point_data(match_id):

    url = POINT_API.format(match_id=match_id)

    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()
    except:
        return []

    return data.get("points", [])


# ----------------------------
# Build stats from points
# ----------------------------
def build_stats_from_points(points):

    stats = {
        "home": {
            "aces": 0,
            "double_faults": 0,
            "serve_points_won": 0,
            "return_points_won": 0
        },
        "away": {
            "aces": 0,
            "double_faults": 0,
            "serve_points_won": 0,
            "return_points_won": 0
        }
    }

    for p in points:

        server = p.get("server")
        winner = p.get("winner")
        result = p.get("result")

        if result == "ace":
            stats[server]["aces"] += 1

        if result == "doubleFault":
            stats[server]["double_faults"] += 1

        if winner == server:
            stats[server]["serve_points_won"] += 1
        else:
            stats[winner]["return_points_won"] += 1

    return stats


# ----------------------------
# Extract stats for a player
# ----------------------------
def extract_player_stats(stat_api, point_stats, side):

    home = side == "home"

    def pick(name):

        if name in stat_api:
            return stat_api[name]["home"] if home else stat_api[name]["away"]

        if point_stats:
            return point_stats[side].get(name)

        return None

    return {
        "aces": safe_int(pick("Aces")),
        "double_faults": safe_int(pick("Double faults")),
        "first_serve_pct": safe_int(pick("First serve %")),
        "first_serve_points_won": safe_int(pick("1st serve points won")),
        "second_serve_points_won": safe_int(pick("2nd serve points won")),
        "break_points_converted": safe_int(pick("Break points converted")),
        "break_points_saved": safe_int(pick("Break points saved"))
    }


# ----------------------------
# Build player JSON
# ----------------------------
def build_player_json(event, stat_api, point_stats, side):

    home = side == "home"

    team = event["homeTeam"] if home else event["awayTeam"]
    opponent = event["awayTeam"] if home else event["homeTeam"]

    ts = event.get("startTimestamp")
    date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else "Unknown"

    tournament = event.get("tournament", {}).get("name", "Unknown")

    surface = event.get(
        "tournament", {}
    ).get(
        "uniqueTournament", {}
    ).get(
        "groundType", "Unknown"
    )

    home_games = safe_score(event.get("homeScore"))
    away_games = safe_score(event.get("awayScore"))

    games_won = home_games if home else away_games
    games_lost = away_games if home else home_games

    stat_values = extract_player_stats(stat_api, point_stats, side)

    match_stats = {
        "aces": stat_values["aces"],
        "double_faults": stat_values["double_faults"],
        "first_serve_pct": stat_values["first_serve_pct"],
        "first_serve_points_won": stat_values["first_serve_points_won"],
        "second_serve_points_won": stat_values["second_serve_points_won"],
        "break_points_converted": stat_values["break_points_converted"],
        "break_points_saved": stat_values["break_points_saved"],
        "service_games_won": games_won,
        "return_games_won": games_lost,
        "total_games_won": games_won,
        "total_games_played": home_games + away_games,
        "total_points_won": None
    }

    aces = match_stats["aces"] or 0
    breaks = match_stats["break_points_converted"] or 0
    games = match_stats["total_games_won"]

    fantasy_score = (aces * 0.5) + (breaks * 2) + (games * 0.2)

    return {
        "match_id": event.get("id"),
        "tournament": tournament,
        "surface": surface,
        "date": date,
        "player": team.get("name"),
        "opponent": opponent.get("name"),
        "match_stats": match_stats,
        "fantasy_metrics": {
            "fantasy_score": round(fantasy_score, 2),
            "props_related": {
                "total_games_won": games,
                "break_points_won": breaks,
                "aces": aces
            }
        },
        "result": "live"
    }


# ----------------------------
# Fetch player stats
# ----------------------------
def fetch_player_stats(match_id):

    raw = r.get(f"tennis:match:{match_id}")

    if not raw:
        return []

    data = json.loads(raw)

    event = data.get("event")

    if not event:
        return []

    stat_api = fetch_match_statistics(match_id)

    point_stats = None

    if not stat_api:
        points = fetch_point_data(match_id)
        point_stats = build_stats_from_points(points)

    home_player = build_player_json(event, stat_api, point_stats, "home")
    away_player = build_player_json(event, stat_api, point_stats, "away")

    status = event.get("status", {}).get("type")

    if status == "finished":

        if home_player["match_stats"]["total_games_won"] > away_player["match_stats"]["total_games_won"]:
            home_player["result"] = "win"
            away_player["result"] = "loss"
        else:
            home_player["result"] = "loss"
            away_player["result"] = "win"

    players = [home_player, away_player]

    r.set(f"tennis:match:{match_id}:players", json.dumps(players))

    return players


# ----------------------------
# Fetch live matches
# ----------------------------
def get_live_matches():

    raw = r.get("tennis:live")

    if not raw:
        return []

    return json.loads(raw)


# ----------------------------
# Collector loop
# ----------------------------
def collector_loop():

    with sync_playwright() as p:

        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        while True:

            print("\n----- TENNIS PLAYER STATS COLLECTOR -----")

            live_matches = get_live_matches()

            print("Live matches found:", len(live_matches))

            for match in live_matches:

                match_id = match.get("match_id")

                if not match_id:
                    continue

                print("Fetching stats for match", match_id)

                fetch_player_stats(match_id)

            print("Sleeping 60 seconds...\n")

            time.sleep(60)


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    collector_loop()