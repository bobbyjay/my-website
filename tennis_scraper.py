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
    raise ValueError("REDIS_URL not set in environment")

r = redis.from_url(REDIS_URL, decode_responses=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.sofascore.com/"
}

# ----------------------------
# Fetch real SofaScore statistics
# ----------------------------
def fetch_match_statistics(match_id):

    url = f"https://api.sofascore.com/api/v1/event/{match_id}/statistics"

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
                home = item.get("home")
                away = item.get("away")

                stats[name] = {"home": home, "away": away}

    return stats


# ----------------------------
# Extract stats for each player
# ----------------------------
def extract_player_stats(stats, side):

    home = side == "home"

    def pick(stat):
        if stat not in stats:
            return None
        return stats[stat]["home"] if home else stats[stat]["away"]

    return {
        "aces": pick("Aces"),
        "double_faults": pick("Double faults"),
        "first_serve_pct": pick("First serve %"),
        "first_serve_points_won": pick("1st serve points won"),
        "second_serve_points_won": pick("2nd serve points won"),
        "break_points_converted": pick("Break points converted"),
        "break_points_saved": pick("Break points saved")
    }


# ----------------------------
# Score helper
# ----------------------------
def safe_get_score(score_obj):
    if not score_obj:
        return 0
    return score_obj.get("current") or score_obj.get("period1") or 0


# ----------------------------
# Build player JSON
# ----------------------------
def build_player_json(event, stats, side):

    home = side == "home"

    team = event["homeTeam"] if home else event["awayTeam"]
    opponent = event["awayTeam"] if home else event["homeTeam"]

    tournament = event.get("tournament", {}).get("name", "Unknown")
    surface = event.get("tournament", {}).get("uniqueTournament", {}).get("groundType", "Unknown")

    ts = event.get("startTimestamp")
    date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else "Unknown"

    home_games = safe_get_score(event.get("homeScore"))
    away_games = safe_get_score(event.get("awayScore"))

    games_won = home_games if home else away_games
    games_lost = away_games if home else home_games

    stat_values = extract_player_stats(stats, side)

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

    stats = fetch_match_statistics(match_id)

    home_player = build_player_json(event, stats, "home")
    away_player = build_player_json(event, stats, "away")

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

                print(f"Fetching LIVE stats for match {match_id}")

                fetch_player_stats(match_id)

            print("Sleeping 60 seconds...\n")

            time.sleep(60)


# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    collector_loop()