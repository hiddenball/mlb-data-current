#!/usr/bin/env python3
"""
mlb_historical_fetch.py

Downloads the "new" data identified as missing from the existing site ingest:
  - line score + full boxscore + play-by-play + win probability, per game
  - historical rosters (with position + jersey number), per team per season
  - draft results, per year
  - transactions, per year
  - standings splits (home/away, last-10, streak, run diff) — merged into
    a standings-splits.json alongside your existing standings.json

Range: 1980–2025 (edit START_YEAR / END_YEAR below).

This does NOT try to guess which years have complete data. It requests
everything for every year, saves whatever comes back, and writes a
coverage report (coverage_report.json + coverage_report.csv) showing,
per year and per field, how much data was actually non-empty. Read that
report after your first run to see the REAL cutoffs for your dataset —
don't trust anyone's guess about this, including mine.

Usage (WSL):
    python3 mlb_historical_fetch.py
    python3 mlb_historical_fetch.py --start 1980 --end 2025
    python3 mlb_historical_fetch.py --only-years 1985 1999 2010   # spot-check
    python3 mlb_historical_fetch.py --skip-existing               # resume-safe (default)
    python3 mlb_historical_fetch.py --games-per-year-limit 5      # quick smoke test
    python3 mlb_historical_fetch.py --live-poll --output-dir data # designed for a 5-min cron:
                                                                    # checks today's schedule,
                                                                    # force-refreshes only games
                                                                    # currently "Live", and also
                                                                    # pulls the full feed/live
                                                                    # payload for those games only
                                                                    # (see games/{gamePk}/feed.json
                                                                    # below) — never touches the
                                                                    # historical archive, so it
                                                                    # can't affect repo size there.

Output layout (matches your existing data/seasons/{YEAR}/ convention):

    data/
      seasons/{YEAR}/
        games/{gamePk}/linescore.json
        games/{gamePk}/boxscore.json
        games/{gamePk}/playbyplay.json
        games/{gamePk}/winprobability.json
        games/{gamePk}/feed.json        <- only written when include_feed=True
                                            (currently: --live-poll only). The full
                                            MLB "feed/live" payload: gameData (venue,
                                            weather, officials, probable pitchers,
                                            review/challenge info, broadcast info,
                                            no-hitter/perfect-game flags) + liveData
                                            (the same plays/boxscore/linescore, but
                                            in their richest raw form). This is the
                                            most complete single-game payload the
                                            MLB Stats API exposes. NOT fetched during
                                            historical backfill or daily catch-up —
                                            it's large (several hundred KB/game raw),
                                            and adding it to all ~46 years would undo
                                            the slim-schema size work already done.
        rosters/{teamId}.json
        transactions.json
        standings-splits.json
      draft/{YEAR}.json
    coverage_report.json
    coverage_report.csv
"""

import argparse
import csv
import json
import time
import sys
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

BASE = "https://statsapi.mlb.com/api/v1"
BASE_1_1 = "https://statsapi.mlb.com/api/v1.1"
SPORT_ID = 1  # MLB
REQUEST_DELAY = 0.6          # seconds between requests — be a good citizen
MAX_RETRIES = 4
RETRY_BACKOFF = 2.0

START_YEAR = 1980
END_YEAR = 2025

# Set from --output-dir in main(). Module-level so every helper function
# (called from multiple places) writes to the same place without having
# to thread an extra argument through every call.
DATA_DIR = Path("data")
# Set from the actual years being processed in this run, so 3 parallel
# terminals each writing to the same --output-dir don't clobber each
# other's coverage report.
COVERAGE_SUFFIX = ""


def fetch_json(url, retries=MAX_RETRIES):
    """GET a URL, retry on transient errors, return parsed JSON or None."""
    last_err = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "personal-site-ingest/1.0"})
            with urlopen(req, timeout=30) as resp:
                raw = resp.read()
            time.sleep(REQUEST_DELAY)
            if not raw:
                return None
            return json.loads(raw)
        except HTTPError as e:
            if e.code == 404:
                return None  # endpoint genuinely doesn't exist for this id — not an error
            last_err = e
        except URLError as e:
            last_err = e
        except json.JSONDecodeError as e:
            last_err = e
        wait = RETRY_BACKOFF ** attempt
        print(f"    retry {attempt+1}/{retries} for {url} ({last_err}) — waiting {wait:.0f}s", file=sys.stderr)
        time.sleep(wait)
    print(f"    GAVE UP on {url}: {last_err}", file=sys.stderr)
    return None


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def is_meaningfully_empty(data):
    """Heuristic: did this endpoint basically give us nothing useful?"""
    if data is None:
        return True
    if isinstance(data, dict):
        # common MLB API "nothing here" shapes
        if not data:
            return True
        if set(data.keys()) <= {"copyright"}:
            return True
    if isinstance(data, list) and not data:
        return True
    return False


class Coverage:
    """Tracks, per year and per field, how many items had data vs were empty."""
    def __init__(self):
        self.rows = {}  # (year, field) -> {"total": n, "nonempty": n}

    def record(self, year, field, had_data: bool):
        key = (year, field)
        row = self.rows.setdefault(key, {"total": 0, "nonempty": 0})
        row["total"] += 1
        if had_data:
            row["nonempty"] += 1

    def write(self):
        out = []
        for (year, field), row in sorted(self.rows.items()):
            pct = (row["nonempty"] / row["total"] * 100) if row["total"] else 0.0
            out.append({
                "year": year,
                "field": field,
                "total_checked": row["total"],
                "had_data": row["nonempty"],
                "pct_available": round(pct, 1),
            })
        json_path = DATA_DIR / f"coverage_report{COVERAGE_SUFFIX}.json"
        csv_path = DATA_DIR / f"coverage_report{COVERAGE_SUFFIX}.csv"
        save_json(json_path, out)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["year", "field", "total_checked", "had_data", "pct_available"])
            w.writeheader()
            w.writerows(out)
        print(f"\nCoverage report written: {json_path} / {csv_path} ({len(out)} rows)")


def get_season_game_pks(year, limit=None):
    """All regular + postseason gamePks for a season, via the schedule endpoint."""
    url = (f"{BASE}/schedule?sportId={SPORT_ID}&season={year}"
           f"&gameType=R,F,D,L,W,C,P,A"  # regular season + all postseason/exhibition rounds
           f"&fields=dates,date,games,gamePk,status,detailedState")
    data = fetch_json(url)
    pks = []
    if data:
        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                pks.append(game["gamePk"])
    if limit:
        pks = pks[:limit]
    return pks


def fetch_game_data(year, game_pk, skip_existing, coverage, include_feed=False):
    """
    include_feed: also fetch the full feed/live payload (the richest single
    endpoint the API has — see the module docstring). Defaults to False so
    every existing caller (historical backfill, --date, daily catch-up) is
    completely unaffected and keeps writing exactly the same files it always
    has. Only --live-poll passes True, and only for games it found Live right
    now, so the historical archive's size never changes because of this.
    """
    game_dir = DATA_DIR / "seasons" / str(year) / "games" / str(game_pk)

    targets = [
        ("linescore", f"{BASE}/game/{game_pk}/linescore", game_dir / "linescore.json"),
        ("boxscore", f"{BASE}/game/{game_pk}/boxscore", game_dir / "boxscore.json"),
        ("playbyplay", f"{BASE}/game/{game_pk}/playByPlay", game_dir / "playbyplay.json"),
        ("winprobability", f"{BASE}/game/{game_pk}/winProbability", game_dir / "winprobability.json"),
    ]
    if include_feed:
        targets.append(
            ("feed", f"{BASE_1_1}/game/{game_pk}/feed/live", game_dir / "feed.json")
        )
    for field, url, path in targets:
        if skip_existing and path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                coverage.record(year, field, not is_meaningfully_empty(existing))
            except Exception:
                pass
            continue
        data = fetch_json(url)
        empty = is_meaningfully_empty(data)
        coverage.record(year, field, not empty)
        save_json(path, data if data is not None else {})


def fetch_team_ids():
    url = f"{BASE}/teams?sportId={SPORT_ID}"
    data = fetch_json(url)
    if not data:
        return []
    return [t["id"] for t in data.get("teams", [])]


def fetch_rosters(year, team_ids, skip_existing, coverage):
    for team_id in team_ids:
        path = DATA_DIR / "seasons" / str(year) / "rosters" / f"{team_id}.json"
        if skip_existing and path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                coverage.record(year, "roster", not is_meaningfully_empty(existing))
            except Exception:
                pass
            continue
        url = f"{BASE}/teams/{team_id}/roster?rosterType=fullSeason&season={year}"
        data = fetch_json(url)
        empty = is_meaningfully_empty(data)
        coverage.record(year, "roster", not empty)
        save_json(path, data if data is not None else {})


def fetch_draft(year, skip_existing, coverage):
    path = DATA_DIR / "draft" / f"{year}.json"
    if skip_existing and path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            coverage.record(year, "draft", not is_meaningfully_empty(existing))
        except Exception:
            pass
        return
    url = f"{BASE}/draft/{year}"
    data = fetch_json(url)
    empty = is_meaningfully_empty(data)
    coverage.record(year, "draft", not empty)
    save_json(path, data if data is not None else {})


def fetch_transactions(year, skip_existing, coverage):
    path = DATA_DIR / "seasons" / str(year) / "transactions.json"
    if skip_existing and path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            coverage.record(year, "transactions", not is_meaningfully_empty(existing))
        except Exception:
            pass
        return
    url = f"{BASE}/transactions?startDate={year}-01-01&endDate={year}-12-31&sportId={SPORT_ID}"
    data = fetch_json(url)
    empty = is_meaningfully_empty(data)
    coverage.record(year, "transactions", not empty)
    save_json(path, data if data is not None else {})


def fetch_standings_splits(year, skip_existing, coverage):
    path = DATA_DIR / "seasons" / str(year) / "standings-splits.json"
    if skip_existing and path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
            coverage.record(year, "standings_splits", not is_meaningfully_empty(existing))
        except Exception:
            pass
        return
    url = (f"{BASE}/standings?leagueId=103,104&season={year}"
           f"&standingsTypes=regularSeason&hydrate=team")
    data = fetch_json(url)
    empty = is_meaningfully_empty(data)
    coverage.record(year, "standings_splits", not empty)
    save_json(path, data if data is not None else {})


def get_date_game_pks(date_str):
    """Just the games scheduled on one specific date (YYYY-MM-DD)."""
    url = (f"{BASE}/schedule?sportId={SPORT_ID}&date={date_str}"
           f"&fields=dates,date,games,gamePk,status,detailedState")
    data = fetch_json(url)
    pks = []
    if data:
        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                pks.append(game["gamePk"])
    return pks


def get_date_games_with_status(date_str):
    """
    Every game scheduled on this date, with its live/preview/final status.
    Used only by --live-poll to find which games are actually in progress
    right now, so a 5-minute cron only ever touches today's live games
    instead of re-fetching everything on the schedule.
    """
    url = (f"{BASE}/schedule?sportId={SPORT_ID}&date={date_str}"
           f"&fields=dates,date,games,gamePk,status,abstractGameState,detailedState")
    data = fetch_json(url)
    games = []
    if data:
        for date_entry in data.get("dates", []):
            for game in date_entry.get("games", []):
                status = game.get("status", {})
                games.append({
                    "gamePk": game["gamePk"],
                    "abstractGameState": status.get("abstractGameState"),
                    "detailedState": status.get("detailedState"),
                })
    return games


def poll_live_games(coverage):
    """
    --live-poll: meant to run on a short cron (e.g. every 5 minutes). Checks
    today's schedule and force-refreshes ONLY the games whose status is
    currently "Live" — including the full feed/live payload for those games.
    Does nothing (cheap, fast exit) when nothing is live right now. Never
    touches rosters/transactions/standings/draft or any other day's games,
    and never touches the historical archive.
    """
    today = time.strftime("%Y-%m-%d", time.gmtime())
    print(f"=== live poll: {today} (UTC) ===")
    games = get_date_games_with_status(today)
    live = [g for g in games if g["abstractGameState"] == "Live"]
    print(f"  {len(games)} game(s) on today's schedule, {len(live)} currently Live")
    year = int(today[:4])
    for g in live:
        print(f"  refreshing gamePk {g['gamePk']} ({g['detailedState']})...")
        fetch_game_data(year, g["gamePk"], skip_existing=False, coverage=coverage, include_feed=True)
    print(f"  live poll complete: {len(live)} game(s) refreshed.\n")


def fetch_single_date(date_str, coverage):
    """
    Incremental daily mode: fetch just this date's games, plus force-refresh
    the things that change day to day for that date's year (standings,
    transactions, roster, draft). Games are always force-refreshed too
    (not skip-existing) since a game fetched the same day it's played may
    not have been final yet.
    """
    year = int(date_str[:4])
    print(f"=== {date_str} (year {year}) ===")

    print("  draft (cheap, always refreshed)...")
    fetch_draft(year, skip_existing=False, coverage=coverage)

    print("  transactions (always refreshed)...")
    fetch_transactions(year, skip_existing=False, coverage=coverage)

    print("  standings splits (always refreshed)...")
    fetch_standings_splits(year, skip_existing=False, coverage=coverage)

    print("  rosters (always refreshed)...")
    team_ids = fetch_team_ids()
    fetch_rosters(year, team_ids, skip_existing=False, coverage=coverage)

    game_pks = get_date_game_pks(date_str)
    print(f"  {len(game_pks)} games scheduled on {date_str}...")
    for pk in game_pks:
        # Always refresh (not skip-existing): a same-day game may not be final yet.
        fetch_game_data(year, pk, skip_existing=False, coverage=coverage)

    print(f"  {date_str} complete.\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=int, default=START_YEAR)
    parser.add_argument("--end", type=int, default=END_YEAR)
    parser.add_argument("--only-years", type=int, nargs="*", default=None,
                         help="Only process these specific years (overrides --start/--end)")
    parser.add_argument("--date", type=str, default=None,
                         help="YYYY-MM-DD: incremental daily mode — only this date's games, "
                              "plus force-refreshed standings/transactions/roster/draft for "
                              "its year. Overrides --start/--end/--only-years.")
    parser.add_argument("--live-poll", action="store_true", default=False,
                         help="Check today's schedule, force-refresh only games currently "
                              "Live (including the full feed/live payload for those games). "
                              "Designed to run on a short cron, e.g. every 5 minutes. "
                              "Overrides --date/--start/--end/--only-years.")
    parser.add_argument("--games-per-year-limit", type=int, default=None,
                         help="Cap games processed per year — useful for a quick smoke test")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                         help="Skip files already on disk (default: on)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false",
                         help="Re-download even if the file already exists")
    parser.add_argument("--output-dir", type=str, default="data",
                         help="Where to write data/ — e.g. an absolute path so multiple "
                              "terminals can all point at the same shared folder")
    args = parser.parse_args()

    global DATA_DIR, COVERAGE_SUFFIX
    DATA_DIR = Path(args.output_dir)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.live_poll:
        COVERAGE_SUFFIX = "_live"
        coverage = Coverage()
        print(f"Output directory: {DATA_DIR.resolve()}")
        poll_live_games(coverage)
        coverage.write()
        print("Live poll processed.")
        return

    if args.date:
        COVERAGE_SUFFIX = f"_{args.date}"
        coverage = Coverage()
        print(f"Output directory: {DATA_DIR.resolve()}")
        fetch_single_date(args.date, coverage)
        coverage.write()
        print("Date processed.")
        return

    years = args.only_years if args.only_years else list(range(args.start, args.end + 1))
    COVERAGE_SUFFIX = f"_{min(years)}-{max(years)}"  # keeps parallel runs from clobbering each other
    coverage = Coverage()

    print(f"Output directory: {DATA_DIR.resolve()}")
    print(f"Years this run: {min(years)}–{max(years)} ({len(years)} years)\n")

    print("Fetching team list (shared across all years)...")
    team_ids = fetch_team_ids()
    print(f"  {len(team_ids)} teams found.\n")

    for year in years:
        print(f"=== {year} ===")

        print("  draft...")
        fetch_draft(year, args.skip_existing, coverage)

        print("  transactions...")
        fetch_transactions(year, args.skip_existing, coverage)

        print("  standings splits...")
        fetch_standings_splits(year, args.skip_existing, coverage)

        print(f"  rosters ({len(team_ids)} teams)...")
        fetch_rosters(year, team_ids, args.skip_existing, coverage)

        print("  schedule...")
        game_pks = get_season_game_pks(year, limit=args.games_per_year_limit)
        print(f"  {len(game_pks)} games to process...")
        for i, pk in enumerate(game_pks, 1):
            fetch_game_data(year, pk, args.skip_existing, coverage)
            if i % 50 == 0:
                print(f"    ...{i}/{len(game_pks)} games done")

        print(f"  {year} complete.\n")
        # Write the coverage report incrementally so a long run can be
        # interrupted and you still get partial results.
        coverage.write()

    print("All years processed.")
    coverage.write()


if __name__ == "__main__":
    main()
