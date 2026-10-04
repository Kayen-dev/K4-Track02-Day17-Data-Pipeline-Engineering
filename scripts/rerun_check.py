"""The Lab 17 grading test (slide "Chạy lại & Backfill an toàn"):

    "Chạy lại một ngày cũ ba lần liên tiếp, ghi checksum bảng Gold sau mỗi lần.
     Ba con số phải giống hệt nhau."

    python -m scripts.rerun_check                  # re-runs config.RERUN_DAY (2026-08-12)
    python -m scripts.rerun_check --day 2026-08-14

1. fresh build from Bronze (reset Silver/Gold, backfill every day)  -> checksum C0
2. re-run the old day three times                                   -> C1, C2, C3
PASS iff C0 == C1 == C2 == C3. Result is written to submission/checksums.txt.
"""
from __future__ import annotations

import argparse
import sys

from pipeline import config
from pipeline.checksum import gold_checksums
from pipeline.run import connect, run_day
from main import fresh_build


def rerun_check(day: str, write: bool = True, quiet: bool = False) -> dict:
    base = fresh_build(quiet=True)
    runs = [("C0 fresh build", base)]
    con = connect()
    try:
        for i in range(1, 4):
            try:
                run_day(con, day)
                runs.append((f"C{i} re-run {day}", gold_checksums(con)))
            except Exception as exc:          # e.g. SnapshotImmutableError
                runs.append((f"C{i} re-run {day}", {"error": f"{type(exc).__name__}: {exc}"}))
    finally:
        con.close()

    ok = all("error" not in cs and cs["gold"] == base["gold"] for _, cs in runs)
    lines = [f"# Lab 17 — re-run check for {day}", ""]
    header = f"{'run':<24}" + "".join(f"{t:<22}" for t in config.GOLD_TABLES) + "gold (combined)"
    lines.append(header)
    for label, cs in runs:
        if "error" in cs:
            lines.append(f"{label:<24}ERROR  {cs['error']}")
        else:
            lines.append(f"{label:<24}" + "".join(f"{cs[t][:12]:<22}" for t in config.GOLD_TABLES)
                         + cs["gold"])
    lines += ["", "RESULT: " + ("PASS — C0 = C1 = C2 = C3" if ok
                                else "FAIL — C0, C1, C2, C3 are not all equal")]
    text = "\n".join(lines)
    if not quiet:
        print(text)
    if write:
        config.SUBMISSION_DIR.mkdir(exist_ok=True)
        (config.SUBMISSION_DIR / "checksums.txt").write_text(text + "\n", encoding="utf-8")
    return {"ok": ok, "runs": runs}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=config.RERUN_DAY)
    args = ap.parse_args()
    sys.exit(0 if rerun_check(args.day)["ok"] else 1)
