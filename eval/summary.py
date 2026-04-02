"""
評測結果統計

用法：
    python eval/summary.py                                    # 列出所有結果
    python eval/summary.py with_evidence                      # 指定資料夾
    python eval/summary.py with_evidence no_evidence          # 比較兩組
    python eval/summary.py with_evidence --db california_schools  # 指定 DB
"""

import argparse
import json
import glob
import sys
from pathlib import Path
from collections import Counter

EVAL_DIR = Path(__file__).parent
RESULTS_DIR = EVAL_DIR / "results"


def load_results(tag: str, db_filter: str = None) -> list:
    pattern = RESULTS_DIR / tag / "*.json"
    results = []
    for f in sorted(glob.glob(str(pattern))):
        with open(f) as fh:
            d = json.load(fh)
        if db_filter and d["db_id"] != db_filter:
            continue
        results.append(d)
    return results


def print_stats(tag: str, results: list):
    total = len(results)
    if total == 0:
        print(f"  ⚠️ {tag}: 無資料")
        return

    correct = sum(1 for r in results if r.get("judge_correct"))
    pct = correct / total * 100

    print(f"📊 [{tag}] {correct}/{total} ({pct:.1f}%)")

    by_diff = {}
    for r in results:
        diff = r["difficulty"]
        if diff not in by_diff:
            by_diff[diff] = {"total": 0, "correct": 0}
        by_diff[diff]["total"] += 1
        if r.get("judge_correct"):
            by_diff[diff]["correct"] += 1

    for diff in ["simple", "moderate", "challenging"]:
        if diff in by_diff:
            d = by_diff[diff]
            dp = d["correct"] / d["total"] * 100 if d["total"] > 0 else 0
            print(f"  {diff:12s}: {d['correct']}/{d['total']} ({dp:.1f}%)")
    print()
    return {"tag": tag, "total": total, "correct": correct, "pct": pct, "by_diff": by_diff}


def print_comparison(stats_list: list):
    if len(stats_list) < 2:
        return
    print("📈 比較：")
    header = f"  {'':12s}"
    for s in stats_list:
        header += f" {s['tag']:>18s}"
    print(header)

    row = f"  {'overall':12s}"
    for s in stats_list:
        row += f" {s['correct']}/{s['total']} ({s['pct']:.1f}%){'':>2s}"
    print(row)

    for diff in ["simple", "moderate", "challenging"]:
        row = f"  {diff:12s}"
        for s in stats_list:
            d = s["by_diff"].get(diff, {"correct": 0, "total": 0})
            dp = d["correct"] / d["total"] * 100 if d["total"] > 0 else 0
            row += f" {d['correct']}/{d['total']} ({dp:.1f}%){'':>2s}"
        print(row)
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tags", nargs="*", help="結果資料夾名稱（如 with_evidence no_evidence）")
    parser.add_argument("--db", default=None, help="只看指定 DB")
    args = parser.parse_args()

    # 自動偵測所有結果資料夾
    if not args.tags:
        args.tags = sorted([
            d.name for d in RESULTS_DIR.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])

    if not args.tags:
        print("❌ eval/results/ 下沒有結果資料夾")
        sys.exit(1)

    stats_list = []
    for tag in args.tags:
        results = load_results(tag, args.db)
        s = print_stats(tag, results)
        if s:
            stats_list.append(s)

    if len(stats_list) >= 2:
        print_comparison(stats_list)


if __name__ == "__main__":
    main()
