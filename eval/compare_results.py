"""
實驗結果比較工具

用法：
    # 比較兩個實驗
    python eval/compare_results.py desql_41mini_cn_validate pipeline_v4_financial

    # 只看一個實驗的統計
    python eval/compare_results.py pipeline_v4_financial

    # 指定 DB
    python eval/compare_results.py pipeline_v4_financial --db financial

    # 顯示翻正翻錯的詳細資訊
    python eval/compare_results.py desql_41mini_cn_validate pipeline_v4_financial --detail

    # 顯示錯題分析
    python eval/compare_results.py pipeline_v4_financial --errors
"""

import argparse
import json
import glob
import os
import sys
from pathlib import Path
from collections import Counter

EVAL_DIR = Path(__file__).parent
RESULTS_DIR = EVAL_DIR / "results"


def load_results(tag, db=None):
    """載入實驗結果"""
    pattern = f"{RESULTS_DIR}/{tag}/"
    if db:
        pattern += f"{db}_*.json"
    else:
        pattern += "*.json"
    
    results = {}
    for f in sorted(glob.glob(pattern)):
        d = json.load(open(f))
        key = (d["db_id"], d["question_id"])
        results[key] = d
    return results


def print_summary(tag, results):
    """印出單一實驗的統計"""
    total = len(results)
    correct = sum(1 for d in results.values() if d.get("judge_correct"))
    
    print(f"\n📊 {tag}: {correct}/{total} ({correct/total*100:.1f}%)")
    
    # 按 DB 分
    by_db = {}
    for (db, qid), d in results.items():
        by_db.setdefault(db, {"total": 0, "correct": 0})
        by_db[db]["total"] += 1
        if d.get("judge_correct"):
            by_db[db]["correct"] += 1
    
    for db in sorted(by_db.keys()):
        s = by_db[db]
        print(f"  {db}: {s['correct']}/{s['total']} ({s['correct']/s['total']*100:.1f}%)")
    
    # 按難度分
    by_diff = {}
    for d in results.values():
        diff = d.get("difficulty", "unknown")
        by_diff.setdefault(diff, {"total": 0, "correct": 0})
        by_diff[diff]["total"] += 1
        if d.get("judge_correct"):
            by_diff[diff]["correct"] += 1
    
    print()
    for diff in ["simple", "moderate", "challenging"]:
        if diff in by_diff:
            s = by_diff[diff]
            print(f"  {diff:12s}: {s['correct']}/{s['total']} ({s['correct']/s['total']*100:.1f}%)")


def print_comparison(tag_a, results_a, tag_b, results_b, detail=False):
    """比較兩個實驗"""
    common = set(results_a.keys()) & set(results_b.keys())
    
    a_correct = sum(1 for k in common if results_a[k].get("judge_correct"))
    b_correct = sum(1 for k in common if results_b[k].get("judge_correct"))
    
    flipped_good = []
    flipped_bad = []
    for k in sorted(common):
        ac = results_a[k].get("judge_correct", False)
        bc = results_b[k].get("judge_correct", False)
        if not ac and bc:
            flipped_good.append(k)
        elif ac and not bc:
            flipped_bad.append(k)
    
    print(f"\n📊 比較 ({len(common)} 題):")
    print(f"  A ({tag_a}): {a_correct}/{len(common)} ({a_correct/len(common)*100:.1f}%)")
    print(f"  B ({tag_b}): {b_correct}/{len(common)} ({b_correct/len(common)*100:.1f}%)")
    print(f"  翻正: {len(flipped_good)}")
    print(f"  翻錯: {len(flipped_bad)}")
    print(f"  淨: {len(flipped_good) - len(flipped_bad):+d}")
    
    # McNemar
    n = len(flipped_good) + len(flipped_bad)
    if n > 0:
        chi2 = (abs(len(flipped_good) - len(flipped_bad)) - 1) ** 2 / n
        sig = "顯著 (p < 0.05)" if chi2 >= 3.84 else "不顯著"
        print(f"  McNemar chi2={chi2:.3f} {sig}")
    
    if detail:
        if flipped_good:
            print(f"\n🟢 翻正 ({len(flipped_good)} 題):")
            for db, qid in flipped_good:
                d = results_b[(db, qid)]
                print(f"  #{qid} ({db}, {d.get('difficulty','')}): {d.get('question','')[:60]}")
        
        if flipped_bad:
            print(f"\n🔴 翻錯 ({len(flipped_bad)} 題):")
            for db, qid in flipped_bad:
                d = results_a[(db, qid)]
                print(f"  #{qid} ({db}, {d.get('difficulty','')}): {d.get('question','')[:60]}")


def print_errors(tag, results):
    """分析錯題"""
    wrong = {k: d for k, d in results.items() if not d.get("judge_correct")}
    
    print(f"\n❌ 錯題分析 ({len(wrong)}/{len(results)} 題):")
    
    # 分類
    crash = []
    empty = []
    wrong_val = []
    
    for k, d in wrong.items():
        ans = d.get("pipeline_answer", "")
        if "Pipeline 執行失敗" in ans:
            crash.append(k)
        elif "無法回答" in ans or "查無" in ans:
            empty.append(k)
        else:
            wrong_val.append(k)
    
    print(f"  Pipeline 崩潰: {len(crash)}")
    print(f"  查無/無法回答: {len(empty)}")
    print(f"  數值/邏輯錯誤: {len(wrong_val)}")
    
    # 列出崩潰和查無的題目
    if crash:
        print(f"\n  崩潰題目:")
        for db, qid in crash:
            d = wrong[(db, qid)]
            print(f"    #{qid} ({db}): {d.get('pipeline_answer','')[:60]}")
    
    if empty:
        print(f"\n  查無/無法回答:")
        for db, qid in empty:
            d = wrong[(db, qid)]
            print(f"    #{qid} ({db}, {d.get('difficulty','')}): {d.get('question','')[:50]}")


def main():
    parser = argparse.ArgumentParser(description="比較實驗結果")
    parser.add_argument("tags", nargs="+", help="實驗 tag（1 個看統計，2 個做比較）")
    parser.add_argument("--db", default=None, help="只看指定 DB")
    parser.add_argument("--detail", action="store_true", help="顯示翻正翻錯詳細")
    parser.add_argument("--errors", action="store_true", help="顯示錯題分析")
    args = parser.parse_args()
    
    if len(args.tags) == 1:
        results = load_results(args.tags[0], args.db)
        if not results:
            print(f"找不到 {args.tags[0]} 的結果")
            sys.exit(1)
        print_summary(args.tags[0], results)
        if args.errors:
            print_errors(args.tags[0], results)
    
    elif len(args.tags) == 2:
        results_a = load_results(args.tags[0], args.db)
        results_b = load_results(args.tags[1], args.db)
        if not results_a or not results_b:
            print(f"找不到結果")
            sys.exit(1)
        
        # 只顯示共同題目的統計
        common = set(results_a.keys()) & set(results_b.keys())
        common_a = {k: results_a[k] for k in common}
        common_b = {k: results_b[k] for k in common}
        
        print(f"\n(共同 {len(common)} 題)")
        print_summary(args.tags[0], common_a)
        print_summary(args.tags[1], common_b)
        print_comparison(args.tags[0], results_a, args.tags[1], results_b, args.detail)
        if args.errors:
            print_errors(args.tags[1], common_b)


if __name__ == "__main__":
    main()
