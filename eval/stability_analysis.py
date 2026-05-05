"""
穩定性分析：根據多次實驗結果，將題目分為「總是錯」「容易錯」「偶爾錯」「穩定對」

用法：
    python eval/stability_analysis.py --tags test_filterval,test_filterval2,test_filterval3,test_filterval4,test_filterval5
"""

import argparse
import json
import glob
from pathlib import Path
from collections import defaultdict

EVAL_DIR = Path(__file__).parent


def load_results(tag):
    """Load results from tag — tries exact dir first, then tag_* subdirs."""
    results = {}
    tag_dir = EVAL_DIR / "results" / tag
    if tag_dir.exists():
        for f in sorted(glob.glob(str(tag_dir / "*.json"))):
            with open(f) as fh:
                r = json.load(fh)
            results[r["question_id"]] = r
    else:
        # Try tag_schools, tag_financial, tag_debit pattern
        for sub in sorted(glob.glob(str(EVAL_DIR / "results" / f"{tag}_*"))):
            for f in sorted(glob.glob(str(Path(sub) / "*.json"))):
                with open(f) as fh:
                    r = json.load(fh)
                results[r["question_id"]] = r
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", required=True, help="逗號分隔的 tag 列表")
    args = parser.parse_args()

    tags = [t.strip() for t in args.tags.split(",")]
    n = len(tags)

    # 收集每題在每次實驗的對錯
    question_runs = defaultdict(
        lambda: {
            "correct": 0,
            "wrong": 0,
            "db_id": "",
            "difficulty": "",
            "question": "",
        }
    )

    for tag in tags:
        results = load_results(tag)
        for qid, r in results.items():
            entry = question_runs[qid]
            entry["db_id"] = r["db_id"]
            entry["difficulty"] = r["difficulty"]
            entry["question"] = r["question"][:80]
            if r.get("judge_correct"):
                entry["correct"] += 1
            else:
                entry["wrong"] += 1

    # 分類
    always_wrong = []  # 5/5 錯
    often_wrong = []  # 3-4/5 錯
    sometimes_wrong = []  # 1-2/5 錯
    always_right = []  # 0/5 錯

    for qid, info in sorted(question_runs.items()):
        wrong_count = info["wrong"]
        row = {
            "qid": qid,
            "db_id": info["db_id"],
            "difficulty": info["difficulty"],
            "question": info["question"],
            "wrong_count": wrong_count,
            "total": info["correct"] + info["wrong"],
            "error_rate": f"{wrong_count}/{info['correct'] + info['wrong']}",
        }
        if wrong_count == n:
            always_wrong.append(row)
        elif wrong_count >= 3:
            often_wrong.append(row)
        elif wrong_count >= 1:
            sometimes_wrong.append(row)
        else:
            always_right.append(row)

    # 統計
    total_q = len(question_runs)
    print(f"=== 穩定性分析 ({n} 次實驗: {', '.join(tags)}) ===\n")
    print(f"總題數: {total_q}")
    print(
        f"  總是對 (0/{n} 錯): {len(always_right)} 題 ({len(always_right)/total_q*100:.1f}%)"
    )
    print(
        f"  偶爾錯 (1-2/{n} 錯): {len(sometimes_wrong)} 題 ({len(sometimes_wrong)/total_q*100:.1f}%)"
    )
    print(
        f"  容易錯 (3-4/{n} 錯): {len(often_wrong)} 題 ({len(often_wrong)/total_q*100:.1f}%)"
    )
    print(
        f"  總是錯 ({n}/{n} 錯): {len(always_wrong)} 題 ({len(always_wrong)/total_q*100:.1f}%)"
    )

    # 按 DB 細分
    for category_name, category_list in [
        ("總是錯", always_wrong),
        ("容易錯", often_wrong),
        ("偶爾錯", sometimes_wrong),
    ]:
        print(f"\n--- {category_name} ({len(category_list)} 題) ---")
        db_groups = defaultdict(list)
        for row in category_list:
            db_groups[row["db_id"]].append(row)

        for db_id in sorted(db_groups):
            items = db_groups[db_id]
            diff_counts = defaultdict(int)
            for item in items:
                diff_counts[item["difficulty"]] += 1
            diff_str = ", ".join(f"{d}: {c}" for d, c in sorted(diff_counts.items()))
            print(f"\n  {db_id} ({len(items)} 題 — {diff_str}):")
            for row in sorted(items, key=lambda x: -x["wrong_count"]):
                print(
                    f"    #{row['qid']:5d} [{row['difficulty']:12s}] {row['error_rate']} | {row['question']}"
                )

    # 存 JSON
    output = {
        "tags": tags,
        "runs": n,
        "summary": {
            "always_right": len(always_right),
            "sometimes_wrong": len(sometimes_wrong),
            "often_wrong": len(often_wrong),
            "always_wrong": len(always_wrong),
        },
        "always_wrong": always_wrong,
        "often_wrong": often_wrong,
        "sometimes_wrong": sometimes_wrong,
    }
    out_path = EVAL_DIR / "report" / "stability_analysis.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n📄 JSON: {out_path}")


if __name__ == "__main__":
    main()
