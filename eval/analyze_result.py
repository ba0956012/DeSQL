"""
分析 validate_sql_result 實驗結果
用法: python eval/analyze_result.py --tag test_programmer --baseline test_bugfix
"""
import json
import os
import argparse
from pathlib import Path

EVAL_DIR = Path(__file__).parent
RESULTS_DIR = EVAL_DIR / "results"
DB_MAP = {
    "schools": "california_schools",
    "financial": "financial",
    "debit": "debit_card_specializing",
}


def load_results(tag_prefix: str) -> dict:
    """載入 {tag_prefix}_{db} 的所有結果，回傳 {question_id: record}"""
    all_results = {}
    for short, db_id in DB_MAP.items():
        d = RESULTS_DIR / f"{tag_prefix}_{short}"
        if not d.exists():
            continue
        for f in d.iterdir():
            if f.suffix == ".json":
                r = json.loads(f.read_text(encoding="utf-8"))
                all_results[r["question_id"]] = r
    return all_results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="test_programmer", help="實驗 tag prefix")
    parser.add_argument("--baseline", default="test_bugfix", help="baseline tag prefix")
    args = parser.parse_args()

    # === 1. 分 DB 統計 ===
    print(f"=== {args.tag} 結果 ===\n")
    db_scores = {}
    for short, db_id in DB_MAP.items():
        d = RESULTS_DIR / f"{args.tag}_{short}"
        if not d.exists():
            print(f"  {short}: (目錄不存在)")
            continue
        total = correct = 0
        for f in d.iterdir():
            if f.suffix == ".json":
                r = json.loads(f.read_text(encoding="utf-8"))
                total += 1
                if r.get("judge_correct"):
                    correct += 1
        db_scores[short] = (correct, total)
        print(f"  {short}: {correct}/{total} ({correct/total*100:.1f}%)")

    grand_c = sum(v[0] for v in db_scores.values())
    grand_t = sum(v[1] for v in db_scores.values())
    if grand_t:
        print(f"  Total: {grand_c}/{grand_t} ({grand_c/grand_t*100:.1f}%)")

    # === 2. Validator 觸發分析 ===
    exp = load_results(args.tag)
    triggered = [r for r in exp.values() if r.get("sql_validation") or r.get("sql_validation_count", 0) > 0]
    print(f"\n=== Validator 觸發分析 ===")
    print(f"  總觸發: {len(triggered)}/{len(exp)}")

    if triggered:
        tp = [r for r in triggered if r.get("judge_correct")]
        fp = [r for r in triggered if not r.get("judge_correct")]
        print(f"  觸發後答對 (retry 有效): {len(tp)}")
        print(f"  觸發後答錯 (retry 無效或有害): {len(fp)}")

        print(f"\n  觸發的題目:")
        for r in sorted(triggered, key=lambda x: x["question_id"]):
            icon = "✅" if r.get("judge_correct") else "❌"
            val = r.get("sql_validation", "") or "(cleared after retry)"
            cnt = r.get("sql_validation_count", 0)
            cnt_str = f" x{cnt}" if cnt > 1 else ""
            print(f"    {icon} #{r['question_id']} ({r['difficulty']}){cnt_str} — {val[:80]}")

    # === 3. 跟 baseline 比較 ===
    base = load_results(args.baseline)
    if not base:
        print(f"\n(baseline {args.baseline} 不存在，跳過比較)")
        return

    common = set(exp) & set(base)
    improved = []  # baseline 錯 → 實驗對
    regressed = []  # baseline 對 → 實驗錯

    for qid in common:
        b_correct = base[qid].get("judge_correct", False)
        e_correct = exp[qid].get("judge_correct", False)
        if not b_correct and e_correct:
            improved.append(qid)
        elif b_correct and not e_correct:
            regressed.append(qid)

    print(f"\n=== vs Baseline ({args.baseline}) ===")
    base_c = sum(1 for r in base.values() if r.get("judge_correct"))
    print(f"  Baseline: {base_c}/{len(base)}")
    print(f"  Experiment: {grand_c}/{grand_t}")
    print(f"  Improved: {len(improved)}, Regressed: {len(regressed)}, Net: {len(improved)-len(regressed)}")

    if improved:
        print(f"\n  ✅ Improved (baseline錯→實驗對):")
        for qid in sorted(improved):
            r = exp[qid]
            val = r.get("sql_validation", "")
            tag = f" [validator: {val[:60]}]" if val else ""
            print(f"    #{qid} ({r['difficulty']}){tag}")

    if regressed:
        print(f"\n  ❌ Regressed (baseline對→實驗錯):")
        for qid in sorted(regressed):
            r = exp[qid]
            b = base[qid]
            val = r.get("sql_validation", "")
            tag = f" [validator: {val[:60]}]" if val else ""
            print(f"    #{qid} ({r['difficulty']}){tag}")
            print(f"      baseline answer: {b.get('pipeline_answer', '')[:80]}")
            print(f"      experiment answer: {r.get('pipeline_answer', '')[:80]}")

    # === 4. Validator 精準度 (只看有觸發的) ===
    if triggered:
        # 看 validator 觸發的題目在 baseline 是否本來就錯
        val_tp = 0  # baseline 也錯 → validator 正確發現問題
        val_fp = 0  # baseline 對 → validator 誤判
        val_unknown = 0
        for r in triggered:
            qid = r["question_id"]
            if qid in base:
                if not base[qid].get("judge_correct"):
                    val_tp += 1
                else:
                    val_fp += 1
            else:
                val_unknown += 1
        total_known = val_tp + val_fp
        print(f"\n=== Validator 精準度 (vs baseline ground truth) ===")
        print(f"  True Positive (baseline也錯，validator正確發現): {val_tp}")
        print(f"  False Positive (baseline對，validator誤判): {val_fp}")
        if total_known:
            print(f"  Precision: {val_tp}/{total_known} = {val_tp/total_known*100:.0f}%")

        # Retry 效果：觸發的題目中，retry 後答對了幾題
        retry_helped = sum(1 for r in triggered if r.get("judge_correct") and r["question_id"] in base and not base[r["question_id"]].get("judge_correct"))
        retry_hurt = sum(1 for r in triggered if not r.get("judge_correct") and r["question_id"] in base and base[r["question_id"]].get("judge_correct"))
        print(f"\n=== Retry 效果 (觸發的題目) ===")
        print(f"  Retry 救回 (baseline錯→實驗對): {retry_helped}")
        print(f"  Retry 搞壞 (baseline對→實驗錯): {retry_hurt}")
        print(f"  Net: {retry_helped - retry_hurt}")


if __name__ == "__main__":
    main()
