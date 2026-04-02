"""
生成單一 DB 的評測報告（含圖表）
用法：
    python eval/generate_report.py --db california_schools
    python eval/generate_report.py --db financial
"""

import argparse
import json
import glob
import os
import sys
from pathlib import Path
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "Arial Unicode MS"

EVAL_DIR = Path(__file__).parent


def load_results(tag, db_id):
    files = sorted(glob.glob(str(EVAL_DIR / "results" / tag / f"{db_id}_*.json")))
    results = []
    for f in files:
        with open(f) as fh:
            results.append(json.load(fh))
    return results


def calc_stats(results):
    total = len(results)
    correct = sum(1 for r in results if r.get("judge_correct"))
    by_diff = {}
    error_types = Counter()
    for r in results:
        diff = r["difficulty"]
        if diff not in by_diff:
            by_diff[diff] = {"total": 0, "correct": 0}
        by_diff[diff]["total"] += 1
        if r.get("judge_correct"):
            by_diff[diff]["correct"] += 1
        else:
            answer = r.get("pipeline_answer", "")
            sql = r.get("pipeline_sql", "")
            reason = r.get("judge_reason", "")
            if not sql and not answer:
                error_types["Pipeline/API 失敗"] += 1
            elif "查無" in answer or "無法" in answer or "沒有" in answer[:20]:
                error_types["查無資料"] += 1
            elif any(w in reason for w in ["數值", "數量", "分數", "比例", "百分", "不一致", "不符"]) and any(w in reason for w in ["數值", "數", "分數", "比例", "百分", "數量"]):
                error_types["數值/排名錯誤"] += 1
            elif any(w in reason for w in ["不完整", "未包含", "缺少", "部分", "只列出"]):
                error_types["回答不完整"] += 1
            else:
                error_types["欄位/實體選錯"] += 1
    return {
        "total": total, "correct": correct,
        "pct": correct / total * 100 if total > 0 else 0,
        "by_diff": by_diff, "error_types": error_types
    }


def find_experiments(db_id):
    """自動偵測有此 DB 結果的所有 tag"""
    experiments = {}
    results_dir = EVAL_DIR / "results"
    for tag_dir in sorted(results_dir.iterdir()):
        if not tag_dir.is_dir() or tag_dir.name.startswith("."):
            continue
        files = list(tag_dir.glob(f"{db_id}_*.json"))
        if files:
            # 生成顯示標籤
            tag = tag_dir.name
            if "desql" in tag:
                label = "DeSQL"
                # 模型標記
                if "54mini" in tag:
                    label += " (5.4-mini)"
                elif "41mini" in tag:
                    label += " (4.1-mini)"
                # 配置標記
                if "reasoning" in tag:
                    effort = "high" if "high" in tag else "medium"
                    label += f"\n(reasoning={effort})"
                elif "compactdesc_in_q" in tag:
                    label += "\n(desc in question)"
                elif "cn_validate" in tag:
                    label += "\n(+validate node CN)"
                elif "en_validate" in tag:
                    label += "\n(+validate node EN)"
                elif "run2" in tag:
                    label += "\n(desc via State)"
                elif "compactdesc" in tag:
                    label += "\n(+compact desc)"
                elif "fulldesc_in_q" in tag:
                    label += "\n(+full desc in Q)"
                elif "fulldesc_in_sql" in tag:
                    label += "\n(+full desc in SQL)"
                elif "filterdesc" in tag:
                    label += "\n(+filtered desc)"
                else:
                    label += "\n(evidence only)"
            elif "greg" in tag:
                label = "Greg-T2S"
                if "compactdesc" in tag:
                    label += "\n(+compact desc)"
                else:
                    label += "\n(evidence only)"
            else:
                label = tag
            experiments[label] = tag
    return experiments


def generate_charts(db_id, experiments, all_stats, out_dir):
    labels = list(all_stats.keys())
    n = len(labels)
    colors = ["#4472C4", "#5B9BD5", "#70AD47", "#2E75B6", "#ED7D31", "#FFC000"][:n]

    # Chart 1: 整體準確率
    fig, ax = plt.subplots(figsize=(max(8, n * 1.5), 5))
    values = [all_stats[l]["pct"] for l in labels]
    bars = ax.bar(labels, values, color=colors, width=0.6)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f"{val:.1f}%", ha="center", fontsize=11, fontweight="bold")
    ax.set_ylabel("Accuracy (%)", fontsize=12)
    ax.set_title(f"Overall Accuracy — {db_id}", fontsize=13)
    ax.set_ylim(0, 80)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(out_dir / "chart_overall.png", dpi=150)
    plt.close(fig)
    print(f"  ✅ chart_overall.png")

    # Chart 2: 按難度（上下排列）
    diff_order = ["simple", "moderate", "challenging"]
    diff_titles = ["Simple", "Moderate", "Challenging"]
    fig, axes = plt.subplots(3, 1, figsize=(11, 14), sharey=True)
    for idx, (diff, title) in enumerate(zip(diff_order, diff_titles)):
        ax = axes[idx]
        vals = []
        lbls = []
        for label in labels:
            s = all_stats[label]
            d = s["by_diff"].get(diff, {"correct": 0, "total": 0})
            pct = d["correct"] / d["total"] * 100 if d["total"] > 0 else 0
            vals.append(pct)
            lbls.append(f"{d['correct']}/{d['total']}")
        bars = ax.barh(range(n), vals, color=colors, height=0.6)
        for i, (bar, lbl) in enumerate(zip(bars, lbls)):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
                    f"{vals[i]:.1f}% ({lbl})", va="center", fontsize=10)
        ax.set_title(title, fontsize=13, fontweight="bold", loc="left")
        ax.set_yticks(range(n))
        ax.set_yticklabels([l.replace("\n", " ") for l in labels], fontsize=9)
        ax.set_xlim(0, 105)
        ax.set_xlabel("Accuracy (%)", fontsize=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.invert_yaxis()
    plt.suptitle(f"Accuracy by Difficulty — {db_id}", fontsize=14, y=1.01)
    plt.tight_layout()
    fig.savefig(out_dir / "chart_by_difficulty.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  ✅ chart_by_difficulty.png")

    # Chart 3: 錯誤類型（所有實驗）
    compare_list = [(l, all_stats[l]) for l in labels if all_stats[l]["total"] > all_stats[l]["correct"]]

    n_charts = len(compare_list)
    cols = min(n_charts, 2)
    rows = (n_charts + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(7 * cols, 6 * rows))
    if n_charts == 1:
        axes = [axes]
    else:
        axes = axes.flatten() if hasattr(axes, 'flatten') else [axes]
    pie_colors = ["#FF6B6B", "#FFA07A", "#FFD700", "#87CEEB", "#DDA0DD"]
    for idx, (label, s) in enumerate(compare_list):
        ax = axes[idx]
        et = s["error_types"]
        if not et:
            continue
        cats = list(et.keys())
        vals = list(et.values())
        wedges, texts, autotexts = ax.pie(vals, labels=cats, autopct="%1.0f%%",
                                           colors=pie_colors[:len(cats)], startangle=90,
                                           textprops={"fontsize": 9})
        ax.set_title(f"{label.replace(chr(10), ' ')}\n({s['total'] - s['correct']} errors)", fontsize=11)
    plt.suptitle(f"Error Type Distribution — {db_id}", fontsize=13, y=1.02)
    # 隱藏多餘的 axes
    for i in range(n_charts, len(axes)):
        axes[i].set_visible(False)
    plt.tight_layout()
    fig.savefig(out_dir / "chart_error_types.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  ✅ chart_error_types.png")


def generate_md(db_id, experiments, all_stats, out_dir, template_override=None):
    # 載入模板和 DB 特定描述
    template_path = EVAL_DIR / (template_override or "report_template.md")
    dataset_desc_path = EVAL_DIR / "databases" / db_id / "dataset_description.md"
    evidence_ex_path = EVAL_DIR / "databases" / db_id / "evidence_examples.md"

    template = template_path.read_text(encoding="utf-8") if template_path.exists() else "# {db_id}\n"
    dataset_desc = dataset_desc_path.read_text(encoding="utf-8").strip() if dataset_desc_path.exists() else ""
    evidence_ex = evidence_ex_path.read_text(encoding="utf-8").strip() if evidence_ex_path.exists() else ""

    header = template.format(
        db_id=db_id,
        dataset_description=dataset_desc,
        evidence_examples=evidence_ex,
    )

    lines = [header]

    # 結果表
    lines.append("\n## 4. 整體結果\n")
    lines.append("![Overall Accuracy](chart_overall.png)\n")
    lines.append("| 配置 | 準確率 | Simple | Moderate | Challenging |")
    lines.append("|------|--------|--------|----------|-------------|")
    for label, s in all_stats.items():
        name = label.replace("\n", " ")
        by = s["by_diff"]
        row_parts = [f"| {name} | {s['pct']:.1f}%"]
        for diff in ["simple", "moderate", "challenging"]:
            d = by.get(diff, {"correct": 0, "total": 0})
            pct = d["correct"] / d["total"] * 100 if d["total"] > 0 else 0
            row_parts.append(f"{d['correct']}/{d['total']} ({pct:.1f}%)")
        lines.append(" | ".join(row_parts) + " |")

    lines.append("\n## 5. 按難度分析\n")
    lines.append("![By Difficulty](chart_by_difficulty.png)\n")

    lines.append("\n## 6. 錯誤類型分析\n")
    lines.append("![Error Types](chart_error_types.png)\n")

    for label, s in all_stats.items():
        name = label.replace("\n", " ")
        err_total = s["total"] - s["correct"]
        lines.append(f"### {name} — {err_total} 題錯誤\n")
        lines.append("| 錯誤類型 | 數量 | 佔比 |")
        lines.append("|----------|------|------|")
        for cat, cnt in s["error_types"].most_common():
            pct = cnt / err_total * 100 if err_total > 0 else 0
            lines.append(f"| {cat} | {cnt} | {pct:.0f}% |")
        lines.append("")

    # 統計顯著性分析（如果有 2 組以上實驗）
    stat_labels = list(all_stats.keys())
    if len(stat_labels) >= 2:
        import math
        lines.append("\n## 7. 統計顯著性分析\n")
        lines.append("使用 Two-proportion z-test 檢驗兩組準確率是否有統計上的顯著差異。\n")
        lines.append("- 顯著性（p-value）：p 值越小，越有信心認為差異不是隨機波動。p < 0.05 為顯著，p < 0.01 為非常顯著，p < 0.001 為極顯著")
        lines.append("- 95% 信賴區間：在 95% 的信心水準下，真實的準確率差距落在此範圍內。若區間下限 > 0%，代表優勢方確實優於對方\n")
        for i in range(len(stat_labels)):
            for j in range(i + 1, len(stat_labels)):
                l1, l2 = stat_labels[i], stat_labels[j]
                s1, s2 = all_stats[l1], all_stats[l2]
                n1, c1 = s1["total"], s1["correct"]
                n2, c2 = s2["total"], s2["correct"]
                p1, p2 = c1 / n1, c2 / n2
                diff = abs(p1 - p2)
                # Two-proportion z-test
                p_pool = (c1 + c2) / (n1 + n2)
                se = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
                z = (p1 - p2) / se if se > 0 else 0
                # 信賴區間
                se_diff = math.sqrt(p1*(1-p1)/n1 + p2*(1-p2)/n2)
                ci_lower = (p1 - p2 - 1.96 * se_diff) * 100
                ci_upper = (p1 - p2 + 1.96 * se_diff) * 100
                # p-value 近似計算（標準常態分布）
                abs_z = abs(z)
                # Abramowitz and Stegun approximation
                t = 1.0 / (1.0 + 0.2316419 * abs_z)
                d = 0.3989422804014327  # 1/sqrt(2*pi)
                p_one_tail = d * math.exp(-abs_z * abs_z / 2.0) * (t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429)))))
                p_value = 2 * p_one_tail
                if p_value < 0.001:
                    sig = f"p = {p_value:.4f}（極顯著）"
                elif p_value < 0.01:
                    sig = f"p = {p_value:.4f}（非常顯著）"
                elif p_value < 0.05:
                    sig = f"p = {p_value:.4f}（顯著）"
                else:
                    sig = f"p = {p_value:.4f}（不顯著）"
                n1_name = l1.replace("\n", " ")
                n2_name = l2.replace("\n", " ")
                winner = n1_name if p1 > p2 else n2_name
                ci_lo = ci_lower if p1 > p2 else -ci_upper
                ci_hi = ci_upper if p1 > p2 else -ci_lower
                lines.append(f"### {n1_name} vs {n2_name}\n")
                lines.append(f"| 指標 | 數值 |")
                lines.append(f"|------|------|")
                lines.append(f"| 準確率差距 | {diff*100:.1f}% |")
                lines.append(f"| z-score | {abs(z):.3f} |")
                lines.append(f"| 顯著性 | {sig} |")
                lines.append(f"| 95% 信賴區間 | {winner} 優於對方 {ci_lo:.1f}% ~ {ci_hi:.1f}% |")
                lines.append("")
                # 白話結論
                if p_value < 0.05:
                    lines.append(f"> ✅ **{winner}** 顯著優於對方，在 95% 信心下至少優 {ci_lo:.1f} 個百分點。")
                elif p1 != p2:
                    higher = n1_name if p1 > p2 else n2_name
                    lines.append(f"> ⚠️ {higher} 觀測上較高（差 {diff*100:.1f}%），但兩者無顯著差異，無法判斷誰更優。需要更多資料才能確認。")
                else:
                    lines.append(f"> ➖ 兩者表現相同。")
                lines.append("")

    md_path = out_dir / "REPORT.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  ✅ REPORT.md")


def generate_html(out_dir):
    """簡易 md → html 轉換"""
    import re, base64
    md_path = out_dir / "REPORT.md"
    html_path = out_dir / "REPORT.html"
    md_text = md_path.read_text(encoding="utf-8")

    html_lines = []
    in_table = False
    for line in md_text.split("\n"):
        s = line.strip()
        img = re.match(r"!\[(.+?)\]\((.+?)\)", s)
        if img:
            alt, src = img.groups()
            p = out_dir / src
            if p.exists():
                b64 = base64.b64encode(p.read_bytes()).decode()
                html_lines.append(f'<img src="data:image/png;base64,{b64}" alt="{alt}" style="max-width:100%;">')
            continue
        if s.startswith("# "):
            html_lines.append(f"<h1>{s[2:]}</h1>")
            continue
        if s.startswith("## "):
            html_lines.append(f"<h2>{s[3:]}</h2>")
            continue
        if s.startswith("### "):
            html_lines.append(f"<h3>{s[4:]}</h3>")
            continue
        if "|" in s and s.startswith("|"):
            cells = [c.strip() for c in s.split("|")[1:-1]]
            if all(set(c) <= set("- :") for c in cells):
                continue
            if not in_table:
                html_lines.append("<table>")
                in_table = True
                html_lines.append("<tr>" + "".join(f"<th>{c}</th>" for c in cells) + "</tr>")
            else:
                html_lines.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
            continue
        else:
            if in_table:
                html_lines.append("</table>")
                in_table = False
        if not s:
            html_lines.append("")
            continue
        html_lines.append(f"<p>{s}</p>")
    if in_table:
        html_lines.append("</table>")

    body = "\n".join(html_lines)
    html = f"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="UTF-8">
<title>DeSQL Eval — {out_dir.name}</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #24292e; line-height: 1.6; }}
h1 {{ border-bottom: 2px solid #4472C4; padding-bottom: 8px; }}
h2 {{ border-bottom: 1px solid #eee; padding-bottom: 6px; margin-top: 32px; }}
table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
th {{ background: #4472C4; color: white; }}
tr:nth-child(even) {{ background: #f6f8fa; }}
img {{ margin: 16px 0; border: 1px solid #eee; border-radius: 4px; }}
</style></head><body>{body}</body></html>"""
    html_path.write_text(html, encoding="utf-8")
    print(f"  ✅ REPORT.html")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="DB name or 'all' for combined report")
    parser.add_argument("--tags", nargs="*", default=None, help="只用指定的 tag（不指定則自動偵測全部）")
    parser.add_argument("--output", default=None, help="自訂輸出資料夾名稱（預設用 db name）")
    parser.add_argument("--template", default=None, help="自訂報告模板（如 report_template_model.md）")
    args = parser.parse_args()

    if args.db == "all":
        # 整合報告
        all_dbs = ["california_schools", "financial", "debit_card_specializing"]
        out_dir = EVAL_DIR / "report" / (args.output or "combined")
        out_dir.mkdir(parents=True, exist_ok=True)

        # 找出所有 DB 都有的 tag
        common_tags = None
        for db_id in all_dbs:
            exps = find_experiments(db_id)
            if args.tags:
                exps = {l: t for l, t in exps.items() if t in args.tags}
            tags = set(exps.values())
            common_tags = tags if common_tags is None else common_tags & tags

        if not common_tags:
            print("❌ 沒有所有 DB 都有的實驗")
            sys.exit(1)

        # 合併結果 — 用第一個 DB 的 label 映射
        first_db = all_dbs[0]
        experiments = {}
        first_exps = find_experiments(first_db)
        if args.tags:
            first_exps = {l: t for l, t in first_exps.items() if t in args.tags}
        for label, tag in first_exps.items():
            if tag in common_tags:
                experiments[label] = tag

        print(f"📊 生成整合報告（{len(experiments)} 組實驗 × {len(all_dbs)} DB）\n")

        all_stats = {}
        for label, tag in experiments.items():
            combined = []
            for db_id in all_dbs:
                combined.extend(load_results(tag, db_id))
            if combined:
                all_stats[label] = calc_stats(combined)

        generate_charts("combined (3 DBs)", experiments, all_stats, out_dir)
        generate_md("combined (3 DBs)", experiments, all_stats, out_dir, template_override=args.template)
        generate_html(out_dir)
        print(f"\n✅ 整合報告已生成至 {out_dir}/")
    else:
        db_id = args.db
        out_dir = EVAL_DIR / "report" / (args.output or db_id)
        out_dir.mkdir(parents=True, exist_ok=True)

        experiments = find_experiments(db_id)
        if args.tags:
            experiments = {l: t for l, t in experiments.items() if t in args.tags}
        if not experiments:
            print(f"❌ 找不到 {db_id} 的評測結果")
            sys.exit(1)

        print(f"📊 生成 {db_id} 報告（{len(experiments)} 組實驗）\n")

        all_stats = {}
        for label, tag in experiments.items():
            data = load_results(tag, db_id)
            if data:
                all_stats[label] = calc_stats(data)

        generate_charts(db_id, experiments, all_stats, out_dir)
        generate_md(db_id, experiments, all_stats, out_dir, template_override=args.template)
        generate_html(out_dir)
        print(f"\n✅ 報告已生成至 {out_dir}/")


if __name__ == "__main__":
    main()
