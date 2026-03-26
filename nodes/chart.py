"""
圖表生成節點
"""

import copy
import json
import base64
import io
from datetime import datetime, timedelta, date

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from langchain_core.messages import HumanMessage
from sqlalchemy import text as sa_text

from db import engine
from llm import llm
from utils import debug_log, clean_llm_json, strip_code_fences
from nodes.code import SAFE_BUILTINS


def _enrich_id_columns(data, eng):
    """嘗試把 ID 欄位對應到 name 欄位，讓圖表更易讀"""
    if not data:
        return data
    columns = list(data[0].keys())
    has_name_cols = any(c.endswith("_name") or c == "name" for c in columns)
    id_cols = [c for c in columns if c.endswith("_id")]
    if not id_cols or has_name_cols:
        return data
    name_sql = """
    SELECT table_name, column_name FROM information_schema.columns
    WHERE table_schema = 'public' AND (column_name LIKE '%\\_name' ESCAPE '\\' OR column_name = 'name')
    """
    try:
        with eng.connect() as conn:
            name_cols_rows = conn.execute(sa_text(name_sql)).fetchall()
            table_name_map = {r[0]: r[1] for r in name_cols_rows}
            table_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            existing_tables = {
                r[0] for r in conn.execute(sa_text(table_sql)).fetchall()
            }
    except Exception:
        return data
    for id_col in id_cols:
        table_guess = id_col.replace("_id", "")
        if table_guess not in existing_tables:
            continue
        name_col = table_name_map.get(table_guess)
        if not name_col:
            continue
        try:
            with eng.connect() as conn:
                rows = conn.execute(
                    sa_text(f'SELECT "{id_col}", "{name_col}" FROM "{table_guess}"')
                ).fetchall()
            id_to_name = {str(r[0]): r[1] for r in rows if r[1]}
            if not id_to_name:
                continue
            for row in data:
                if id_col in row:
                    row[name_col] = id_to_name.get(str(row[id_col]), str(row[id_col]))
        except Exception:
            continue
    return data


def _render_table_image(data, question, max_rows=50):
    """用 matplotlib 把 list of dict 畫成表格圖片，回傳 base64 PNG"""
    if not data:
        return ""
    # 過濾掉 _id 欄位，只保留可讀欄位
    all_keys = list(data[0].keys())
    cols = [k for k in all_keys if not k.endswith("_id")]
    if not cols:
        cols = all_keys
    rows = data[:max_rows]
    cell_text = [[str(row.get(c, "")) for c in cols] for row in rows]
    # 截斷過長的文字
    for r in cell_text:
        for i, v in enumerate(r):
            if len(v) > 30:
                r[i] = v[:28] + "…"

    n_rows = len(cell_text)
    n_cols = len(cols)
    fig_w = max(8, n_cols * 2.5)
    fig_h = max(3, 0.4 * n_rows + 1.5)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")
    plt.rcParams["font.family"] = "Arial Unicode MS"
    title = question if len(question) <= 40 else question[:38] + "…"
    ax.set_title(title, fontsize=14, pad=12)

    table = ax.table(
        cellText=cell_text,
        colLabels=cols,
        loc="center",
        cellLoc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.4)
    # 表頭樣式
    for j in range(n_cols):
        cell = table[0, j]
        cell.set_facecolor("#4472C4")
        cell.set_text_props(color="white", fontweight="bold")
    # 斑馬紋
    for i in range(1, n_rows + 1):
        for j in range(n_cols):
            if i % 2 == 0:
                table[i, j].set_facecolor("#D9E2F3")

    if len(data) > max_rows:
        ax.text(
            0.5, -0.02, f"（僅顯示前 {max_rows} 筆，共 {len(data)} 筆）",
            transform=ax.transAxes, ha="center", fontsize=9, color="gray",
        )

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def generate_chart(state):
    sql_result = state.get("sql_result", [])
    if not sql_result or len(sql_result) < 3:
        debug_log("generate_chart", skip="data too small")
        return {"chart_code": "", "chart_image": ""}

    chart_data = state.get("chart_data", [])
    if chart_data and len(chart_data) >= 3:
        plot_data = chart_data
    else:
        plot_data = _enrich_id_columns(copy.deepcopy(sql_result), engine)

    plot_sample = json.dumps(plot_data[:5], indent=2, ensure_ascii=False, default=str)
    final_answer = state.get("final_answer", "")

    # Step 1: 判斷是否需要圖表
    judge_prompt = f"""你是資料視覺化顧問。請判斷以下資料最適合用哪種方式呈現給使用者。

判斷標準：
1. 使用者只是要查/列出資料（如「有哪些」「列出」「提供」），且資料筆數少（≤10）→ 不需要視覺化 → should_chart: false, chart_type: "none"
2. 使用者要分析/比較（如「排名」「佔比」「趨勢」「比較」），且資料適合單一圖表（單一維度、≤20 筆）→ 畫圖表 → should_chart: true, chart_type: "bar"/"pie"/"line" 等
3. 資料是多維度交叉（如「每個 A × 每個 B」），或資料筆數多（>20），或有多個分組維度 → 圖表會太擠，改用表格圖片 → should_chart: true, chart_type: "table"
4. 資料筆數多但結構是扁平列表（每筆有多個欄位需要對照閱讀）→ should_chart: true, chart_type: "table"

注意：table（表格圖片）也是一種視覺化輸出，當你認為適合用表格時，should_chart 必須設為 true，chart_type 設為 "table"。

使用者問題：{state["question"]}
分析結果（摘要）：{final_answer[:300] if len(final_answer) > 300 else final_answer}
資料筆數：{len(plot_data)}
資料欄位：{list(plot_data[0].keys()) if plot_data else []}
資料樣本：
{plot_sample}

只輸出純 JSON：
{{"insight": "選擇此呈現方式的理由", "should_chart": true/false, "chart_type": "bar/pie/line/treemap/scatter/table/none"}}
"""
    debug_log("generate_chart_judge", prompt=judge_prompt)
    judge_res = llm.invoke([HumanMessage(content=judge_prompt)])
    try:
        parsed = clean_llm_json(judge_res.content)
        should_chart = parsed.get("should_chart", False)
        chart_type = parsed.get("chart_type", "bar")
        reason = parsed.get("insight", "")
    except (json.JSONDecodeError, KeyError):
        should_chart = False
        chart_type = "none"
        reason = "parse error"

    debug_log(
        "generate_chart_judge",
        should_chart=should_chart,
        chart_type=chart_type,
        insight=reason,
    )
    if not should_chart or chart_type == "none":
        return {"chart_code": "", "chart_image": "", "chart_reason": reason}

    # 使用者指定圖表類型（優先於 LLM 判斷）
    question_lower = state["question"].lower()
    for kw, ct in {
        "樹地圖": "treemap",
        "treemap": "treemap",
        "圓餅": "pie",
        "pie": "pie",
        "折線": "line",
        "line": "line",
        "散佈": "scatter",
        "熱力": "heatmap",
        "表格": "table",
        "table": "table",
    }.items():
        if kw in question_lower:
            chart_type = ct
            break

    # table 類型：直接用 matplotlib 畫表格圖片，不需要 LLM 生成 code
    if chart_type == "table":
        debug_log("generate_chart", mode="table", rows=len(plot_data))
        img_b64 = _render_table_image(plot_data, state["question"])
        return {
            "chart_code": "",
            "chart_image": img_b64,
            "chart_reason": reason,
        }

    # Step 2: 生成 chart code
    code_prompt = f"""根據以下資料畫一張 {chart_type} 圖表。

問題：{state["question"]}
資料筆數：{len(plot_data)}
資料欄位：{list(plot_data[0].keys()) if plot_data else []}
資料樣本：
{plot_sample}

輸出 matplotlib Python code（不要 JSON、不要解釋、不要 markdown）：
- data 變數已存在（list of dict），請使用完全一致的 key 名稱存取資料
- fig, ax, plt, buf, matplotlib, squarify 已存在，直接使用，不要重新建立 fig 或 ax
- 不要使用 numpy，用純 Python 內建函式處理數值計算
- 不要 import 任何東西
- datetime、timedelta、date 已可直接使用（是類別不是模組）
- 日期/時間欄位已經是 Python datetime 物件，不需要解析字串
- 中文字型：plt.rcParams["font.family"] = "Arial Unicode MS"
- 最後呼叫 fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
- 不要 plt.show()
- 標題和軸標籤用繁體中文
- bar chart 項目多（>5）用水平 barh 並排序
- pie chart 顯示百分比，項目太多（>8）只顯示前幾名，其餘合併為「其他」
- treemap 用 squarify.plot()，顯示標籤和數值
- 配色：使用 colors = plt.cm.Set3(range(len(data))) 產生色盤
"""
    debug_log("generate_chart_code", prompt=code_prompt)
    code_res = llm.invoke([HumanMessage(content=code_prompt)])
    chart_code = strip_code_fences(code_res.content)

    # 執行 + retry
    for attempt in range(2):
        try:
            plt.rcParams["font.family"] = "Arial Unicode MS"
            fig, ax = plt.subplots(figsize=(9, 7))
            buf = io.BytesIO()
            lines = [
                ln
                for ln in chart_code.split("\n")
                if not ln.strip().startswith(("import ", "from "))
            ]
            exec_ns = {
                "data": plot_data,
                "fig": fig,
                "ax": ax,
                "plt": plt,
                "buf": buf,
                "matplotlib": matplotlib,
                "squarify": __import__("squarify"),
                "datetime": datetime,
                "timedelta": timedelta,
                "date": date,
                **SAFE_BUILTINS,
            }
            exec("\n".join(lines), exec_ns)
            final_fig = exec_ns.get("fig", fig)
            if buf.tell() == 0:
                final_fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
            buf.seek(0)
            img_b64 = base64.b64encode(buf.read()).decode()
            plt.close("all")
            debug_log("generate_chart", image_size=len(img_b64), attempt=attempt)
            return {
                "chart_code": chart_code,
                "chart_image": img_b64,
                "chart_reason": reason,
            }
        except Exception as e:
            plt.close("all")
            debug_log("generate_chart", error=str(e), attempt=attempt)
            if attempt == 0:
                fix_prompt = (
                    f"上次的圖表 code 執行失敗。\n錯誤：{e}\n失敗的 code：\n{chart_code}\n\n"
                    "請修正，不要 import，datetime 是類別不是模組，日期欄位已是 datetime 物件。"
                    "只輸出修正後的純 Python code。"
                )
                fix_res = llm.invoke([HumanMessage(content=fix_prompt)])
                chart_code = strip_code_fences(fix_res.content)

    return {"chart_code": chart_code, "chart_image": "", "chart_reason": reason}
