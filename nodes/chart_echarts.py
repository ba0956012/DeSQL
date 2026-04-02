"""
圖表生成節點 — PyEcharts 版本

生成互動式 HTML 圖表，替代 matplotlib 靜態 PNG。
透過環境變數 CHART_ENGINE=echarts 啟用。
"""

import copy
import json
from datetime import datetime, timedelta, date

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


def _render_table_html(data, question, max_rows=50):
    """用純 HTML/CSS 生成表格，回傳 HTML 字串"""
    if not data:
        return ""
    all_keys = list(data[0].keys())
    cols = [k for k in all_keys if not k.endswith("_id")]
    if not cols:
        cols = all_keys
    rows = data[:max_rows]

    title = question if len(question) <= 60 else question[:58] + "…"
    html_parts = [
        '<div style="font-family: Arial, sans-serif; padding: 10px;">',
        f'<h3 style="text-align:center; color:#333; margin-bottom:12px;">{title}</h3>',
        '<table style="border-collapse:collapse; width:100%; font-size:13px;">',
        "<thead><tr>",
    ]
    for c in cols:
        html_parts.append(
            f'<th style="background:#4472C4; color:white; padding:8px 12px; '
            f'text-align:center; border:1px solid #ddd;">{c}</th>'
        )
    html_parts.append("</tr></thead><tbody>")

    for i, row in enumerate(rows):
        bg = "#D9E2F3" if i % 2 == 0 else "#FFFFFF"
        html_parts.append(f'<tr style="background:{bg};">')
        for c in cols:
            val = str(row.get(c, ""))
            if len(val) > 40:
                val = val[:38] + "…"
            html_parts.append(
                f'<td style="padding:6px 10px; border:1px solid #ddd; text-align:center;">{val}</td>'
            )
        html_parts.append("</tr>")

    html_parts.append("</tbody></table>")
    if len(data) > max_rows:
        html_parts.append(
            f'<p style="text-align:center; color:gray; font-size:12px;">'
            f'（僅顯示前 {max_rows} 筆，共 {len(data)} 筆）</p>'
        )
    html_parts.append("</div>")
    return "\n".join(html_parts)


# PyEcharts 圖表類型對應
_ECHARTS_TYPE_MAP = {
    "bar": "Bar",
    "pie": "Pie",
    "line": "Line",
    "scatter": "Scatter",
    "treemap": "TreeMap",
    "heatmap": "HeatMap",
}


def generate_chart(state):
    sql_result = state.get("sql_result", [])
    if not sql_result or len(sql_result) < 3:
        debug_log("generate_chart_echarts", skip="data too small")
        return {"chart_code": "", "chart_option": "", "chart_html": "", "chart_image": ""}

    chart_data = state.get("chart_data", [])
    if chart_data and len(chart_data) >= 3:
        plot_data = chart_data
    else:
        plot_data = _enrich_id_columns(copy.deepcopy(sql_result), engine)

    plot_sample = json.dumps(plot_data[:5], indent=2, ensure_ascii=False, default=str)
    final_answer = state.get("final_answer", "")

    # Step 1: 判斷是否需要圖表（與 matplotlib 版相同邏輯）
    judge_prompt = f"""你是資料視覺化顧問。請判斷以下資料最適合用哪種方式呈現給使用者。

判斷標準：
1. 使用者只是要查/列出資料（如「有哪些」「列出」「提供」），且資料筆數少（≤10）→ 不需要視覺化 → should_chart: false, chart_type: "none"
2. 使用者要分析/比較（如「排名」「佔比」「趨勢」「比較」），且資料適合單一圖表（單一維度、≤20 筆）→ 畫圖表 → should_chart: true, chart_type: "bar"/"pie"/"line" 等
3. 資料是多維度交叉（如「每個 A × 每個 B」），或資料筆數多（>20），或有多個分組維度 → 圖表會太擠，改用表格圖片 → should_chart: true, chart_type: "table"
4. 資料筆數多但結構是扁平列表（每筆有多個欄位需要對照閱讀）→ should_chart: true, chart_type: "table"

注意：table（表格）也是一種視覺化輸出，當你認為適合用表格時，should_chart 必須設為 true，chart_type 設為 "table"。

使用者問題：{state["question"]}
分析結果（摘要）：{final_answer[:300] if len(final_answer) > 300 else final_answer}
資料筆數：{len(plot_data)}
資料欄位：{list(plot_data[0].keys()) if plot_data else []}
資料樣本：
{plot_sample}

只輸出純 JSON：
{{"insight": "選擇此呈現方式的理由", "should_chart": true/false, "chart_type": "bar/pie/line/treemap/scatter/table/none"}}
"""
    debug_log("generate_chart_echarts_judge", prompt=judge_prompt)
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

    debug_log("generate_chart_echarts_judge", should_chart=should_chart, chart_type=chart_type, insight=reason)
    if not should_chart or chart_type == "none":
        return {"chart_code": "", "chart_option": "", "chart_html": "", "chart_image": "", "chart_reason": reason}

    # 使用者指定圖表類型
    question_lower = state["question"].lower()
    for kw, ct in {
        "樹地圖": "treemap", "treemap": "treemap",
        "圓餅": "pie", "pie": "pie",
        "折線": "line", "line": "line",
        "散佈": "scatter",
        "熱力": "heatmap",
        "表格": "table", "table": "table",
    }.items():
        if kw in question_lower:
            chart_type = ct
            break

    # table 類型：直接生成 HTML 表格
    if chart_type == "table":
        debug_log("generate_chart_echarts", mode="table", rows=len(plot_data))
        html = _render_table_html(plot_data, state["question"])
        return {"chart_code": "", "chart_option": "", "chart_html": html, "chart_image": "", "chart_reason": reason}

    # Step 2: 生成 pyecharts code
    code_prompt = f"""根據以下資料用 pyecharts 畫一張 {chart_type} 圖表。

問題：{state["question"]}
資料筆數：{len(plot_data)}
資料欄位：{list(plot_data[0].keys()) if plot_data else []}
資料樣本：
{plot_sample}

輸出純 Python code（不要 JSON、不要解釋、不要 markdown）：
- data 變數已存在（list of dict），請使用完全一致的 key 名稱存取資料
- 可用的 import 已完成：pyecharts 的所有圖表類別和 opts 都可直接使用
- 可用的類別：Bar, Pie, Line, Scatter, TreeMap, HeatMap, Grid, Tab 等
- 可用的選項：opts.TitleOpts, opts.TooltipOpts, opts.LegendOpts, opts.ToolboxOpts 等
- datetime、timedelta、date 已可直接使用
- 日期/時間欄位已經是 Python datetime 物件，不需要解析字串
- 最後必須把圖表物件存入 chart 變數（如 chart = bar）
- 標題和軸標籤用繁體中文
- bar chart 項目多（>5）用 reversal_axis() 做水平並排序
- pie chart 顯示百分比，項目太多（>8）只顯示前幾名，其餘合併為「其他」
- 加上 ToolboxOpts 讓使用者可以下載圖片
- 設定合適的圖表大小：init_opts=opts.InitOpts(width="800px", height="500px")
"""
    debug_log("generate_chart_echarts_code", prompt=code_prompt)
    code_res = llm.invoke([HumanMessage(content=code_prompt)])
    chart_code = strip_code_fences(code_res.content)

    # 執行 + retry
    for attempt in range(2):
        try:
            result = _exec_echarts_code(chart_code, plot_data)
            if result:
                option_json, preview_html = result
                debug_log("generate_chart_echarts", option_size=len(option_json), attempt=attempt)
                return {
                    "chart_code": chart_code,
                    "chart_option": option_json,
                    "chart_html": preview_html,
                    "chart_image": "",
                    "chart_reason": reason,
                }
            raise ValueError("chart variable not found or render failed")
        except Exception as e:
            debug_log("generate_chart_echarts", error=str(e), attempt=attempt)
            if attempt == 0:
                fix_prompt = (
                    f"上次的 pyecharts code 執行失敗。\n錯誤：{e}\n失敗的 code：\n{chart_code}\n\n"
                    "請修正。datetime 是類別不是模組，日期欄位已是 datetime 物件。"
                    "最後必須把圖表物件存入 chart 變數。只輸出修正後的純 Python code。"
                )
                fix_res = llm.invoke([HumanMessage(content=fix_prompt)])
                chart_code = strip_code_fences(fix_res.content)

    return {"chart_code": chart_code, "chart_option": "", "chart_html": "", "chart_image": "", "chart_reason": reason}


def _exec_echarts_code(code, plot_data):
    """在受限環境中執行 pyecharts code，回傳 (option_json, preview_html)"""
    import pyecharts.charts as charts
    from pyecharts import options as opts
    from pyecharts.globals import ThemeType

    lines = [ln for ln in code.split("\n") if not ln.strip().startswith(("import ", "from "))]

    exec_ns = {
        "data": plot_data,
        "datetime": datetime,
        "timedelta": timedelta,
        "date": date,
        "opts": opts,
        "ThemeType": ThemeType,
        "Bar": charts.Bar,
        "Pie": charts.Pie,
        "Line": charts.Line,
        "Scatter": charts.Scatter,
        "TreeMap": charts.TreeMap,
        "HeatMap": charts.HeatMap,
        "Grid": charts.Grid,
        "Tab": charts.Tab,
        "Funnel": charts.Funnel,
        "Radar": charts.Radar,
        "WordCloud": charts.WordCloud,
        **SAFE_BUILTINS,
    }
    exec("\n".join(lines), exec_ns)

    chart_obj = exec_ns.get("chart")
    if chart_obj is None:
        return None

    import uuid
    options_json = chart_obj.dump_options()
    # 確保中文不被 escape 成 \uXXXX
    options_json = json.dumps(json.loads(options_json), ensure_ascii=False, indent=2)

    # 組裝預覽用 HTML（Streamlit components.html 用）
    chart_id = uuid.uuid4().hex[:12]
    width = "800px"
    height = "500px"
    try:
        if chart_obj.width:
            width = chart_obj.width
        if chart_obj.height:
            height = chart_obj.height
    except Exception:
        pass

    preview_html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
</head><body>
<div id="{chart_id}" style="width:{width};height:{height};"></div>
<script>
var chart = echarts.init(document.getElementById('{chart_id}'));
var option = {options_json};
chart.setOption(option);
window.addEventListener('resize', function(){{ chart.resize(); }});
</script>
</body></html>"""
    return options_json, preview_html
