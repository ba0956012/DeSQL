"""
核心圖表生成邏輯：LLM 判斷圖表類型、表格渲染、LLM 程式碼生成 + 沙箱執行。
"""

import json
import base64
import io

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from chart_service.config import Settings
from chart_service.models import ChartResponse
from chart_service.sandbox import execute_matplotlib_code, execute_echarts_code


def _strip_code_fences(text: str) -> str:
    """清除 LLM 回傳中的 markdown code fences。"""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


def _clean_llm_json(text: str) -> dict:
    """清除 LLM 回傳中的 markdown 包裹，解析 JSON。"""
    text = _strip_code_fences(text)
    return json.loads(text)


class ChartGenerator:
    """圖表生成器：判斷類型 → 渲染表格 / LLM 生成程式碼 → 沙箱執行。"""

    def __init__(self, settings: Settings):
        from openai import AzureOpenAI
        self.client = AzureOpenAI(
            api_key=settings.azure_openai_api_key,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.openai_api_version,
        )
        self.model = settings.llm_deployment
        self.temperature = settings.llm_temperature

    # ── 主入口 ────────────────────────────────────────────

    def generate(
        self,
        data: list[dict],
        question: str,
        engine: str,
        chart_type: str,
    ) -> ChartResponse:
        """主入口：判斷類型 → 生成圖表 → 回傳結果。"""
        reason = ""

        # Step 1: 判斷圖表類型
        if chart_type == "auto":
            should_chart, chart_type, reason = self._judge_chart_type(data, question)
            if not should_chart or chart_type == "none":
                return ChartResponse(chart_type="none", reason=reason)

        # Step 2: table 類型 → 直接渲染
        if chart_type == "table":
            if engine == "echarts":
                html = self._render_table_html(data, question)
                return ChartResponse(chart_type="table", reason=reason, chart_html=html)
            else:
                image = self._render_table_image(data, question)
                return ChartResponse(chart_type="table", reason=reason, chart_image=image)

        # Step 3: LLM 生成程式碼
        if engine == "echarts":
            option_json, html = self._generate_echarts(data, question, chart_type)
            return ChartResponse(
                chart_type=chart_type,
                reason=reason,
                chart_option=option_json,
                chart_html=html,
            )
        else:
            image = self._generate_matplotlib(data, question, chart_type)
            return ChartResponse(
                chart_type=chart_type,
                reason=reason,
                chart_image=image,
            )

    # ── LLM 呼叫輔助 ─────────────────────────────────────

    def _call_llm(self, prompt: str) -> str:
        """呼叫 Azure OpenAI，回傳內容字串。"""
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=self.temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content.strip()

    # ── 圖表類型判斷 ─────────────────────────────────────

    def _judge_chart_type(
        self, data: list[dict], question: str
    ) -> tuple[bool, str, str]:
        """使用 LLM 判斷圖表類型。回傳 (should_chart, chart_type, reason)。"""
        plot_sample = json.dumps(
            data[:5], indent=2, ensure_ascii=False, default=str
        )
        prompt = f"""你是資料視覺化顧問。請判斷以下資料最適合用哪種方式呈現給使用者。

判斷標準：
1. 使用者只是要查/列出資料（如「有哪些」「列出」「提供」），且資料筆數少（≤10）→ 不需要視覺化 → should_chart: false, chart_type: "none"
2. 使用者要分析/比較（如「排名」「佔比」「趨勢」「比較」），且資料適合單一圖表（單一維度、≤20 筆）→ 畫圖表 → should_chart: true, chart_type: "bar"/"pie"/"line" 等
3. 資料是多維度交叉（如「每個 A × 每個 B」），或資料筆數多（>20），或有多個分組維度 → 圖表會太擠，改用表格圖片 → should_chart: true, chart_type: "table"
4. 資料筆數多但結構是扁平列表（每筆有多個欄位需要對照閱讀）→ should_chart: true, chart_type: "table"

注意：table（表格）也是一種視覺化輸出，當你認為適合用表格時，should_chart 必須設為 true，chart_type 設為 "table"。

使用者問題：{question}
資料筆數：{len(data)}
資料欄位：{list(data[0].keys()) if data else []}
資料樣本：
{plot_sample}

只輸出純 JSON：
{{"insight": "選擇此呈現方式的理由", "should_chart": true/false, "chart_type": "bar/pie/line/treemap/scatter/table/none"}}
"""
        raw = self._call_llm(prompt)
        try:
            parsed = _clean_llm_json(raw)
            if not isinstance(parsed, dict):
                return (False, "none", "parse error")
            should_chart = parsed.get("should_chart", False)
            chart_type = parsed.get("chart_type", "bar")
            reason = parsed.get("insight", "")
            return (should_chart, chart_type, reason)
        except (json.JSONDecodeError, KeyError, ValueError):
            return (False, "none", "parse error")

    # ── 表格渲染 ─────────────────────────────────────────

    def _render_table_html(
        self, data: list[dict], question: str, max_rows: int = 50
    ) -> str:
        """用純 HTML/CSS 生成表格，回傳 HTML 字串。無 PostgreSQL 依賴。"""
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
                    f'<td style="padding:6px 10px; border:1px solid #ddd; '
                    f'text-align:center;">{val}</td>'
                )
            html_parts.append("</tr>")

        html_parts.append("</tbody></table>")
        if len(data) > max_rows:
            html_parts.append(
                f'<p style="text-align:center; color:gray; font-size:12px;">'
                f"（僅顯示前 {max_rows} 筆，共 {len(data)} 筆）</p>"
            )
        html_parts.append("</div>")
        return "\n".join(html_parts)

    def _render_table_image(
        self, data: list[dict], question: str, max_rows: int = 50
    ) -> str:
        """用 matplotlib 把 list of dict 畫成表格圖片，回傳 base64 PNG。無 PostgreSQL 依賴。"""
        if not data:
            return ""
        all_keys = list(data[0].keys())
        cols = [k for k in all_keys if not k.endswith("_id")]
        if not cols:
            cols = all_keys
        rows = data[:max_rows]
        cell_text = [[str(row.get(c, "")) for c in cols] for row in rows]

        # 截斷過長文字
        for r in cell_text:
            for i, v in enumerate(r):
                if len(v) > 25:
                    r[i] = v[:23] + "…"

        # 偵測數值欄位
        numeric_cols = set()
        for c in cols:
            sample_vals = [row.get(c) for row in data[:10] if row.get(c) is not None]
            if sample_vals and all(isinstance(v, (int, float)) for v in sample_vals):
                numeric_cols.add(c)

        n_rows = len(cell_text)
        n_cols = len(cols)

        # 根據欄位內容估算寬度
        col_max_len = []
        for j, c in enumerate(cols):
            max_len = len(c)
            for r in cell_text:
                max_len = max(max_len, len(r[j]))
            col_max_len.append(max_len)
        total_chars = sum(col_max_len)
        fig_w = max(10, total_chars * 0.18 + 2)
        row_height = 0.35 if n_rows > 30 else 0.4
        fig_h = min(max(3, row_height * n_rows + 2), 28)

        plt.rcParams["font.family"] = "Arial Unicode MS"
        fig, ax = plt.subplots(figsize=(fig_w, fig_h))
        ax.axis("off")
        title = question if len(question) <= 40 else question[:38] + "…"
        ax.set_title(title, fontsize=14, pad=14, fontweight="bold")

        table = ax.table(
            cellText=cell_text,
            colLabels=cols,
            loc="center",
            cellLoc="center",
            bbox=[0, 0, 1, 1],
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9 if n_rows <= 30 else 8)
        table.auto_set_column_width(list(range(n_cols)))
        row_scale = 1.3 if n_rows > 30 else 1.4
        table.scale(1, row_scale)

        # 表頭樣式
        for j in range(n_cols):
            cell = table[0, j]
            cell.set_facecolor("#4472C4")
            cell.set_text_props(color="white", fontweight="bold")
        # 資料列樣式
        for i in range(1, n_rows + 1):
            for j in range(n_cols):
                cell = table[i, j]
                if i % 2 == 0:
                    cell.set_facecolor("#D9E2F3")
                if cols[j] in numeric_cols:
                    cell._loc = "right"

        if len(data) > max_rows:
            ax.text(
                0.5, -0.01,
                f"（僅顯示前 {max_rows} 筆，共 {len(data)} 筆）",
                transform=ax.transAxes, ha="center", fontsize=9, color="gray",
            )

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode()

    # ── ECharts 程式碼生成 ────────────────────────────────

    def _generate_echarts(
        self, data: list[dict], question: str, chart_type: str
    ) -> tuple[str, str]:
        """LLM 生成 pyecharts 程式碼 → 沙箱執行 → 回傳 (option_json, html)。含 1 次重試。"""
        plot_sample = json.dumps(
            data[:5], indent=2, ensure_ascii=False, default=str
        )
        code_prompt = f"""根據以下資料用 pyecharts 畫一張 {chart_type} 圖表。

問題：{question}
資料筆數：{len(data)}
資料欄位：{list(data[0].keys()) if data else []}
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
        chart_code = _strip_code_fences(self._call_llm(code_prompt))

        for attempt in range(2):
            result = execute_echarts_code(chart_code, data)
            if result["success"]:
                return (result["option_json"], result["html"])
            if attempt == 0:
                fix_prompt = (
                    f"上次的 pyecharts code 執行失敗。\n錯誤：{result['error']}\n"
                    f"失敗的 code：\n{chart_code}\n\n"
                    "請修正。datetime 是類別不是模組，日期欄位已是 datetime 物件。"
                    "最後必須把圖表物件存入 chart 變數。只輸出修正後的純 Python code。"
                )
                chart_code = _strip_code_fences(self._call_llm(fix_prompt))

        return ("", "")

    # ── Matplotlib 程式碼生成 ─────────────────────────────

    def _generate_matplotlib(
        self, data: list[dict], question: str, chart_type: str
    ) -> str:
        """LLM 生成 matplotlib 程式碼 → 沙箱執行 → 回傳 base64 PNG。含 1 次重試。"""
        plot_sample = json.dumps(
            data[:5], indent=2, ensure_ascii=False, default=str
        )
        code_prompt = f"""根據以下資料畫一張 {chart_type} 圖表。

問題：{question}
資料筆數：{len(data)}
資料欄位：{list(data[0].keys()) if data else []}
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
        chart_code = _strip_code_fences(self._call_llm(code_prompt))

        for attempt in range(2):
            result = execute_matplotlib_code(chart_code, data)
            if result["success"]:
                return result["image"]
            if attempt == 0:
                fix_prompt = (
                    f"上次的圖表 code 執行失敗。\n錯誤：{result['error']}\n"
                    f"失敗的 code：\n{chart_code}\n\n"
                    "請修正，不要 import，datetime 是類別不是模組，日期欄位已是 datetime 物件。"
                    "只輸出修正後的純 Python code。"
                )
                chart_code = _strip_code_fences(self._call_llm(fix_prompt))

        return ""
