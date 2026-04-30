"""Code node prompt templates — default (中文, gpt-4.1-mini)"""


def build_code_prompt(question, sql, sql_result_len, sample, result_guidance_section, error_context, chart_instruction, data_profile="") -> str:
    import json
    columns = list(sample[0].keys()) if sample and isinstance(sample[0], dict) else []
    sample_str = json.dumps(sample, indent=2, ensure_ascii=False, default=str)

    profile_section = f"\n{data_profile}\n" if data_profile else ""

    return f"""
問題：{question}

SQL 查詢已執行完成，data 變數中包含完整結果：
SQL：{sql}

資料筆數：{sql_result_len}
資料欄位：{columns}
資料樣本：
{sample_str}
{profile_section}
{result_guidance_section}
{error_context}
請寫 Python code 處理 data 變數中的資料：
1. 根據任務需求處理 data（計算、篩選、排序、整理格式等）
2. 將最終答案存入 result 變數
   - 如果 SQL 已經用 GROUP BY / ORDER BY / LIMIT / 聚合函數算出結果，不要再重複聚合，直接提取答案
   - 如果 SQL 回傳原始資料，按照 Python 任務描述進行處理
   - result 應該是完整的答案，包含所有需要的數值和名稱
3. 如果資料中有 None 值，做數學運算前要先過濾掉，不要省略
{chart_instruction}

重要提醒：
- data 是 list of dict，每個 dict 的 key 就是 SQL SELECT 的欄位名（小寫）
- 只使用 data 中存在的欄位（參考上方的欄位列表），不要引用 SQL 沒有 SELECT 的欄位
- SQL 的 WHERE 已經篩選過資料了，不要在 Python 中重複篩選
- 如果資料特徵顯示有重複行，考慮是否需要去重後再計數或聚合
- 如果問題問的是「數量」或「有多少」，檢查是否需要 COUNT(DISTINCT)
- 如果問題問的是「比例」或「百分比」，確認分子和分母都從 data 中正確計算
- 不能使用 import 語句
- Counter、defaultdict、Decimal、datetime、timedelta、date 已可直接使用
- SQL 回傳的日期欄位已統一為 datetime.date 物件，直接用 < > == 比較即可
- 不要對 date 物件呼叫 .date() 方法或做字串解析
"""


def build_chart_instruction(enable_chart: bool) -> str:
    if not enable_chart:
        return ""
    return """4. 另外將適合畫圖的結構化資料存入 chart_data 變數（list of dict）
   - chart_data 的 key 必須用使用者看得懂的名稱，不要用 ID
   - 如果結果不適合畫圖（如單一數值、純列表、沒有數值比較），chart_data 設為空 list []"""
