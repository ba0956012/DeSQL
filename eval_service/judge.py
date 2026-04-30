"""LLM 判題模組：封裝 Gold SQL 執行與 LLM Judge 邏輯。"""

import json
import sqlite3

from eval_service.config import Settings


def run_gold_sql(sqlite_path: str, gold_sql: str) -> list[dict]:
    """在 SQLite 上執行 Gold SQL，回傳結果列表。"""
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()
    cursor.execute(gold_sql)
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


class LLMJudge:
    """使用 Azure OpenAI 比對預期結果與使用者答案的語意判斷模組。"""

    def __init__(self, settings: Settings):
        from openai import AzureOpenAI

        self.client = AzureOpenAI(
            api_key=settings.azure_openai_api_key,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.openai_api_version,
        )
        self.model = settings.llm_deployment
        self.temperature = settings.llm_temperature

    def judge(self, question: str, expected: list[dict], actual_answer: str) -> dict:
        """
        比對預期結果與使用者答案。

        回傳 {"correct": bool, "reason": str}
        """
        # Format expected results
        if len(expected) == 1 and len(expected[0]) == 1:
            expected_str = str(list(expected[0].values())[0])
        elif len(expected) <= 10:
            expected_str = json.dumps(expected, ensure_ascii=False, default=str)
        else:
            expected_str = json.dumps(expected[:10], ensure_ascii=False, default=str)
            expected_str += f"\n... (共 {len(expected)} 筆，僅顯示前 10 筆)"

        prompt = f"""你是一個評測裁判。請判斷「系統回答」是否正確回答了「問題」。

判斷標準：
- 比對「系統回答」和「標準答案」的語意和數值是否一致
- 數值允許微小的四捨五入差異
- 不要求格式完全一致，只要語意正確即可
- 如果標準答案是一個列表，系統回答只要包含相同的項目即可（順序不重要）
- 如果系統回答只列出部分結果但方向正確（如標準答案有 10 筆，系統回答列了前 5 筆且都正確），視為正確

問題：{question}
標準答案：{expected_str}
系統回答：{actual_answer}

只輸出純 JSON：
{{"correct": true/false, "reason": "簡短說明判斷理由"}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=self.temperature,
                messages=[{"role": "user", "content": prompt}],
            )
            content = response.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if content.startswith("```"):
                content = content.split("\n", 1)[1]
            if content.endswith("```"):
                content = content[:-3]
            return json.loads(content.strip())
        except (json.JSONDecodeError, KeyError, IndexError):
            return {"correct": False, "reason": f"parse error: {content[:200]}"}
        except Exception as e:
            return {"correct": False, "reason": f"LLM judge error: {e}"}
