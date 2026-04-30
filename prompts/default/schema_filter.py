"""Schema filter node prompt templates — default (中文, gpt-4.1-mini)"""


def build_schema_filter_prompt(question: str, table: str, desc_text: str) -> str:
    return f"""根據使用者的問題，從以下表的欄位中找出「確定不需要」的欄位。

規則：
- 只排除你非常確定跟問題無關的欄位
- 如果有任何可能用到，就不要排除
- 不要排除 ID 欄位、key 欄位、或可能用於 JOIN 的欄位
- 不要排除名稱或描述跟問題有任何關聯的欄位
- 不確定的就保留（不要列入 exclude）

使用者問題：{question}

表 {table} 的欄位：
{desc_text}

只輸出純 JSON（不要 markdown）：
{{"exclude": ["col1", "col2", ...]}}
如果沒有需要排除的，輸出：{{"exclude": []}}"""


def build_desc_filter_prompt(question: str, compact_desc: str) -> str:
    return f"""根據使用者的問題，從以下 schema 備註中挑出相關的行。

規則：
- 一定要保留問題涉及的表的 JOIN key
- 只保留和問題相關的欄位說明
- 只保留問題提到的值或類別的對應關係
- 移除不相關的表或欄位的說明
- 保持 -- 前綴格式，直接輸出選中的行（不要 JSON、不要 markdown）
- 如果都不相關，只輸出 JOIN key

使用者問題：{question}

Schema 備註：
{compact_desc}

只輸出相關的行："""
