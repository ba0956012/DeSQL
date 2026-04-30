# BIRD-SQL Eval Service API 使用範例

> Base URL: `http://localhost:8080`（本地）或你的 ECS 服務 URL

## 1. 健康檢查

```bash
curl http://localhost:8080/health
```

回應：
```json
{"status": "ok", "version": "0.1.0"}
```

## 2. 取得資料庫描述

```bash
curl http://localhost:8080/databases/california_schools/description
```

回應：
```json
{
  "db_id": "california_schools",
  "original_desc": "使用 BIRD-SQL dev set 中的 california_schools 子集...",
  "compact_desc": "-- JOIN keys: frpm.cdscode = schools.cdscode = satscores.cds..."
}
```

## 3. 查詢題目數量

```bash
curl http://localhost:8080/databases/california_schools/count
```

回應：
```json
{
  "db_id": "california_schools",
  "total": 89,
  "by_difficulty": {"simple": 54, "moderate": 30, "challenging": 5}
}
```

## 4. 取得單題

```bash
curl http://localhost:8080/databases/california_schools/questions/0
```

回應：
```json
{
  "question_id": 0,
  "db_id": "california_schools",
  "question": "What is the highest eligible free rate for K-12 students in the schools in Alameda County?",
  "evidence": "Eligible free rate for K-12 = `Free Meal Count (K-12)` / `Enrollment (K-12)`",
  "difficulty": "simple"
}
```

## 5. 取得全部提示

```bash
curl http://localhost:8080/databases/financial/hints
```

回應：
```json
{
  "db_id": "financial",
  "total": 106,
  "hints": [
    {
      "question_id": 89,
      "question": "How many accounts who choose issuance after transaction are staying in East Bohemia?",
      "evidence": "Issuance after transaction refers to frequency = 'POPLATEK PO OBRATU'; East Bohemia refers to A3 = 'east Bohemia'"
    },
    ...
  ]
}
```

## 6. 單題測試（評判答案）

```bash
curl -X POST http://localhost:8080/databases/california_schools/questions/0/evaluate \
  -H "Content-Type: application/json" \
  -d '{"answer": "The highest eligible free rate for K-12 students in Alameda County is 1.0"}'
```

回應：
```json
{
  "question_id": 0,
  "db_id": "california_schools",
  "correct": true,
  "reason": "系統回答的數值 1.0 與標準答案一致"
}
```

## Python 範例

```python
import requests

BASE_URL = "http://localhost:8080"

# 查看有哪些 DB 可用（透過 count 端點）
for db in ["california_schools", "financial", "debit_card_specializing"]:
    resp = requests.get(f"{BASE_URL}/databases/{db}/count")
    if resp.status_code == 200:
        data = resp.json()
        print(f"{db}: {data['total']} 題")

# 取得題目並提交答案
q = requests.get(f"{BASE_URL}/databases/california_schools/questions/0").json()
print(f"題目: {q['question']}")
print(f"提示: {q['evidence']}")

result = requests.post(
    f"{BASE_URL}/databases/california_schools/questions/0/evaluate",
    json={"answer": "The highest eligible free rate is 1.0"}
).json()
print(f"結果: {'✅ 正確' if result['correct'] else '❌ 錯誤'}")
print(f"理由: {result['reason']}")
```

## 錯誤回應範例

DB 不存在：
```bash
curl http://localhost:8080/databases/nonexistent/count
# 404: {"detail": "Database 'nonexistent' not found"}
```

題目不存在：
```bash
curl http://localhost:8080/databases/california_schools/questions/9999
# 404: {"detail": "Question 9999 not found in database 'california_schools'"}
```

空答案：
```bash
curl -X POST http://localhost:8080/databases/california_schools/questions/0/evaluate \
  -H "Content-Type: application/json" \
  -d '{"answer": ""}'
# 422: Pydantic validation error
```
