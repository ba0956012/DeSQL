# Evaluation (BIRD-SQL Benchmark)

使用 [BIRD-SQL](https://bird-bench.github.io/) dev set 評測 DeSQL pipeline 的答案準確度。

## 評測方式

1. Gold SQL 在 SQLite 上執行 → expected result
2. Question 送入 DeSQL pipeline（PostgreSQL）→ answer
3. LLM-as-Judge（gpt-4.1-mini）比對兩邊結果，判斷語意是否一致
   - 數值允許微小四捨五入差異
   - 列表不要求順序一致
   - 部分正確但方向正確視為正確

## 評測結果摘要

測試範圍：3 個 BIRD 資料庫，共 259 題。模型：gpt-4.1-mini。

| Version | Overall | Schools (89) | Debit Card (64) | Financial (106) |
|---------|---------|-------------|-----------------|-----------------|
| v0.1.0 | 168/259 (64.9%) | 60 (67.4%) | 39 (60.9%) | 69 (65.1%) |
| v0.2.0 | 176/259 (67.9%) | 62 (69.7%) | 43 (67.2%) | 71 (67.0%) |
| v0.3.0 | 182/259 (70.3%) | 62 (69.7%) | 43 (67.2%) | 77 (72.6%) |

See `EXPERIMENT_LOG.md` for full experiment history.

### 各資料庫表現

| 資料庫 | 題數 | v0.1.0 | v0.2.0 | v0.3.0 |
|--------|------|--------|--------|--------|
| california_schools | 89 | 67.4% | 69.7% | 69.7% |
| financial | 106 | 65.1% | 67.0% | 72.6% |
| debit_card_specializing | 64 | 60.9% | 67.2% | 67.2% |

## Setup

### 1. 準備 BIRD 資料集

從 [BIRD-SQL GitHub](https://bird-bench.github.io/) 下載 dev set，將以下檔案放入 `eval/`：

```
eval/dev.json              # 題目（含 question, SQL, evidence, difficulty）
eval/dev.sql               # Gold SQL
eval/dev_tables.json       # Schema 定義
eval/databases/            # SQLite 資料庫（每個 DB 一個資料夾）
```

### 2. 準備環境變數

```bash
cp eval/.env.eval.example eval/.env.eval
# 填入 Azure OpenAI credentials 和 PostgreSQL 連線
```

### 3. 匯入 BIRD 資料庫到 PostgreSQL

```bash
# 測試 PG 連線
python eval/test_pg.py

# 匯入單一資料庫（會建立 bird_{db_id} database）
python eval/import_to_pg.py california_schools

# 匯入全部已下載的資料庫
python eval/import_to_pg.py
```

匯入時會自動：
- 表名和欄位名轉小寫
- 偵測日期格式欄位轉為 DATE 型別
- 建立 `bird_` 前綴的獨立 database

### 4. 執行評測

```bash
# 單題測試
python eval/run_eval.py --db california_schools --id 0

# 該 DB 全部（自動跳過已完成的題目）
python eval/run_eval.py --db california_schools --tag desql_evidence

# 附加精簡版欄位描述（推薦，透過 State 注入 SQL prompt）
python eval/run_eval.py --db california_schools --with-desc --tag desql_41mini_run2

# 把描述放在 question 裡
python eval/run_eval.py --db california_schools --with-desc --desc-in-question --tag desql_41mini_desc_in_q

# 不使用 evidence hint
python eval/run_eval.py --db california_schools --no-evidence --tag desql_no_evidence
```

### 5. 查看結果

```bash
# 統計所有實驗的準確率
python eval/summary.py

# 只看特定 DB
python eval/summary.py --db california_schools

# 比較特定實驗
python eval/summary.py desql_41mini_run2 desql_evidence_compactdesc
```

### 6. 生成報告

```bash
# 單一 DB 報告（含圖表、統計顯著性分析）
python eval/generate_report.py --db california_schools

# 整合報告（所有已測試的 DB）
python eval/generate_report.py --db all

# 指定實驗
python eval/generate_report.py --db california_schools --tags desql_41mini_run2 desql_evidence_compactdesc

# 轉獨立 HTML
python eval/generate_html.py eval/report/california_schools/REPORT.md
```

## 檔案說明

| 檔案 | 說明 |
|------|------|
| `run_eval.py` | DeSQL pipeline 評測腳本 |
| `import_to_pg.py` | SQLite → PostgreSQL 匯入工具 |
| `summary.py` | 準確率統計（支援多組比較） |
| `generate_report.py` | 生成 Markdown 報告（含圖表、統計顯著性） |
| `generate_html.py` | Markdown → 獨立 HTML 轉換 |
| `rerun_sql.py` | 重新執行指定題目的 SQL |
| `test_pg.py` | PostgreSQL 連線測試 |
| `report_template.md` | 報告模板 |
| `databases/*/description_compact.txt` | 精簡版欄位描述（由 Claude Opus 4.6 從 BIRD CSV 精簡而成） |
| `databases/*/dataset_description.md` | 資料集說明 |
| `databases/*/evidence_examples.md` | Evidence hint 範例 |

## 評測參數

| 參數 | 說明 | 預設 |
|------|------|------|
| `--db` | BIRD 資料庫名稱 | california_schools |
| `--id` | 單題模式：指定 question_id | - |
| `--limit` | 批次模式：最多跑幾題 | 全部 |
| `--tag` | 實驗標籤，結果存入 `results/{tag}/` | - |
| `--with-desc` | 附加精簡版欄位描述 | false |
| `--desc-in-question` | 描述放在 question 裡（搭配 --with-desc） | false |
| `--no-evidence` | 不使用 evidence hint | false |

## 已測試的資料庫

| db_id | 題數 | 說明 |
|-------|------|------|
| california_schools | 89 | 加州學校資料（3 表） |
| financial | 106 | 捷克銀行金融資料（8 表） |
| debit_card_specializing | 64 | 加油站消費資料（4 表） |
