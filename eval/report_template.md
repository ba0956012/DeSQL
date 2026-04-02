# DeSQL Evaluation Report — {db_id}

## 1. 評測方法

### 評測流程
1. 從 BIRD-SQL benchmark 取得問題和標準 SQL
2. 標準 SQL 在 SQLite 上執行，得到 expected result
3. 問題丟進待測系統（DeSQL pipeline 或 Greg-T2S API），得到系統回答
4. LLM-as-Judge 比對系統回答與標準答案的語意一致性

### 評測標準
- 數值允許微小的四捨五入差異
- 列表類答案不要求順序一致
- 部分正確但方向正確的回答視為正確
- 兩個系統的回答都先轉為自然語言，再由 LLM（gpt-4.1-mini）作為裁判，比對系統回答與標準答案的語意是否一致，判斷是否正確回答了問題

### 使用模型
所有系統統一使用 Azure OpenAI `gpt-4.1-mini` 作為 LLM，LLM Judge 也使用同一模型。

## 2. 資料集

{dataset_description}

### Evidence Hint

BIRD 資料集中每題附帶一個 `evidence` 欄位，提供領域知識提示，幫助模型理解題目中的專有名詞或計算方式。評測時以 `(Hint: ...)` 的形式附加在問題後面。

{evidence_examples}

## 3. 比較策略

### 待測系統

| 系統 | 說明 |
|------|------|
| DeSQL | SQL + Python 雙層架構，SQL 取資料、Python 做邏輯運算 |
| Greg-Text-to-SQL | 外部 Text-to-SQL 服務，透過 API 呼叫取得查詢結果 |

### DeSQL 配置變體

| 配置 | 說明 |
|------|------|
| desc via State | evidence + 精簡版欄位描述注入 SQL 生成 prompt（DeSQL 最佳配置） |
| desc in question | evidence + 精簡版欄位描述接在問題後面（與 Greg-T2S 公平比較用） |

### Greg-T2S 配置變體

| 配置 | 說明 |
|------|------|
| +compact desc | evidence + 精簡版欄位描述接在問題後面 |

### Schema Description 處理

BIRD 資料集為每張表提供了 CSV 格式的欄位描述（`database_description/*.csv`），包含欄位名、說明、值域等。原始描述冗長且包含大量對 LLM 無用的資訊。

由 Claude Opus 4.6 精簡為 compact description，只保留三類關鍵資訊：
1. JOIN key 對應關係（最常導致 SQL 錯誤的地方）
2. 欄位值編碼（如 `charter: 1=Yes, 0=No`、`status: 'A'=finished OK`）
3. 計算公式（如 `eligible free rate = free meal count / enrollment`）

此精簡版 description 同時用於 DeSQL 和 Greg-T2S 的 `+compact desc` 配置。DeSQL 將其注入 SQL 生成的 prompt，Greg-T2S 將其接在問題後面。

### 注入方式

DeSQL：compact description 作為「欄位說明」區塊注入 SQL 生成的 prompt，位於 schema 和 enum 資訊之後、問題之前。不經過 retrieval subgraph，避免干擾條件分析。

Greg-T2S：compact description 直接接在問題後面，作為額外上下文一起送入 API。

Greg-T2S 實際送入的問題格式範例：
```
What is the highest eligible free rate for K-12 students in the schools in Alameda County?
(Hint: Eligible free rate for K-12 = `Free Meal Count (K-12)` / `Enrollment (K-12)`)

Column descriptions:
-- JOIN keys: frpm.cdscode = schools.cdscode = satscores.cds
-- frpm: "charter school (y/n)": 1=Yes, 0=No
-- frpm: eligible free rate = free meal count / enrollment
-- satscores: excellence rate = numge1500 / numtsttakr
...
```
