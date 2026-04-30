# 優化實驗記錄

## Baseline
- Tag: `desql_41mini_cn_validate`
- Model: gpt-4.1-mini
- Overall: 168/259 (64.9%)
- california_schools: 60/89 (67.4%)
- debit_card_specializing: 39/64 (60.9%)
- financial: 69/106 (65.1%)

---

## 實驗 1: Hint Rewriter
- 日期: 2026-04-08
- 改動: 在 pipeline 前加一個 LLM node，根據 schema + compact_desc 改寫 hint
- 抽樣結果 (30 題): A=22/30, B=18/30, 翻正 1, 翻錯 5, 淨 -4
- 結論: **有害**，改寫 hint 會過度解讀，導致原本正確的題目變錯
- 狀態: ❌ 放棄

## 實驗 2: Validate Prompt V2（激進版）
- 日期: 2026-04-08
- 改動: validate_sql_result prompt 從「欄位檢查」升級為「全面合理性檢查」（5 項檢查）
- 完整 DB 結果:
  - debit_card: 39/64 → 39/64 (持平，moderate -2)
  - financial: 69/106 → 67/106 (-2，challenging -2)
- 結論: **有害**，太激進導致不必要的重試，把正確答案改壞
- 狀態: ❌ 放棄

## 實驗 3: Validate Prompt V2（保守版）
- 日期: 2026-04-08
- 改動: 只檢查 Hint 公式遺漏和關鍵篩選遺漏，加「寧可放過」原則
- 分層抽樣 seed=42: 翻正 7, 翻錯 0 → 但完整 DB 無法復現
- 分層抽樣 seed=99: 翻正 2, 翻錯 2, 淨 0
- 完整 DB: 持平
- Log 分析: validate 全部判 sufficient=True，等於沒有觸發
- 結論: **無效**，保守到完全不觸發
- 狀態: ❌ 保留環境變數切換機制 (VALIDATE_PROMPT=v2)，但預設用 legacy

## 實驗 4: Validate V2 + 資料科學家角色 + reasoning-first
- 日期: 2026-04-08
- 改動: 角色改為「基礎資料科學家」，JSON 輸出 reasoning 在前 sufficient 在後
- 分層抽樣 seed=123: 翻正 3, 翻錯 1, 淨 +2
- Log 分析: validate 仍然全部判 sufficient=True，翻正是 LLM 隨機性
- 結論: **無效**，validate node 在 gpt-4.1-mini 上無法可靠判斷 SQL 邏輯正確性
- 狀態: ❌ 保留但不啟用

## 實驗 5: SQL 生成規則改動
- 日期: 2026-04-08
- 改動: _BASE_SQL_RULES 從「SQL 只取資料」改為「根據問題複雜度動態選擇策略」
  - 簡單題 → SQL 直接用 GROUP BY/ORDER BY/LIMIT 算出答案
  - 複雜題 → SQL 取資料讓 Python 處理
  - 加「比例/百分比要取分子分母兩邊」
  - 加「WHERE 寧鬆勿嚴」
  - 移除「不要加 LIMIT」
- 分層抽樣 seed=200: A=15/30, B=18/30, 翻正 4, 翻錯 1, 淨 +3
- 完整 DB (debit_card): 39/64 → 38/64 (-1，moderate -1)
- 結論: 抽樣 +3 但完整 DB 微退，改動效果被 LLM 隨機性淹沒
- 狀態: ⚠️ 保留改動（不退回），但效果不確定，需要更多 DB 驗證

---

## 錯誤分析 (baseline 91 題錯題)

### 最終答案錯誤分類
| 類型 | 數量 | 佔比 |
|------|------|------|
| 數值/計算結果錯誤 | 55 | 60% |
| 查無資料/無法回答 | 18 | 20% |
| 回答不完整 | 12 | 13% |
| 選錯實體/欄位 | 4 | 4% |
| Pipeline 崩潰 | 2 | 2% |

### SQL vs Python 歸因 (10 題 wrong_val 抽樣)
| 歸因 | 數量 | 佔比 |
|------|------|------|
| SQL 取錯資料 | 7 | 70% |
| Python 後處理出錯 | 3 | 30% |

---

## 實驗 6: gpt-5.4-mini + SQL rules v2
- 日期: 2026-04-08
- 改動: 換模型為 gpt-5.4-mini（reasoning model），搭配新 SQL rules
- 結果:
  - debit_card: 39/64 → 43/64 (+6.3%) ✅
  - california_schools: 60/89 → 54/89 (-6.7%) ❌
  - financial: 69/106 → 67/106 (-1.9%) ❌
  - 合計: 168/259 (64.9%) → 164/259 (63.3%), p=0.70 不顯著
- 結論: 5.4-mini 整體不優於 4.1-mini，且成本更高（reasoning model）
- 狀態: ❌ 不採用，切回 4.1-mini

---

## 待測方向
1. 換模型 — gpt-4.1（非 mini）
2. Self-correction 迴路 — Python 結果 sanity check
3. QA Node V2 — 加入預期結果格式分析
4. 混合模型策略 — 不同 node 用不同模型

---

## 實驗 7: Schema Linking Node
- 日期: 2026-04-09
- 改動: 在 retrieval 和 generate_sql 之間加入 schema_linking node，用 LLM 篩選相關表/欄位
- A/B 抽樣 seed=300 (42 題): 翻正 7, 翻錯 2, 淨 +5
- A/B 抽樣 seed=400 (42 題，含動態 CSV desc): 翻正 4, 翻錯 4, 淨 0
- 完整 DB (debit_card): 39/64 → 38/64 (-1)
- 結論: 抽樣結果不穩定，完整 DB 持平。小 schema DB 上 schema linking 價值有限
- 狀態: ❌ 回退

## 實驗 8: Desc 嵌入 DDL
- 日期: 2026-04-09
- 改動: 從 BIRD CSV 讀取欄位描述，嵌入 DDL 的每個欄位行後面作為 SQL 註解
- 修復: load_full_description 的 BOM bug（utf-8-sig）
- A/B 抽樣 seed=500 (42 題): 翻正 6, 翻錯 5, 淨 +1
- 9 題硬題 spot test (4.1-mini): 0/9 改善
- 9 題硬題 spot test (5.4-mini + desc): 4/9 改善（#174, #136, #145, #1511）
- 結論: desc 嵌入對 4.1-mini 效果不穩定，對 5.4-mini 有幫助但 5.4 整體不優於 4.1
- 狀態: ⚠️ 保留機制（--full-desc 參數），但不預設啟用

## 實驗 9: FK/PK Constraints
- 日期: 2026-04-09
- 改動: 修正 import_to_pg.py，從 SQLite 讀取 FK 定義，在 PG 建立 PK + FK constraints
- DDL 自動包含 FK 資訊（如 FOREIGN KEY(account_id) REFERENCES account(account_id)）
- 完整 DB 結果 (4.1-mini):
  - california_schools: 60/89 → 54/89 (-6, 翻正 5 翻錯 11)
  - debit_card: 39/64 → 39/64 (持平, 翻正 5 翻錯 5)
  - financial: 69/106 → 71/106 (+2, 翻正 10 翻錯 8)
  - 合計: 168/259 → 164/259 (-4), McNemar chi2=0.205 **不顯著**
- 結論: FK 在 financial（多表複雜 JOIN）上有正面效果，但在 california_schools 上退化。整體不顯著
- 狀態: ⚠️ 保留 FK（正確的 DB metadata），但不能期待顯著提升

## 實驗 10: CoT SQL Prompt
- 日期: 2026-04-09
- 改動: generate_sql prompt 改為 5 步驟思考（理解問題→選表→WHERE→SELECT→自我檢查）
- 結論: 和其他改動一起測試，無法單獨評估效果
- 狀態: ⚠️ 保留

## 實驗 11: gpt-5.4-mini + FK + Desc
- 日期: 2026-04-09
- 改動: 5.4-mini + FK constraints + desc 嵌入 DDL
- 完整 DB (financial): 66/106 (62.3%), baseline 69/106 (65.1%), **退 3 題**
- 翻錯分析 (14 題): FORMAT 問題 3 題 (摘要化), CALC 問題 6 題, SQL/OTHER 5 題
- 結論: 5.4-mini 的回答風格太摘要化，不列具體數值，導致 judge 判錯
- 狀態: ❌ 不採用

## 實驗 12: Format Answer V2
- 日期: 2026-04-09
- 改動: format_answer prompt 改為「必須列出具體數值」「至少列出前 10 筆」，截斷限制 500→2000
- 完整 DB (financial, 4.1-mini + FK): 62/106 (58.5%), 比 FK 版 71/106 退 9 題
- 結論: **有害**，新 prompt 讓 LLM 花太多 token 列舉，反而影響準確性
- 狀態: ❌ 回退

## 實驗 13: Question Analysis Node
- 日期: 2026-04-09
- 改動: 在 retrieval 後加入 question_analysis node，結構化分解問題（NER-like）
  - 提取 target、entities、filters、computation、tables_needed、join_path
  - task_plan 傳遞給 generate_sql 和 generate_code
- 9 題硬題 spot test (4.1-mini, 無 desc): 2/9 改善（#174 ✅, #145 ✅）
- 9 題硬題 spot test (5.4-mini, 無 desc): 0/9
- 9 題硬題 spot test (4.1-mini + desc): #174 ✅, 其他持平
- 完整 DB (financial, 4.1-mini + FK + desc): 前 32 題 22/32 (68.8%), baseline 同題 23/32 (71.9%), -1
- Log 分析發現:
  - QA 分析通常正確，但 SQL 生成不一定遵循 task_plan
  - 部分錯誤來自 gold SQL 語意歧義（如 #107 的 client.district_id vs account.district_id）
  - 部分錯誤來自 LLM judge 隨機性（同樣答案不同次判定不同）
  - QA 缺少「預期結果格式」分析（如 #101 應回傳 1 筆但回傳 315 筆）
- 結論: QA node 在特定題目上有幫助（#174 的 account_id vs client_id），但整體持平
- 狀態: ⚠️ 保留但需要進一步優化

---

## 深度錯誤分析 (baseline 91 題)

### SQL 層面錯誤分類
| 類型 | 數量 | 佔比 |
|------|------|------|
| SQL 邏輯/計算差異 | 61 | 67% |
| 缺少必要的表 | 21 | 23% |
| 空結果/查無 | 7 | 8% |
| Pipeline 崩潰 | 2 | 2% |

### 關鍵發現
1. **LLM 隨機性是主要噪音源**: 259 題只能可靠偵測 ≥10% 的差異，3-5% 的改動被淹沒
2. **Gold SQL 語意歧義**: 部分題目的 gold SQL 邏輯和自然語言表述不一致（如 #107, #150, #136）
3. **LLM Judge 隨機性**: 同樣的答案，judge 有時判對有時判錯（如 #101）
4. **67% 的錯誤是 SQL 邏輯差異**: 表和欄位選對了，但 WHERE/聚合/排序邏輯不同
5. **Desc/FK 等資訊注入效果有限**: 因為大部分錯誤不是「不知道欄位語意」而是「邏輯推理不同」
6. **5.4-mini 不優於 4.1-mini**: reasoning model 在 format_answer 上反而退化（摘要化）

## 實驗 14: Pipeline v5b — QA 隔離 + Bug Fixes
- 日期: 2026-04-09
- Tag: `pipeline_v5b_financial`, `pipeline_v5b_schools`, `pipeline_v5b_debit`
- Model: gpt-4.1-mini + FK + full-desc
- 改動:
  1. **方向 A（QA 隔離）**: generate_sql 有 task_plan 時不傳原始問題，只看 QA 的 sql_task 指令
     - 解決 QA 和 generate_sql 之間的指令衝突（generate_sql 看到原始問題會自行加 WHERE 條件，忽略 QA 的分工）
     - 方向 B（QA 當參考，generate_sql 仍看原始問題）spot test 效果差，放棄
  2. **datetime 正規化**: `_normalize_dates()` 把 SQL 回傳的 `datetime.datetime` 統一轉成 `datetime.date`
     - 修復 Python code 中 `datetime` vs `date` 型別比較錯誤
  3. **generate_code prompt**: 明確告知日期欄位是 `date` 物件，不要呼叫 `.date()` 方法
  4. **`__import__` sandbox fix**: `SAFE_BUILTINS` 加入 `_safe_import`，允許 datetime/time 等內部依賴，阻擋其他模組
  5. **`strip_code_fences` 改進**: 截斷 code 後面的 markdown 說明（LLM 有時在 code 後加 ``` 和解釋）
- 完整 DB 結果:
  - financial: 69/106 → 73/106 (+4, 翻正 13 翻錯 9)
  - california_schools: 60/89 → 58/89 (-2, 翻正 7 翻錯 9)
  - debit_card: 39/64 → 40/64 (+1, 翻正 6 翻錯 5)
  - **合計: 168/259 (64.9%) → 171/259 (66.0%), 淨 +3**
- 按難度:
  - financial: simple 74.2%→79.0%, moderate 48.6%→56.8%, challenging 71.4%→42.9%
  - california_schools: simple 70.4%→74.1%, moderate 60.0%→50.0%, challenging 80.0%→60.0%
  - debit_card: simple 62.8%→72.1%, moderate 64.7%→47.1%, challenging 25.0%→25.0%
- 分析:
  - simple 題目全面提升（financial +5%, schools +4%, debit +9%）
  - moderate 題目全面退化（financial -8%, schools -10%, debit -18%）
  - QA 隔離對簡單題有效（SQL 更忠實遵循 QA 的取資料指令），但對複雜題可能過度簡化
  - california_schools 退化主要來自欄位選錯（gsoffered vs GSserved）和地址格式差異，非 QA 隔離直接導致
- 結論: 整體 +3 題，simple 提升明顯，但 moderate 退化需要關注。Bug fixes（datetime、__import__、strip_code_fences）是確定有益的改動
- 狀態: ⚠️ 保留，但 moderate 退化需要進一步分析

### 待測方向
1. 移除 generate_code prompt 中的 chart_data 要求（減少 LLM 注意力分散）
2. QA node 對簡單題不啟用（只對 moderate/challenging 啟用）
3. generate_sql 對 moderate 題保留原始問題（混合策略）

## 實驗 15: Pipeline v5c — Format Answer 改進
- 日期: 2026-04-09
- Tag: `pipeline_v5c_financial`, `pipeline_v5c_schools`, `pipeline_v5c_debit`
- Model: gpt-4.1-mini + FK + full-desc
- 基於 v5b 的所有改動，額外加上:
  1. **format_answer prompt 改進**: 移除「摘要說明重點即可」，改為「必須保留具體數值」「不要用模糊描述」「列表至少列前 20 筆」
  2. **截斷限制放寬**: final_answer 截斷從 500 → 2000 字元，讓 LLM 看到更多具體數值
- 完整 DB 結果:
  - financial: 69/106 → 71/106 (+2, 翻正 10 翻錯 8)
  - california_schools: 60/89 → 62/89 (+2, 翻正 11 翻錯 9)
  - debit_card: 39/64 → 43/64 (+4, 翻正 5 翻錯 1)
  - **合計: 168/259 (64.9%) → 176/259 (67.9%), 淨 +8**
- 與 v5b 比較:
  - financial: 73 → 71 (-2, LLM 隨機性)
  - california_schools: 58 → 62 (+4, format 改進明顯)
  - debit_card: 40 → 43 (+3, format 改進明顯)
  - 合計: 171 → 176 (+5)
- 結論: **目前最佳結果**。三個 DB 全部正向提升。format_answer 改進在 schools 和 debit 上效果明顯（+4, +3），financial 因 LLM 隨機性微退但仍優於 baseline。
- 狀態: ✅ 採用為新基準

### Pipeline v5c 改動總結（相對於 baseline）
1. QA node（question_analysis）: 結構化分解問題，生成 task_plan
2. QA 隔離: generate_sql 有 task_plan 時不傳原始問題
3. datetime 正規化: SQL 回傳的 datetime 統一轉 date
4. generate_code prompt: 明確告知日期是 date 物件
5. __import__ sandbox: _safe_import 允許內部依賴，阻擋外部模組
6. strip_code_fences: 截斷 code 後的 markdown 說明
7. format_answer: 保留具體數值，不摘要化，截斷 2000 字元
8. validate_sql_result: pass through（不啟用）
9. FK/PK constraints: import_to_pg 建立 FK
10. full-desc: BIRD CSV 欄位描述嵌入 DDL

### 錯誤歸因分析 (v5b 20 題抽樣)
| 歸因 | 數量 | 佔比 |
|------|------|------|
| QA 選錯表/欄位/JOIN 路徑 | 8 | 40% |
| QA 理解問題/計算邏輯錯 | 3 | 15% |
| Python 聚合/計算邏輯不同 | 3 | 15% |
| Format 摘要化 | 2 | 10% |
| SQL 日期/值不匹配 | 2 | 10% |
| Judge 隨機性 | 1 | 5% |
| SQL WHERE 條件差異 | 1 | 5% |

### 待優化方向
1. **QA node 優化**（55% 錯誤歸因）: 減少多餘 JOIN、改進欄位選擇
2. **移除 generate_code 的 chart_data 要求**: 減少 LLM 注意力分散
3. **Python code 去重問題**: LLM 傾向自作主張去重，但 gold SQL 不一定去重

## 實驗 16-20: 後續優化嘗試（v5c 之後）

### 實驗 16: QA 計算精確性 prompt
- 改動: QA prompt 加 calculation 欄位，要求用數學公式描述計算邏輯
- 結果: 翻正 2/6 錯題，翻錯 2/10 對題 → 有害，回退
- 原因: QA 變得太積極，在 sql_task 裡多加篩選條件

### 實驗 17: QA 用 gpt-5.4-mini
- 改動: QA node 用獨立的 5.4-mini，其他 node 用 4.1-mini
- 結果: 翻正 2/7 錯題，翻錯 2/10 對題 → 持平
- 原因: 5.4-mini 過度推理（"total price" → amount × price）

### 實驗 18: 全 pipeline gpt-5.4-mini
- 改動: 所有 node 用 5.4-mini（在 v5c pipeline 上）
- 結果: 翻正 8/20 錯題，翻錯 4/20 對題，淨 +4
- 分析: 比之前的實驗 6 好很多（v5c 的 format 改進修掉了 5.4 的摘要化問題）
- 狀態: ⚠️ 有潛力但需要完整 DB 驗證

### 實驗 19: Retrieval hints 注入 QA
- 改動: 把 retrieval subgraph 的 conditions（表/欄位對應）注入 QA prompt
- 結果: 翻正 2/6 特定錯題，翻錯 3/28 對題（LLM 隨機性），隨機錯題 0/15 翻正
- 結論: 對特定題目有效但整體效果不顯著，回退

### 實驗 20: QA prompt 結構改動 + 欄位過濾
- v5d: QA prompt 加任務說明、改 JSON 格式
  - schools: 57/89 (-5 vs v5c) ❌
  - financial: 73/106 (+2 vs v5c) ✅
  - 結論: 不穩定，回退
- v5e: 加欄位過濾（>15 欄的表根據關鍵字過濾不相關欄位）
  - financial: 75/106 (+4 vs v5c) ✅
  - schools: 61/89 (-1 vs v5c) ≈ 持平
  - debit_card: 39/64 (-4 vs v5c) ❌（非過濾導致，是 prompt 改動）
  - 合計: 175/259 (-1 vs v5c)
  - 分析: 欄位過濾在 financial 上有效，但關鍵字匹配太粗糙（cdscode 等 PK 被過濾掉）
  - 狀態: ⚠️ 方向正確但實作需要改進

### 待優化方向（下次繼續）
1. **改進版欄位過濾**: 用 column_descs 做更智能的過濾，有 desc 的欄位都保留，只 skip 沒有 desc 且不匹配的欄位。過濾後的 schema 傳給所有後續 node
2. **5.4-mini 完整 DB 驗證**: v5c pipeline + 5.4-mini 的 spot test 結果正面（淨 +4/40），值得跑完整 DB
3. **混合模型策略**: QA 用 4.1-mini，generate_sql 用 5.4-mini（或反過來）

## 實驗 21: Pipeline v5g — LLM Schema Filter + CoT Code + Format 改進
- 日期: 2026-04-14
- Tag: `pipeline_v5g_schools`, `pipeline_v5g_financial`, `pipeline_v5g_debit`
- Model: gpt-4.1-mini + FK + full-desc
- 基於 v5c 的所有改動，額外加上:
  1. **LLM-based schema filter**: 新增 `nodes/schema_filter.py`，在 retrieval 後、QA 前，用 LLM 根據問題和欄位描述篩選大表（>15 欄）的相關欄位。保留 PK/FK + LLM 選的 + retrieval 發現的欄位。過濾後的 DDL 傳給 QA 和 SQL node
  2. **generate_code CoT prompt**: 要求 LLM 先分析 SQL 結果狀態（是否已聚合），再寫 code。python_task 保留但 LLM 以實際 SQL 結果為準
  3. **format_answer 短答案優化**: 短答案（數值或 ≤50 字元）走獨立 prompt，強調「分析結果就是答案本身」，避免 LLM 誤解數值（如 5.0 → 「5筆」）
  4. **chart_data 條件化**: ENABLE_CHART=false 時不在 generate_code prompt 中要求 chart_data
  5. **strip_code_fences 改進**: 支援「分析文字 + code block」格式，用 regex 提取最後一個 code block
- 完整 DB 結果:
  - california_schools: 62/89 → 64/89 (+2, 翻正 9 翻錯 7)
  - financial: 71/106 → 75/106 (+4, 翻正 5 翻錯 1)
  - debit_card: 43/64 → 42/64 (-1, 翻正 3 翻錯 4)
  - **合計: 176/259 (67.9%) → 181/259 (69.9%), 淨 +5**
- 按難度:
  - schools: simple 75.9%→77.8%, moderate 60.0%→60.0%, challenging 60.0%→80.0%
  - financial: simple 79.0%→82.3%, moderate 51.4%→56.8%, challenging 42.9%→42.9%
  - debit: simple 69.8%→72.1%, moderate 70.6%→52.9%, challenging 25.0%→50.0%
- 分析:
  - LLM schema filter 在大表 DB 上效果明顯（schools +2, financial +4）
  - CoT prompt 對 debit 的簡單聚合題有負面影響（AVG 被誤解為先 group by 再平均）
  - debit 的表都 ≤9 欄，schema filter 完全不觸發，-1 純粹是 CoT + LLM 隨機性
  - format_answer 短答案改進修復了數值誤解問題（如 #73 的 5.0）
  - 各 DB McNemar 檢定均不顯著，但整體趨勢正面
- 結論: 70% 在無 Self-Consistency 的單次推理下接近極限
- 狀態: ✅ 採用為新基準

### 實驗 21b: CoT vs NoCot 對比
- CoT（先分析再寫 code）vs NoCot（直接寫 code，保留「不要重複聚合」提示）
- CoT 結果: 181/259 (69.9%) — schools 64, financial 75, debit 42
- NoCot 結果: 182/259 (70.3%) — schools 62, financial 77, debit 43
- 分析: CoT 在 debit 的簡單聚合題上有負面影響（AVG 被誤解為先 group by 再平均），增加 output token 但無顯著提升
- 結論: **NoCot 更優**，更簡單、省 token、效果相當或更好。採用 NoCot 作為最終 v5g

### Pipeline v5g 最終改動（相對於 v5c）
1. LLM schema filter（nodes/schema_filter.py）: 大表（>15 欄）欄位智能過濾
2. generate_code prompt: 加「不要重複 SQL 已做的聚合」提示（無 CoT）
3. format_answer 短答案: 數值/短文字走獨立 prompt，避免誤解
4. chart_data 條件化: ENABLE_CHART=false 時省略
5. strip_code_fences: 支援混合格式（regex 提取最後一個 code block）

### 各版本準確率對照
| 版本 | Overall | Schools (89) | Debit (64) | Financial (106) |
|------|---------|-------------|-----------|----------------|
| v0.1.0 (baseline) | 168/259 (64.9%) | 60 (67.4%) | 39 (60.9%) | 69 (65.1%) |
| v0.2.0 (v5c) | 176/259 (67.9%) | 62 (69.7%) | 43 (67.2%) | 71 (67.0%) |
| v5f (rule filter) | 178/259 (68.7%) | 61 (68.5%) | 41 (64.1%) | 76 (71.7%) |
| v5g CoT | 181/259 (69.9%) | 64 (71.9%) | 42 (65.6%) | 75 (70.8%) |
| v5g NoCot | 182/259 (70.3%) | 62 (69.7%) | 43 (67.2%) | 77 (72.6%) |


---

## 實驗: validate_sql_result (2026-04-22)

### 背景
- Bugfix baseline (commit 8b780c7): 185/259 (71.4%), 0 crashes
- 目標: 在 SQL 執行後、code 生成前，用 LLM 檢查 SQL 結果是否足夠回答問題

### 嘗試的 prompt 版本

| 版本 | 分數 | Precision | Retries | 說明 |
|---|---|---|---|---|
| v1 (3 checks) | 183 | 33% | ~28 | MISSING_COLUMN, WRONG_TABLE, MISSING_JOIN |
| v2 (6 checks) | 181 | ? | ~30 | +TIME_FILTER, AGGREGATION, ROW_COUNT |
| v3 (3+context) | 179 | 33% | ~30 | 加了 Python 會處理的說明 |
| v4 (WRONG_SUBSET) | 177 | 39% | ~28 | 加了 WRONG_SUBSET 檢查 |
| v5 (+data profile) | 179 | 39% | ~28 | 加了每欄位 distinct count |
| v6 (CoT) | 188, 178 | 57% | ~30 | chain-of-thought 推論 |
| v7 (CoT+step4) | 184 | 52% | ~27 | 加了保守判斷步驟 |
| v8 (constraint) | 186 | 50% | ~22 | 約束條件檢查 |
| v10 (task_plan compare) | 189, 185, 184 | 65%/50% | ~17-24 | 用完整 task_plan 對比 |
| v11 (positive reasoning) | N/A (log-only) | 35% | 40 flagged | 正向推論「能算出什麼」— 太嚴格 |

### 關鍵發現
1. **false positive 是核心問題**: LLM 把正確的 SQL 判成 insufficient，觸發不必要的 retry
2. **SQL retry 方向不可控**: 即使 validator 判斷正確，SQL LLM 的修正也常常是錯的
3. **task_plan 對比最有效**: 給 validator 完整的 QA task_plan 讓它對比規劃 vs 實際，precision 最高 (65%)
4. **正向推論太嚴格**: 讓 LLM 推論「能算出什麼」反而讓它過度懷疑 (precision 35%)
5. **log-only 分析**: task_plan 版本 log-only 跑出 10 flagged, 6 TP, 4 FP (precision 60%)
   - 4 個 FP 都是「欄位存在但 validator 不理解 Python 會過濾/聚合」
   - 6 個 TP 都很精準（缺少百分比分母、錯誤的 k_symbol 欄位等）

### Stash 記錄
- stash@{0}: validate_sql_result task_plan compare (log-only mode)
- stash@{1}: validate_sql_result experiments (v1-v9 simulation)
- stash@{2}: all experiments: empty retry + WHERE diag + typehint + crash fix
- stash@{3}: empty replan + diagnosis experiment

### 下一步方向
- 需要降低 false positive: 讓 validator 只在「欄位完全缺失」時 flag
- 考慮不觸發 retry，而是把 validation 結果傳給 code LLM 作為參考


---

## 實驗: 穩定性分析 + Prompt 微調 + 規則驗證 (2026-04-24)

### 背景
- Baseline: test_bugfix (commit 8b780c7), 185/259 (71.4%)
- Model: qwen3-next-80b (QA/main) + qwen3-coder-30b (SQL/Code), via Bedrock
- 目標: 分析錯誤 pattern，嘗試提升準確率

### 穩定性分析 (5 次 filterval 實驗)
- 5 次結果: 190, 188, 183, 189, 183，平均 186.6 (72.0%)
- 分佈:
  - 總是對 (0/5 錯): 151 題 (58.3%)
  - 偶爾錯 (1-2/5): 36 題 (13.9%)
  - 容易錯 (3-4/5): 31 題 (12.0%)
  - 總是錯 (5/5): 41 題 (15.8%)
- Oracle (5次任一對): 218/259 (84.2%)
- Self-Consistency@3 預估: 187.2/259 (72.3%) — 只比單次多 ~1 題
- 結論: SC 效果有限，因為分佈兩極化（穩定對 vs 穩定錯）

### 41 題總是錯的分類
| 類型 | 題數 | 佔比 |
|------|------|------|
| WRONG_LOGIC | 20 | 49% |
| MISSING_TABLE | 11 | 27% |
| MISSING_AGGREGATION | 6 | 15% |
| EMPTY/NO_ANSWER | 3 | 7% |
| FEWER_JOINS | 1 | 2% |

### 容易錯 (3-4/5) 的差異來源
| 差異來源 | 題數 | 佔比 |
|----------|------|------|
| DIFF_SQL_WHERE | 29 | 74% |
| DIFF_SQL_TABLES | 5 | 13% |
| 其他 | 5 | 13% |
- 結論: 74% 的不穩定來自 SQL WHERE 條件差異（LLM 隨機性）

### 嘗試的 Prompt 改動（全部在噪音範圍內或有害）

| 改動 | Tag | 結果 | 結論 |
|------|-----|------|------|
| grouped_count type | test_typedesc | 184 (-1) | 中性，#172 翻正但整體持平 |
| filter source 引用 | test_filtersrc | ~185 | 中性 |
| free type 描述 | test_freetype | 179 (-6) | ❌ 有害 |
| WHERE 出處註解 | test_wheresrc×3 | 189,190,183 avg 187 | 中性偏正 |
| unique columns hint | test_uniqcol | 179 (-6) | ❌ 有害（干擾 LLM） |
| SQL 聚合策略明確化 | test_sqlagg | 177 (-8) | ❌ 有害（SQL 聚合錯了 Python 沒法補） |
| duplicate warning | test_dupwarn | 184 (-1) | 中性，保留（零成本） |

### ✅ 規則驗證 validate_sql_result（確認保留）
- 改動: 用純規則（非 LLM）檢查 SQL 是否包含 task_plan 要求的所有表
  - 從 task_plan 取 tables_needed
  - 從 SQL 文字解析實際用了哪些表（regex: FROM/JOIN）
  - 缺表 → 觸發 SQL retry，告訴 SQL LLM 要加哪些表
- Tag: test_rulesql, test_rulesql2, test_rulesql3
- 結果: 190, 183, 190，平均 187.7 (72.5%)
- vs baseline 185: 淨 +5, +(-2), +5
- 觸發率: 5-6/259 (~2%)
- Precision: 50-67%（3-4/5-6 次有效）
- 特性: 零 LLM 成本，只在 SQL 確實缺表時觸發
- 結論: **保留** — 唯一穩定正向的改動

### 保留的改動總結
1. **validate_sql_result 規則驗證**: 比較 task_plan.tables_needed vs SQL 實際用的表，缺表觸發 retry
2. **duplicate warning in _profile_data**: 在 code prompt 中明確警告重複行（零成本，中性）
3. **FK path completion**: code 保留但 disabled（eval 未顯著改善）
4. **stability_analysis.py**: 新增穩定性分析工具

### 關鍵發現
1. Prompt 微調已到極限 — 加更多規則/資訊反而分散 LLM 注意力
2. 確定性規則（非 LLM）比 LLM 驗證更可靠（precision 更高，零成本）
3. 74% 的不穩定來自 SQL WHERE 條件差異 — 需要 self-consistency 或更強模型
4. 41 題總是錯中 11 題是 MISSING_TABLE — 規則驗證能部分修復
5. LLM 隨機性 ±4 題，需要 ≥5 題的穩定提升才能確認改動有效


## 實驗: SQL 例子引導 + 聚合分析 (2026-04-24 續)

### SQL 複雜度分析
Pipeline SQL vs Gold SQL 的聚合使用率（259 題）：
| 特性 | Pipeline | Gold | 比率 |
|------|---------|------|------|
| subqueries | 2 | 16 | 0.12 |
| GROUP BY | 10 | 34 | 0.29 |
| ORDER BY | 8 | 79 | 0.10 |
| LIMIT | 6 | 77 | 0.08 |
| COUNT() | 9 | 83 | 0.11 |
| SUM() | 7 | 89 | 0.08 |
- 結論: Pipeline SQL 幾乎不用聚合，把太多工作丟給 Python

### 嘗試的改動

| 改動 | Tag | 結果 | 結論 |
|------|-----|------|------|
| SQL 允許所有聚合 (sqlflex) | test_sqlflex | 172 (-18) | ❌ 大退，SQL 聚合錯了 Python 沒法補 |
| SQL 例子引導 OK/NOT OK (sqlex) | test_sqlex×3 | 190,184,187 avg 187 | ✅ 中性偏正，GROUP BY 6/10→6/10 但更精準 |
| Code aggregation warning | test_aggwarn×2 | 189,183 avg 186 | 中性，跟 python_task 衝突 |
| QA python_task 多步聚合提示 | test_qagrp | 180 (-10) | ❌ 有害，加提示分散注意力 |

### SQL 例子引導效果
- GROUP BY 使用: rulesql 10 題 → sqlex 10 題（數量不變但正確率 4/10→6/10）
- ORDER BY+LIMIT: 5 → 7 題
- 例子引導比直接列允許的函數名更安全

### 多步聚合問題分析
11 題 gold 有 GROUP BY 但 pipeline 沒有且答錯：
- #1472: Python 取單筆 min 而不是先按客戶加總再取 min
- #1498: Python 取單筆 max 而不是先按月份加總再取 max
- #30: Python 沒有 GROUP BY city 加總 enrollment
- 根因: QA 的 python_task 只描述了最後一步（取 min/max），漏掉了第一步（先分組加總）
- aggregation warning 在 code 層跟 python_task 衝突（資訊矛盾），LLM 傾向聽 python_task
- QA prompt 加多步聚合提示也有害（分散注意力）

### 最終保留的改動（相對於 filterval baseline）
1. **validate_sql_result 規則驗證**: 比較 task_plan.tables_needed vs SQL 實際用的表
2. **_profile_data duplicate warning**: 確定性計數提示
3. **SQL task section 例子引導**: OK/NOT OK 例子（COUNT, GROUP BY, ORDER BY+LIMIT vs subquery, CASE WHEN）
4. **stability_analysis.py**: 穩定性分析工具
5. FK path completion / unique columns hint: code 保留但 disabled

### 各版本準確率對照（含今天的實驗）
| 版本 | 平均 | 範圍 | 說明 |
|------|------|------|------|
| filterval baseline | 186.6 (72.0%) | 183-190 | 無改動 |
| rulesql (+ 規則驗證) | 186.6 (72.0%) | 183-190 | + validate_sql_result |
| sqlex (+ 例子引導) | 187.0 (72.2%) | 184-190 | + SQL OK/NOT OK 例子 |
| sqlflex (允許所有聚合) | 172 (66.4%) | - | ❌ 有害 |
| aggwarn (聚合警告) | 186 (71.8%) | 183-189 | 中性 |
| qagrp (QA 多步聚合) | 180 (69.5%) | - | ❌ 有害 |

### 關鍵發現
1. **SQL 聚合是雙刃劍**: 對的時候更準確，錯的時候完全沒救（Python 看到已聚合的數字無法補救）
2. **例子引導比規則列表更安全**: OK/NOT OK 格式讓 LLM 知道邊界，不會過度使用
3. **code 層的提示不能跟 QA 的 python_task 衝突**: aggregation warning 被 python_task 覆蓋
4. **多步聚合問題的根因在 QA**: python_task 不夠精確，但在 QA prompt 加提示反而有害
5. **10 次實驗 37 題全錯是硬天花板**: 需要架構改動（self-consistency、更強模型）才能突破
