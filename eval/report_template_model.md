# DeSQL Model Comparison Report

## 1. 評測目的

比較不同 LLM 模型在 DeSQL pipeline 上的表現差異，確認模型選擇對準確度的影響。

### 評測流程
1. 使用相同的 DeSQL pipeline（SQL + Python 架構）
2. 只替換底層 LLM 模型，其他配置完全相同
3. 在 BIRD-SQL benchmark 上評測，使用 LLM-as-Judge 比對答案

### 評測配置
- Pipeline：DeSQL（evidence + compact desc via State）
- 資料集：BIRD-SQL dev set（california_schools 89 題 + financial 106 題 + debit_card_specializing 64 題 = 259 題）
- Judge：gpt-4.1-mini（固定，不隨測試模型變動）

## 2. 比較模型

| 模型 | 說明 |
|------|------|
| gpt-4.1-mini | OpenAI GPT-4.1 系列的輕量版本 |
| gpt-5.4-mini | OpenAI GPT-5.4 系列的輕量版本，預設 reasoning_effort=none |
| gpt-5.4-mini (reasoning=medium) | 同上，但明確啟用 reasoning_effort=medium |

注意：gpt-5.4-mini 的 `reasoning_effort` 預設為 `none`，需要明確設定才會啟用推理能力。reasoning 會消耗額外的 token（reasoning tokens），增加延遲和成本。
