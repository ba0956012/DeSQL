# Chart Service API 使用範例

> Base URL: `http://localhost:8081`（本地）或你的 ECS 服務 URL

## 1. 健康檢查

```bash
curl http://localhost:8081/health
```

回應：
```json
{"status": "ok", "version": "0.1.0"}
```

## 2. 自動判斷圖表類型（ECharts）

```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"category": "電子產品", "sales": 15000},
      {"category": "服飾", "sales": 8500},
      {"category": "食品", "sales": 12000},
      {"category": "家具", "sales": 6000}
    ],
    "question": "各類別銷售佔比",
    "engine": "echarts",
    "chart_type": "auto"
  }'
```

回應：
```json
{
  "chart_type": "pie",
  "reason": "使用者要分析佔比，適合用圓餅圖",
  "chart_html": "<!DOCTYPE html>...<script>var chart = echarts.init(...)...</script>...",
  "chart_option": "{\"series\": [...]}",
  "chart_image": ""
}
```

## 3. 指定 bar chart（Matplotlib）

```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"store": "台北店", "revenue": 250000},
      {"store": "台中店", "revenue": 180000},
      {"store": "高雄店", "revenue": 210000}
    ],
    "question": "每間門市的總銷售額比較",
    "engine": "matplotlib",
    "chart_type": "bar"
  }'
```

回應：
```json
{
  "chart_type": "bar",
  "reason": "",
  "chart_html": "",
  "chart_option": "",
  "chart_image": "iVBORw0KGgoAAAANSUhEUgAA..."
}
```

## 4. 表格模式（ECharts）

```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"name": "Alice", "department": "Engineering", "salary": 90000},
      {"name": "Bob", "department": "Marketing", "salary": 75000},
      {"name": "Carol", "department": "Engineering", "salary": 95000}
    ],
    "question": "列出所有員工資料",
    "engine": "echarts",
    "chart_type": "table"
  }'
```

回應：
```json
{
  "chart_type": "table",
  "reason": "",
  "chart_html": "<div style=\"font-family: Arial...\"><table>...</table></div>",
  "chart_option": "",
  "chart_image": ""
}
```

## 5. 表格模式（Matplotlib PNG）

```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"name": "Alice", "score": 90},
      {"name": "Bob", "score": 85}
    ],
    "question": "成績表",
    "engine": "matplotlib",
    "chart_type": "table"
  }'
```

回應：
```json
{
  "chart_type": "table",
  "reason": "",
  "chart_html": "",
  "chart_option": "",
  "chart_image": "iVBORw0KGgoAAAANSUhEUgAA..."
}
```

## Python 範例

```python
import requests

BASE_URL = "http://localhost:8081"

# ECharts 互動式圖表
resp = requests.post(f"{BASE_URL}/chart/generate", json={
    "data": [
        {"month": "1月", "sales": 100},
        {"month": "2月", "sales": 150},
        {"month": "3月", "sales": 130},
    ],
    "question": "月銷售趨勢",
    "engine": "echarts",
    "chart_type": "line",
})
result = resp.json()

# 把 HTML 存成檔案，瀏覽器開啟即可看到互動式圖表
with open("chart.html", "w") as f:
    f.write(result["chart_html"])

# Matplotlib 靜態圖片
import base64
resp = requests.post(f"{BASE_URL}/chart/generate", json={
    "data": [
        {"product": "A", "count": 50},
        {"product": "B", "count": 30},
    ],
    "question": "產品數量比較",
    "engine": "matplotlib",
    "chart_type": "bar",
})
result = resp.json()

# 把 base64 存成 PNG
with open("chart.png", "wb") as f:
    f.write(base64.b64decode(result["chart_image"]))
```

## 錯誤回應範例

空資料：
```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{"data": [], "question": "test", "engine": "echarts"}'
# 422: data 至少需要 1 筆
```

空白問題：
```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{"data": [{"a":1}], "question": "   ", "engine": "echarts"}'
# 422: question 不可為空白
```

無效引擎：
```bash
curl -X POST http://localhost:8081/chart/generate \
  -H "Content-Type: application/json" \
  -d '{"data": [{"a":1}], "question": "test", "engine": "plotly"}'
# 422: engine 必須是 echarts 或 matplotlib
```
