#!/usr/bin/env python3
"""
呼叫 Chart Service API，把回傳的圖表寫入 test-chart.html 並自動開啟瀏覽器。

用法：
    python chart_service/test_api.py
    python chart_service/test_api.py --url http://your-ecs-host:8081
    python chart_service/test_api.py --engine matplotlib --chart-type bar
    python chart_service/test_api.py --question "月銷售趨勢" --engine echarts --chart-type line
"""

import argparse
import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import requests

SCRIPT_DIR = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "test-chart.html"

SAMPLE_DATA = [
    {"category": "電子產品", "sales": 15000},
    {"category": "服飾", "sales": 8500},
    {"category": "食品", "sales": 12000},
    {"category": "家具", "sales": 6000},
    {"category": "日用品", "sales": 9200},
]


def build_html(chart_type: str, reason: str, chart_html: str, chart_image: str) -> str:
    if chart_html:
        # echarts: 用 iframe 嵌入
        body = f'<iframe srcdoc="{chart_html.replace(chr(34), "&quot;")}" style="width:100%;height:600px;border:none;"></iframe>'
    elif chart_image:
        # matplotlib: base64 PNG
        body = f'<img src="data:image/png;base64,{chart_image}" style="max-width:100%;">'
    else:
        body = '<p style="color:#999;padding:40px;text-align:center;">LLM 判斷不需要圖表</p>'

    return f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8">
<title>Chart API 測試結果</title>
<style>
  body {{ font-family: -apple-system, sans-serif; max-width: 1000px; margin: 40px auto; padding: 0 20px; }}
  .meta {{ color: #666; font-size: 14px; margin: 6px 0; }}
  .chart {{ margin-top: 16px; border: 1px solid #ddd; border-radius: 4px; }}
</style>
</head>
<body>
<h1>📊 Chart API 測試結果</h1>
<p class="meta">Chart Type: <strong>{chart_type}</strong></p>
<p class="meta">Reason: {reason or '(無)'}</p>
<div class="chart">{body}</div>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(description="呼叫 Chart Service API 並產生 test-chart.html")
    parser.add_argument("--url", default="http://aiot-chart-service-alb-247680518.ap-northeast-1.elb.amazonaws.com", help="Chart Service base URL")
    parser.add_argument("--engine", default="echarts", choices=["echarts", "matplotlib"])
    parser.add_argument("--chart-type", default="auto")
    parser.add_argument("--question", default="各類別銷售佔比")
    parser.add_argument("--data", default=None, help="JSON 檔案路徑或 JSON 字串")
    args = parser.parse_args()

    # 載入資料
    if args.data:
        if os.path.isfile(args.data):
            with open(args.data) as f:
                data = json.load(f)
        else:
            data = json.loads(args.data)
    else:
        data = SAMPLE_DATA

    payload = {
        "data": data,
        "question": args.question,
        "engine": args.engine,
        "chart_type": args.chart_type,
    }

    print(f"🚀 呼叫 {args.url}/chart/generate")
    print(f"   engine={args.engine}, chart_type={args.chart_type}")
    print(f"   question={args.question}")
    print(f"   data: {len(data)} 筆")

    resp = requests.post(f"{args.url}/chart/generate", json=payload, timeout=60)
    if resp.status_code != 200:
        print(f"❌ 錯誤 {resp.status_code}: {resp.text}")
        sys.exit(1)

    result = resp.json()
    print(f"✅ chart_type={result['chart_type']}, reason={result.get('reason', '')}")

    html = build_html(
        chart_type=result["chart_type"],
        reason=result.get("reason", ""),
        chart_html=result.get("chart_html", ""),
        chart_image=result.get("chart_image", ""),
    )

    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"📄 已寫入 {OUTPUT_HTML}")

    # macOS 自動開啟瀏覽器
    subprocess.run(["open", str(OUTPUT_HTML)])


if __name__ == "__main__":
    main()
