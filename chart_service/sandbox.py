"""
沙箱執行環境：SAFE_BUILTINS 白名單 + 受限 exec
"""

import base64
import io
import json
import uuid
from datetime import datetime, timedelta, date
from collections import Counter, defaultdict
from decimal import Decimal

# 安全的 builtins 白名單（從 nodes/code.py 移植）
SAFE_BUILTINS = {
    "len": len,
    "sum": sum,
    "min": min,
    "max": max,
    "sorted": sorted,
    "reversed": reversed,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "range": range,
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "round": round,
    "abs": abs,
    "any": any,
    "all": all,
    "isinstance": isinstance,
    "hasattr": hasattr,
    "getattr": getattr,
    "type": type,
    "print": print,
    "Counter": Counter,
    "defaultdict": defaultdict,
    "Decimal": Decimal,
    "datetime": datetime,
    "timedelta": timedelta,
    "date": date,
}


def strip_imports(code: str) -> str:
    """移除程式碼中的所有 import 與 from...import 語句。"""
    return "\n".join(
        ln for ln in code.split("\n") if not ln.strip().startswith(("import ", "from "))
    )


def execute_matplotlib_code(code: str, data: list[dict]) -> dict:
    """在受限命名空間中執行 matplotlib 程式碼，回傳 {"success", "image", "error"}。"""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    code = strip_imports(code)
    fig, ax = plt.subplots(figsize=(9, 7))
    buf = io.BytesIO()
    plt.rcParams["font.family"] = "Arial Unicode MS"

    exec_ns = {
        "__builtins__": SAFE_BUILTINS,
        "data": data,
        "fig": fig,
        "ax": ax,
        "plt": plt,
        "buf": buf,
        "matplotlib": matplotlib,
        "squarify": __import__("squarify"),
        "datetime": datetime,
        "timedelta": timedelta,
        "date": date,
    }
    try:
        exec(code, exec_ns)
        final_fig = exec_ns.get("fig", fig)
        if buf.tell() == 0:
            final_fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
        buf.seek(0)
        img_b64 = base64.b64encode(buf.read()).decode()
        plt.close("all")
        return {"success": True, "image": img_b64, "error": ""}
    except Exception as e:
        plt.close("all")
        return {"success": False, "image": "", "error": str(e)}


def execute_echarts_code(code: str, data: list[dict]) -> dict:
    """在受限命名空間中執行 pyecharts 程式碼，回傳 {"success", "option_json", "html", "error"}。"""
    import pyecharts.charts as charts
    from pyecharts import options as opts
    from pyecharts.globals import ThemeType

    code = strip_imports(code)
    exec_ns = {
        "__builtins__": SAFE_BUILTINS,
        "data": data,
        "datetime": datetime,
        "timedelta": timedelta,
        "date": date,
        "opts": opts,
        "ThemeType": ThemeType,
        "Bar": charts.Bar,
        "Pie": charts.Pie,
        "Line": charts.Line,
        "Scatter": charts.Scatter,
        "TreeMap": charts.TreeMap,
        "HeatMap": charts.HeatMap,
        "Grid": charts.Grid,
        "Tab": charts.Tab,
        "Funnel": charts.Funnel,
        "Radar": charts.Radar,
        "WordCloud": charts.WordCloud,
    }
    try:
        exec(code, exec_ns)
        chart_obj = exec_ns.get("chart")
        if chart_obj is None:
            return {
                "success": False,
                "option_json": "",
                "html": "",
                "error": "chart variable not found",
            }

        option_json = chart_obj.dump_options()
        option_json = json.dumps(json.loads(option_json), ensure_ascii=False, indent=2)

        chart_id = uuid.uuid4().hex[:12]
        width = getattr(chart_obj, "width", "800px") or "800px"
        height = getattr(chart_obj, "height", "500px") or "500px"

        html = (
            '<!DOCTYPE html>\n<html><head><meta charset="UTF-8">\n'
            '<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>\n'
            "</head><body>\n"
            f'<div id="{chart_id}" style="width:{width};height:{height};"></div>\n'
            "<script>\n"
            f"var chart = echarts.init(document.getElementById('{chart_id}'));\n"
            f"var option = {option_json};\n"
            "chart.setOption(option);\n"
            "window.addEventListener('resize', function(){ chart.resize(); });\n"
            "</script>\n</body></html>"
        )
        return {
            "success": True,
            "option_json": option_json,
            "html": html,
            "error": "",
        }
    except Exception as e:
        return {"success": False, "option_json": "", "html": "", "error": str(e)}
