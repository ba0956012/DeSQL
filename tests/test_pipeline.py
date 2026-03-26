"""
Pipeline 整合測試

用法：
  cd langgraph_sql_pipeline
  python -m pytest tests/test_pipeline.py -v -s
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import app
from logger import init_run_logger

import logging

_logger = logging.getLogger("pipeline")


def run_pipeline(question: str) -> dict:
    init_run_logger(question)
    result = app.invoke({"question": question, "retry": 0})
    _logger.info(f"🔍 策略: {result.get('strategy')}")
    _logger.info(f"📄 檢索: {result.get('retrieved_docs')}")
    _logger.info(f"🗄️ SQL: {result.get('sql')}")
    _logger.info(f"📊 筆數: {len(result.get('sql_result', []))}")
    _logger.info(f"💡 答案: {result.get('final_answer')}")
    if result.get("error"):
        _logger.error(f"❌ 錯誤: {result.get('error')}")
    return result


class TestRetrieval:
    def test_product_search(self):
        result = run_pipeline("iphone手機殼有哪些？")
        assert any(c.get("type") == "keyword" for c in result.get("conditions", []))
        assert result.get("final_answer") is not None

    def test_category_search(self):
        result = run_pipeline("保健類商品的平均售價是多少？")
        assert any(
            c.get("type") in ("enum", "keyword") for c in result.get("conditions", [])
        )
        assert result.get("final_answer") is not None

    def test_store_salesperson(self):
        result = run_pipeline("新店遠東的銷售員有哪些人？")
        assert result.get("final_answer") is not None


class TestPureCalculation:
    def test_daily_sales_above_avg(self):
        result = run_pipeline("這兩個月來，哪幾天的總銷售額高於平均？")
        assert not any(c.get("type") == "keyword" for c in result.get("conditions", []))
        assert result.get("final_answer") is not None

    def test_store_sales_ranking(self):
        result = run_pipeline("每家門市的總銷售額排名？")
        assert result.get("final_answer") is not None

    def test_payment_method_ratio(self):
        result = run_pipeline("信用卡和現金的付款比例各是多少？")
        assert result.get("final_answer") is not None


class TestMultiTableJoin:
    def test_salesperson_category_ranking(self):
        result = run_pipeline("哪個銷售員賣出最多筆通訊商品類的交易？")
        assert result.get("final_answer") is not None

    def test_store_comparison(self):
        result = run_pipeline("新店遠東和板橋華江的人均銷售額差多少？")
        assert result.get("final_answer") is not None


class TestTransfer:
    def test_delivery_within_2days(self):
        result = run_pipeline("有多少筆交易在2天內完成配送？")
        assert result.get("final_answer") is not None

    def test_missing_transfer_status(self):
        result = run_pipeline("哪些訂單有配送單但配送狀態是缺少資料？")
        assert result.get("final_answer") is not None


class TestEdgeCases:
    def test_whatif_delist(self):
        result = run_pipeline(
            "如果新店遠東不賣定價2000元以上的商品，該店的會員購買金額會減少多少？"
        )
        assert result.get("final_answer") is not None

    def test_return_orders(self):
        result = run_pipeline("上個月退貨最多的商品是什麼？")
        assert result.get("final_answer") is not None

    def test_null_member_ratio(self):
        result = run_pipeline("沒有會員編號的交易佔總交易的比例？")
        assert result.get("final_answer") is not None
