"""
領域特定的 SQL 生成規則

把業務邏輯從 prompt 中抽離，方便不同專案自訂。
留空 list 即為通用模式，不注入任何領域規則。
"""

DOMAIN_SQL_RULES = [
    "注意區分「商品定價」（product.price）和「交易總額」（pos_sale.total_amount）",
    "如果問題涉及「會員」，記得篩選 member_id IS NOT NULL",
]
