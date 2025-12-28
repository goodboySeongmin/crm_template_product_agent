from __future__ import annotations

import hashlib
from typing import Dict, Any, List

from sqlalchemy import text
from sqlalchemy.orm import Session


_DUMMY_PRODUCTS = [
    {"product_id": "P001", "name": "진정 수분 크림", "deep_link": "https://example.com/p001", "category": "skincare"},
    {"product_id": "P002", "name": "저자극 클렌저", "deep_link": "https://example.com/p002", "category": "cleanser"},
    {"product_id": "P003", "name": "수분 토너", "deep_link": "https://example.com/p003", "category": "toner"},
    {"product_id": "P004", "name": "보습 세럼", "deep_link": "https://example.com/p004", "category": "serum"},
    {"product_id": "P005", "name": "선크림", "deep_link": "https://example.com/p005", "category": "suncare"},
]

class ProductCatalog:
    """
    상품 테이블이 아직 없어도 동작하도록 만든 어댑터.
    - products 테이블이 있으면 DB 기반 추천(간단 버전)
    - 없으면 더미 추천(유저 해시 기반으로 항상 동일하게)
    """

    def __init__(self, db: Session):
        self.db = db
        self._has_products = self._detect_products_table()

    def _detect_products_table(self) -> bool:
        try:
            row = self.db.execute(
                text(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                      AND table_name IN ('products','product','catalog_products')
                    """
                )
            ).mappings().first()
            return bool(row and int(row.get("cnt", 0)) > 0)
        except Exception:
            return False

    def recommend_for_user(self, user: Dict[str, Any], top_k: int = 3) -> List[Dict[str, Any]]:
        if self._has_products:
            rec = self._recommend_from_db(user, top_k=top_k)
            if rec:
                return rec
        return self._recommend_dummy(user, top_k=top_k)

    def _recommend_from_db(self, user: Dict[str, Any], top_k: int = 3) -> List[Dict[str, Any]]:
        """
        ⚠️ 실제 product schema가 확정되면 여기만 교체하면 됨.
        (가정) products(product_id, name, deep_link, category, is_active)
        """
        cat = (user.get("top_category_30d") or "").strip() or None
        try:
            rows = self.db.execute(
                text(
                    """
                    SELECT product_id, name, deep_link, category
                    FROM products
                    WHERE (:cat IS NULL OR category = :cat)
                    ORDER BY product_id DESC
                    LIMIT :k
                    """
                ),
                {"cat": cat, "k": int(top_k)},
            ).mappings().all()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def _recommend_dummy(self, user: Dict[str, Any], top_k: int = 3) -> List[Dict[str, Any]]:
        uid = str(user.get("user_id") or "unknown")
        h = int(hashlib.md5(uid.encode("utf-8")).hexdigest(), 16)
        start = h % len(_DUMMY_PRODUCTS)
        out = []
        for i in range(top_k):
            out.append(_DUMMY_PRODUCTS[(start + i) % len(_DUMMY_PRODUCTS)])
        return out
