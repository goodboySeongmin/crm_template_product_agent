from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List, Optional

from langgraph.graph import StateGraph, END
from sqlalchemy import text, bindparam

from crm_agent.db.engine import SessionLocal
from crm_agent.db.repo import Repo
from crm_agent.product_agent.state import ProductState
from crm_agent.product_agent.services.slot_fill import extract_slots, fill_slots
from crm_agent.product_agent.services.rules import validate_message
from crm_agent.product_agent.services.product_catalog import ProductCatalog

# handoff stages (Template Agent가 이미 쓰는 것과 맞춤)
ST_BRIEF = "BRIEF"
ST_TARGET_AUDIENCE = "TARGET_AUDIENCE"
ST_SELECTED_TEMPLATE = "SELECTED_TEMPLATE"
ST_EXECUTION_RESULT = "EXECUTION_RESULT"

# 디버깅/요약용(새 stage)
ST_PRODUCT_AGENT_RESULT = "PRODUCT_AGENT_RESULT"

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _repo():
    db = SessionLocal()
    return Repo(db)

def _close(repo: Repo):
    try:
        repo.db.close()
    except Exception:
        pass

def node_load_context(state: ProductState) -> ProductState:
    repo = _repo()
    try:
        run_id = state["run_id"]
        run = repo.get_run(run_id)
        if not run:
            raise RuntimeError(f"run not found: {run_id}")

        brief = (run.get("brief_json") or {})
        h_brief = repo.get_latest_handoff(run_id, ST_BRIEF)
        if h_brief:
            brief = h_brief["payload_json"] or brief

        h_sel = repo.get_latest_handoff(run_id, ST_SELECTED_TEMPLATE)
        if not h_sel:
            raise RuntimeError("SELECTED_TEMPLATE가 없습니다. Step3에서 확정 후 진행하세요.")
        selected = h_sel["payload_json"] or {}

        h_aud = repo.get_latest_handoff(run_id, ST_TARGET_AUDIENCE)
        if not h_aud:
            raise RuntimeError("TARGET_AUDIENCE가 없습니다. Step2에서 타겟 생성 후 진행하세요.")
        target_audience = h_aud["payload_json"] or {}

        channel = (run.get("channel") or brief.get("channel_hint") or "SMS").upper()
        campaign_goal = str(brief.get("campaign_goal") or "").strip() or "unknown_goal"

        candidate_id = selected.get("template_id") or selected.get("candidate_id")
        if isinstance(candidate_id, str):
            candidate_id = candidate_id[:16]
        else:
            candidate_id = None

        user_ids = target_audience.get("user_ids") or []
        if not isinstance(user_ids, list):
            user_ids = []

        return {
            **state,
            "brief": brief,
            "selected_template": selected,
            "target_audience": target_audience,
            "channel": channel,
            "campaign_goal": campaign_goal,
            "candidate_id": candidate_id,
            "user_ids": user_ids,
        }
    finally:
        _close(repo)

def node_load_users(state: ProductState) -> ProductState:
    repo = _repo()
    try:
        db = repo.db
        user_ids = state.get("user_ids") or []
        if not user_ids:
            return {**state, "users": []}

        q = text(
            """
            SELECT
              u.user_id, u.customer_name, u.gender, u.birth_year, u.region,
              u.preferred_channel, u.sms_opt_in, u.kakao_opt_in, u.push_opt_in, u.email_opt_in,
              uf.skin_type, uf.skin_concern_primary, uf.sensitivity_level, uf.top_category_30d,
              uf.last_browse_at, uf.last_purchase_at
            FROM users u
            LEFT JOIN user_features uf ON uf.user_id = u.user_id
            WHERE u.user_id IN :ids
            """
        ).bindparams(bindparam("ids", expanding=True))

        rows = db.execute(q, {"ids": user_ids}).mappings().all()
        users = [dict(r) for r in rows]

        return {**state, "users": users}
    finally:
        _close(repo)

def _opt_in_ok(user: Dict[str, Any], channel: str) -> bool:
    ch = (channel or "").upper()
    if ch == "SMS":
        return int(user.get("sms_opt_in") or 0) == 1
    if ch == "KAKAO":
        return int(user.get("kakao_opt_in") or 0) == 1
    if ch == "PUSH":
        return int(user.get("push_opt_in") or 0) == 1
    if ch == "EMAIL":
        return int(user.get("email_opt_in") or 0) == 1
    return True

def _default_offer(campaign_goal: str) -> str:
    g = (campaign_goal or "").lower()
    if "browse" in g:
        return "관심 상품을 다시 확인해보세요."
    if "cart" in g:
        return "장바구니에 담긴 상품이 기다리고 있어요."
    return "지금 확인해보세요."

def _default_cta(channel: str) -> str:
    ch = (channel or "").upper()
    if ch == "SMS":
        return "다시 보기"
    if ch == "KAKAO":
        return "자세히 보기"
    if ch == "PUSH":
        return "확인하기"
    return "바로가기"

def _default_unsub(channel: str) -> str:
    # 실제 운영 값은 정책/벤더에 맞춰 교체
    ch = (channel or "").upper()
    if ch == "SMS":
        return "수신거부: 설정>알림"
    if ch == "EMAIL":
        return "Unsubscribe"
    return ""

def node_recommend_products(state: ProductState) -> ProductState:
    repo = _repo()
    try:
        db = repo.db
        users = state.get("users") or []
        top_k = int(state.get("top_k_products") or 3)
        catalog = ProductCatalog(db)

        recs: Dict[str, List[Dict[str, Any]]] = {}
        for u in users:
            recs[str(u["user_id"])] = catalog.recommend_for_user(u, top_k=top_k)

        return {**state, "recommendations": recs}
    finally:
        _close(repo)

def node_render_and_write(state: ProductState) -> ProductState:
    repo = _repo()
    try:
        db = repo.db
        run_id = state["run_id"]
        channel = state.get("channel") or "SMS"
        campaign_goal = state.get("campaign_goal") or "unknown_goal"
        selected = state.get("selected_template") or {}
        candidate_id = state.get("candidate_id")

        body = selected.get("body_with_slots") or selected.get("body") or ""

        users = state.get("users") or []
        recs = state.get("recommendations") or {}

        send_logs: List[Dict[str, Any]] = []
        fail_count = 0
        skip_count = 0

        for u in users:
            uid = str(u.get("user_id"))

            # ✅ opt-in 여부는 체크하되, 렌더링을 막지 않기 위해 변수로만 둔다
            opt_ok = _opt_in_ok(u, channel)

            products = recs.get(uid) or []
            p0 = products[0] if products else {}

            values = {
                "customer_name": (u.get("customer_name") or "고객님"),
                "product_name": (p0.get("name") or ""),
                "deep_link": (p0.get("deep_link") or ""),
                "offer": _default_offer(campaign_goal),
                "cta": _default_cta(channel),
                "unsubscribe": _default_unsub(channel),
            }

            # ✅ 렌더링(슬롯 채움)은 무조건 수행
            rendered = fill_slots(body, values, keep_unknown=True).strip()

            # ✅ 룰 체크는 그대로
            status, reasons = validate_message(rendered, channel)

            if status == "FAIL":
                fail_count += 1
                send_logs.append({
                    "run_id": run_id,
                    "user_id": uid,
                    "campaign_goal": campaign_goal,
                    "channel": channel,
                    "step_id": "S1",
                    "candidate_id": candidate_id,
                    "status": "FAILED",
                    "rendered_text": rendered,  # FAIL이어도 완성 문장 확인 가능
                    "error_code": "RULE_FAIL",
                    "error_message": "; ".join(reasons)[:255],
                })
                continue

            # ✅ 여기서 opt-in이 true면 실제 발송 payload(CREATED)
            # ✅ opt-in이 false면 미리보기 payload(PREVIEW)로 저장 (rendered_text 포함)
            if opt_ok:
                send_logs.append({
                    "run_id": run_id,
                    "user_id": uid,
                    "campaign_goal": campaign_goal,
                    "channel": channel,
                    "step_id": "S1",
                    "candidate_id": candidate_id,
                    "status": "CREATED",
                    "rendered_text": rendered,
                    "error_code": None,
                    "error_message": None,
                })
            else:
                # opt-in이 false여도 "완성 텍스트를 보고싶다" 목적을 위해 PREVIEW로 남김
                skip_count += 1
                send_logs.append({
                    "run_id": run_id,
                    "user_id": uid,
                    "campaign_goal": campaign_goal,
                    "channel": channel,
                    "step_id": "S1",
                    "candidate_id": candidate_id,
                    "status": "PREVIEW",
                    "rendered_text": rendered,
                    "error_code": "OPT_OUT_PREVIEW",
                    "error_message": "preview generated although opt-in is false",
                })


        # ✅ 테스트 반복 시 중복 방지용: 같은 run_id의 기존 로그 제거
        # 운영에서 '이력 보존'이 필요하면 이 줄을 주석 처리해.
        db.execute(text("DELETE FROM campaign_send_logs WHERE run_id = :run_id"), {"run_id": run_id})

        if send_logs:
            insert_sql = text(
                """
                INSERT INTO campaign_send_logs
                (run_id, user_id, campaign_goal, channel, step_id, candidate_id, status, rendered_text, error_code, error_message, created_at)
                VALUES
                (:run_id, :user_id, :campaign_goal, :channel, :step_id, :candidate_id, :status, :rendered_text, :error_code, :error_message, :created_at)
                """
            )
            now = _now()
            for row in send_logs:
                row["created_at"] = now
            db.execute(insert_sql, send_logs)
            db.commit()

        max_preview = int(state.get("max_preview") or 5)
        preview_texts = []
        for x in send_logs:
            if x.get("status") == "CREATED" and x.get("rendered_text"):
                preview_texts.append(x["rendered_text"])
            if len(preview_texts) >= max_preview:
                wWbreak


        summary = {
            "run_id": run_id,
            "channel": channel,
            "campaign_goal": campaign_goal,
            "template_id": selected.get("template_id"),
            "total_users_in": len(users),
            "logs_written": len(send_logs),
            "failed": fail_count,
            "skipped": skip_count,
            "sample": preview_texts,
            "created_at": _now(),
        }

        # Template Agent와 stage name 맞춰서 저장
        repo.create_handoff(run_id, ST_EXECUTION_RESULT, {
            "final_message_preview": summary["sample"],
            "used_template_id": summary.get("template_id"),
            "note": "Per-user send logs are stored in campaign_send_logs.",
        })
        repo.create_handoff(run_id, ST_PRODUCT_AGENT_RESULT, summary)

        try:
            repo.update_run(run_id, step_id="S6_EXEC", status="EXECUTED")
        except Exception:
            pass

        return {**state, "send_logs": send_logs, "summary": summary}
    finally:
        _close(repo)

def build_product_graph():
    g = StateGraph(ProductState)
    g.add_node("load_context", node_load_context)
    g.add_node("load_users", node_load_users)
    g.add_node("recommend_products", node_recommend_products)
    g.add_node("render_and_write", node_render_and_write)

    g.set_entry_point("load_context")
    g.add_edge("load_context", "load_users")
    g.add_edge("load_users", "recommend_products")
    g.add_edge("recommend_products", "render_and_write")
    g.add_edge("render_and_write", END)
    return g.compile()

GRAPH = build_product_graph()

def run_product_agent(run_id: str, top_k_products: int = 3, ignore_opt_in: bool = True, max_preview: int = 5) -> Dict[str, Any]:
    init: ProductState = {
        "run_id": run_id,
        "top_k_products": int(top_k_products),
        "ignore_opt_in": bool(ignore_opt_in),
        "max_preview": int(max_preview),
    }
    return GRAPH.invoke(init)

