from __future__ import annotations

from typing import TypedDict, List, Dict, Any, Optional

class ProductState(TypedDict, total = False):
    run_id: str

    # inputs (from DB/handoffs)
    brief: Dict[str, Any]
    selected_template: Dict[str, Any]
    target_audience: Dict[str, Any]

    channel: str
    campaign_goal: str
    candidate_id: Optional[str]

    # computed
    user_ids: List[str]
    users: List[Dict[str, Any]]
    recommendations: Dict[str, List[Dict[str, Any]]]

    # outputs
    send_logs: List[Dict[str, Any]]
    summary: Dict[str, Any]

    ignore_opt_in: bool
    max_preview: int
