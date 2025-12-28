from __future__ import annotations

from typing import List, Tuple

# MVP용 기본 규칙(팀 회의에서 '공통제약' 합의되면 여기로 옮기면 됨)
CHANNEL_MAX_LEN = {
    "SMS": 140,
    "KAKAO": 900,
    "PUSH": 180,
    "EMAIL": 2000,
}

BANNED_PHRASES = [
    "완치",
    "100% 효과",
    "부작용 없음",
]

def validate_message(text: str, channel: str) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    t = (text or "").strip()

    max_len = CHANNEL_MAX_LEN.get((channel or "").upper())
    if max_len and len(t) > max_len:
        reasons.append(f"길이 초과: {len(t)} > {max_len} (channel={channel})")

    for p in BANNED_PHRASES:
        if p in t:
            reasons.append(f"금칙 표현 포함: {p}")

    status = "PASS" if not reasons else "FAIL"
    return status, reasons
