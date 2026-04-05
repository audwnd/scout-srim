# -*- coding: utf-8 -*-
"""
event_watcher.py - 긍정/부정 공시 이벤트 감지
KRX KIND todaydisclosure.do API 사용
최근 7일 공시를 날짜별로 수집하여 종목코드 → 이벤트 매핑
"""

import json, requests
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

BASE      = Path(__file__).parent
CACHE_DIR = BASE / "WORK"
CACHE_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer":    "https://kind.krx.co.kr",
}

# ── 긍정 공시 키워드 ─────────────────────────────────────
# {키워드: (등급, 설명)}
POSITIVE_EVENTS = {
    # ★★★ 직접 주주가치 상승
    "자기주식소각결정":   ("★★★", "자사주 소각 → 주당가치 즉시 상승"),
    "자기주식소각":       ("★★★", "자사주 소각 → 주당가치 상승"),
    "자기주식취득결정":   ("★★★", "자사주 매입 → 수급 개선"),
    "무상증자결정":       ("★★★", "무상증자 → 유동성 확대"),
    "주식분할결정":       ("★★",  "주식분할 → 유동성 확대"),
    "현금배당결정":       ("★★",  "배당 결정 → 주주환원"),
    "주식배당결정":       ("★★",  "주식배당 → 주주환원"),
    # ★★ 성장/실적
    "신규시설투자등":     ("★★",  "대규모 설비투자 → 성장 기대"),
    "주요계약체결":       ("★★",  "주요 계약 체결 → 매출 증가"),
    "기술이전계약":       ("★★",  "기술이전 계약 → 수익 창출"),
    "공급계약":           ("★★",  "공급계약 체결 → 매출 증가"),
    "잠정실적":           ("★★",  "실적공시 (서프라이즈 가능)"),
    "영업(잠정)실적":     ("★★",  "잠정 실적 공시"),
    "수주":               ("★★",  "수주 공시 → 매출 증가"),
    "제3자배정":          ("★★",  "전략적 투자자 유입"),
    # ★ 구조적 변화
    "합병결정":           ("★",   "합병 → 규모 확대"),
    "영업양수결정":       ("★",   "사업 인수"),
    "기업가치 제고":      ("★",   "기업가치 제고 계획 발표"),
    "전략적투자":         ("★",   "전략적 투자 유치"),
}

# ── 부정 공시 키워드 ─────────────────────────────────────
NEGATIVE_EVENTS = {
    "자기주식처분":       ("⚠",   "자사주 매도 → 수급 부담"),
    "유상증자결정":       ("⚠",   "유상증자 → 주식 희석"),
    "불성실공시":         ("⛔",  "불성실공시 → 신뢰도 하락"),
    "상장폐지":           ("⛔",  "상장폐지 위험"),
    "관리종목":           ("⛔",  "관리종목 지정"),
    "횡령":               ("⛔",  "횡령/배임 의혹"),
    "배임":               ("⛔",  "횡령/배임 의혹"),
    "소수계좌매도":       ("⛔",  "소수 계좌 집중 매도 경보"),
    "감사의견거절":       ("⛔",  "감사의견 거절"),
    "한정의견":           ("⛔",  "감사 한정의견"),
    "거래정지":           ("⛔",  "거래정지"),
}


def _get_biz_dates(days: int = 7) -> list:
    """최근 N 영업일 날짜 목록 반환 (YYYYMMDD 형식)"""
    result = []
    d = datetime.now()
    while len(result) < days:
        if d.weekday() < 5:  # 월~금
            result.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return result


def _fetch_day(date_str: str) -> list:
    """특정 날짜의 공시 목록 수집"""
    events = []
    try:
        r = requests.post(
            "https://kind.krx.co.kr/disclosure/todaydisclosure.do",
            headers=HEADERS,
            data={
                "method":          "searchTodayDisclosureSub",
                "currentPageSize": "200",
                "pageIndex":       "1",
                "orderMode":       "0",
                "orderStat":       "D",
                "forward":         "todaydisclosure_sub",
                "disclosureType":  "",
                "searchDt":        date_str,
            },
            timeout=10
        )
        soup = BeautifulSoup(r.text, "html.parser")

        for tr in soup.select("table tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 3: continue

            title    = tds[2].get_text(strip=True)
            corp_td  = tds[1]
            corp_nm  = corp_td.get_text(strip=True)
            link     = corp_td.find("a", href=True)
            code     = ""
            if link:
                href = link.get("href", "")
                if "gicode=A" in href:
                    code = href.split("gicode=A")[-1][:6]

            # 긍정 키워드 매칭
            for kw, (grade, desc) in POSITIVE_EVENTS.items():
                if kw in title:
                    events.append({
                        "code": code, "name": corp_nm,
                        "title": title, "grade": grade,
                        "desc": desc, "type": "positive",
                        "date": date_str,
                    })
                    break

            # 부정 키워드 매칭
            for kw, (grade, desc) in NEGATIVE_EVENTS.items():
                if kw in title:
                    events.append({
                        "code": code, "name": corp_nm,
                        "title": title, "grade": grade,
                        "desc": desc, "type": "negative",
                        "date": date_str,
                    })
                    break

    except Exception as e:
        print(f"  [공시 조회 오류] {date_str}: {e}")

    return events


def fetch_events(days: int = 7) -> list:
    """최근 N 영업일 긍정/부정 공시 수집"""
    dates     = _get_biz_dates(days)
    all_events= []

    print(f"  공시 조회: {dates[-1]} ~ {dates[0]} ({len(dates)}일)")
    for d in dates:
        day_ev = _fetch_day(d)
        all_events.extend(day_ev)

    pos = sum(1 for e in all_events if e["type"] == "positive")
    neg = sum(1 for e in all_events if e["type"] == "negative")
    print(f"  긍정 공시: {pos}건 / 부정 공시: {neg}건")
    return all_events


def get_event_map(days: int = 7, force: bool = False) -> dict:
    """
    종목코드 → 이벤트 목록 딕셔너리
    캐시: 4시간 (장중 변화 반영)
    """
    cache_f = CACHE_DIR / "events.json"

    if not force:
        try:
            if cache_f.exists():
                c   = json.loads(cache_f.read_text(encoding="utf-8"))
                age = datetime.now() - datetime.fromisoformat(c.get("cached_at", "2000-01-01"))
                if age.total_seconds() < 4 * 3600:
                    return c.get("data", {})
        except Exception:
            pass

    events = fetch_events(days)
    ev_map = {}
    for e in events:
        code = e.get("code", "")
        if code:
            ev_map.setdefault(code, []).append(e)

    cache_f.write_text(
        json.dumps({"cached_at": datetime.now().isoformat(), "data": ev_map},
                   ensure_ascii=False),
        encoding="utf-8"
    )
    return ev_map


def enrich_with_events(results: list, ev_map: dict) -> list:
    """
    스크리닝 결과에 공시 이벤트 정보 추가
    이벤트 점수 높은 순 → 괴리율 낮은 순으로 정렬
    """
    grade_score = {"★★★": 3, "★★": 2, "★": 1, "⚠": -1, "⛔": -3}

    for r in results:
        code   = r.get("code", "")
        events = ev_map.get(code, [])
        pos    = [e for e in events if e["type"] == "positive"]
        neg    = [e for e in events if e["type"] == "negative"]

        r["events_positive"] = pos
        r["events_negative"] = neg
        r["event_score"]     = sum(grade_score.get(e["grade"], 0) for e in events)

    # 정렬: 이벤트 점수 높은 순 → 괴리율 낮은 순
    results.sort(key=lambda x: (-x.get("event_score", 0), x.get("괴리율", 0)))
    return results


def get_event_summary(ev_map: dict) -> str:
    """이벤트 통계 요약 문자열"""
    pos_total = sum(len([e for e in v if e["type"]=="positive"]) for v in ev_map.values())
    neg_total = sum(len([e for e in v if e["type"]=="negative"]) for v in ev_map.values())
    return f"긍정 {pos_total}건 / 부정 {neg_total}건 ({len(ev_map)}개 종목)"


if __name__ == "__main__":
    print("=== 공시 이벤트 조회 테스트 ===")
    ev_map = get_event_map(days=7, force=True)
    print(f"\n이벤트 보유 종목: {len(ev_map)}개")
    print(get_event_summary(ev_map))
    print("\n--- 긍정 공시 샘플 ---")
    count = 0
    for code, events in ev_map.items():
        pos = [e for e in events if e["type"] == "positive"]
        if pos:
            for e in pos:
                print(f"  [{e['date']}] {e['name']}({code}) {e['grade']} {e['title']}")
                count += 1
                if count >= 20: break
        if count >= 20: break
