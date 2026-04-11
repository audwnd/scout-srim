# -*- coding: utf-8 -*-
"""
rim_screener.py - RIM병합 프로젝트 스크리닝 엔진 v2
필터 체계:
  [pykrx]    시가총액 KOSPI 500억↑ / KOSDAQ 300억↑, 주가 1000원↑, 거래정지 제외
  [XML 빠름] 해외기업 제외, 영업이익 2년↑ 손실 제외, 법인세차감전 1년↑ 손실 제외
  [KRX]      소수계좌매도 공시 종목 제외
  [Finance]  단기차입금 > 이익잉여금 제외 (2단계에서 적용)
"""

import json, sys, importlib, threading, re
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

BASE      = Path(__file__).parent
CACHE_DIR = BASE / "WORK" / "xml_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://comp.fnguide.com"
}
MAX_WORKERS    = 10
XML_CACHE_DAYS = 7


# ══════════════════════════════════════════════
# 1. 할인율
# ══════════════════════════════════════════════

def get_discount_rate() -> float:
    cache_f = CACHE_DIR / "ke.json"
    today   = datetime.now().strftime("%Y-%m-%d")
    try:
        if cache_f.exists():
            c = json.loads(cache_f.read_text())
            if c.get("date") == today:
                return c["rate"]
    except Exception:
        pass
    try:
        from io import BytesIO
        import pandas as pd
        r  = requests.post(
            "https://www.kisrating.com/ratingsStatistics/statics_spread.do",
            data={}, headers=HEADERS, timeout=10)
        df = pd.read_html(BytesIO(r.content), header=0)[0].set_index("구분")
        ke = float(df.loc["BBB-", "5년"]) / 100
        cache_f.write_text(json.dumps({"date": today, "rate": ke}))
        print(f"  할인율(BBB- 5년): {ke*100:.2f}%")
        return ke
    except Exception:
        print(f"  [할인율 조회 실패] → 기본값 10.26%")
        return 0.1026


# ══════════════════════════════════════════════
# 2. 소수계좌매도 공시 종목 (KRX KIND)
# ══════════════════════════════════════════════

def get_minority_sell_codes() -> set:
    """KRX KIND에서 소수계좌매도 공시 종목 코드 조회 (캐시: 1일)"""
    cache_f = CACHE_DIR / "minority_sell.json"
    today   = datetime.now().strftime("%Y-%m-%d")
    try:
        if cache_f.exists():
            c = json.loads(cache_f.read_text())
            if c.get("date") == today:
                return set(c.get("codes", []))
    except Exception:
        pass

    codes = set()
    try:
        # KRX KIND 소수계좌매도 공시 조회
        url  = "https://kind.krx.co.kr/disclosure/searchtodaydisclosure.do"
        data = {
            "method": "searchTodayDisclosureSub",
            "currentPageSize": "100",
            "pageIndex": "1",
            "orderMode": "0",
            "orderStat": "D",
            "forward": "todaydisclosure_sub",
            "disclosureType": "",
            "searchCodeType": "",
            "searchCorpName": "",
            "marketType": "",
            "reportNm": "소수계좌매도",
        }
        r    = requests.post(url, data=data, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        for row in soup.select("table tr"):
            tds = row.find_all("td")
            if not tds: continue
            # 종목코드 추출
            for td in tds:
                link = td.find("a", href=True)
                if link and "gicode" in link.get("href",""):
                    code = link["href"].split("gicode=A")[-1][:6]
                    if code.isdigit():
                        codes.add(code)

        cache_f.write_text(json.dumps({"date": today, "codes": list(codes)}))
        print(f"  소수계좌매도 공시: {len(codes)}개")
    except Exception as e:
        print(f"  [소수계좌매도 조회 실패] {e}")

    return codes


# ══════════════════════════════════════════════
# 3. pykrx 필터링
# ══════════════════════════════════════════════

def get_filtered_tickers(minority_codes: set) -> list:
    """
    pykrx로 전종목 수집 후 필터링
    - 시가총액: KOSPI 500억↑, KOSDAQ 300억↑
    - 주가 1,000원↑, 거래량 > 0 (거래정지 제외)
    - 소수계좌매도 공시 종목 제외
    """
    from pykrx import stock as _s
    date = _s.get_nearest_business_day_in_a_week()
    print(f"  기준일: {date}")

    result  = []
    summary = {}

    for market in ["KOSPI", "KOSDAQ"]:
        print(f"  [{market}] 수집 중...", end=" ", flush=True)
        df_p = _s.get_market_ohlcv_by_ticker(date, market=market)
        df_c = _s.get_market_cap_by_ticker(date, market=market)

        before = len(df_p)

        # 주가 1,000원↑ + 거래정지 제외
        df_f = df_p[(df_p["종가"] > 1000) & (df_p["거래량"] > 0)]

        # 시가총액 필터
        cap_limit = 50_000_000_000 if market == "KOSPI" else 30_000_000_000  # 500억/300억
        df_cf = df_c[df_c.index.isin(df_f.index)]
        if "시가총액" in df_cf.columns:
            df_cf = df_cf[df_cf["시가총액"] >= cap_limit]
            df_f  = df_f[df_f.index.isin(df_cf.index)]

        # 소수계좌매도 제외
        df_f = df_f[~df_f.index.isin(minority_codes)]

        after = len(df_f)
        print(f"{before}개 → {after}개 (제외: {before-after}개)")
        summary[market] = {"before": before, "after": after}

        for code in df_f.index:
            try:
                name   = _s.get_market_ticker_name(code)
                price  = int(df_f.loc[code, "종가"])
                shares = int(df_cf.loc[code, "상장주식수"]) if ("상장주식수" in df_cf.columns and code in df_cf.index) else 0
                mktcap = int(df_cf.loc[code, "시가총액"]) if ("시가총액" in df_cf.columns and code in df_cf.index) else 0
                result.append({
                    "code": code, "name": name,
                    "price": price, "market": market,
                    "shares": shares, "mktcap": mktcap,
                })
            except Exception:
                pass

    total_b = sum(v["before"] for v in summary.values())
    total_a = len(result)
    print(f"  최종: {total_b}개 → {total_a}개 ({total_b-total_a}개 제외)")
    return result


# ══════════════════════════════════════════════
# 4. FnGuide XML 수집 + 재무 필터
# ══════════════════════════════════════════════

def _is_foreign_company(soup: BeautifulSoup) -> bool:
    """해외기업 국내상장 여부 판단"""
    try:
        # 자본금 통화가 원화가 아닌 경우
        fv_unit = soup.find("face_value_unit")
        if fv_unit:
            unit = fv_unit.get_text(strip=True)
            if unit and unit not in ("원", "KRW", ""):
                return True
        # 액면가 단위가 달러/위안 등인 경우
        fv = soup.find("face_value")
        if fv:
            text = fv.get_text(strip=True)
            if any(x in text for x in ["$", "USD", "CNY", "CNH", "HKD"]):
                return True
    except Exception:
        pass
    return False


def fetch_xml_with_filter(code: str) -> dict:
    """
    FnGuide XML 수집 + 재무 필터 적용
    반환: 데이터 dict (filtered=True 이면 제외 대상)
    """
    cache_f = CACHE_DIR / f"{code}.json"
    if cache_f.exists():
        age = datetime.now() - datetime.fromtimestamp(cache_f.stat().st_mtime)
        if age.days < XML_CACHE_DAYS:
            try:
                return json.loads(cache_f.read_text(encoding="utf-8"))
            except Exception:
                pass

    url = f"https://comp.fnguide.com/SVO2/xml/Snapshot_all/{code}.xml"
    try:
        r    = requests.get(url, headers=HEADERS, timeout=12)
        soup = BeautifulSoup(r.content, "lxml-xml")

        def _num(tag):
            el = soup.find(tag)
            if not el: return None
            t  = el.get_text(strip=True).replace(",","")
            try: return float(t)
            except: return None

        def _rv(el):
            if not el: return None
            t = el.get_text(strip=True).replace(",","")
            try: return float(t)
            except: return None

        # ── 해외기업 판단
        filtered  = False
        filter_reason = ""
        if _is_foreign_company(soup):
            filtered = True
            filter_reason = "해외기업"

        # ── 발행주식수 / 자기주식
        shares   = _num("listed_stock_1") or 0
        treasury = 0

        # ── 시장 구분
        market = "KOSPI"
        stxt   = soup.find("stxt_group") or soup.find("mkt_nm")
        if stxt and ("KOSDAQ" in stxt.get_text() or "코스닥" in stxt.get_text()):
            market = "KOSDAQ"

        # ── Financial Highlight (공시 기준) 파싱
        ann_years   = []
        ann_roe     = []
        ann_equity  = []
        ann_bps     = []
        ann_op_prof = []   # 영업이익
        ann_pretax  = []   # 법인세차감전이익
        con_roe     = []

        fg = soup.find("financial_highlight_gongsi") or soup.find("financial_highlight_annual")
        if fg:
            for rec in fg.find_all("record"):
                yr = rec.find("year_nm")
                if not yr: continue
                yr_txt = yr.get_text(strip=True)
                # 추정치는 연간 실적용이 아님
                if "(E)" in yr_txt or "(P)" in yr_txt.upper():
                    continue

                ann_years.append(yr_txt)
                ann_roe.append(_rv(rec.find("roe")))
                ann_equity.append(_rv(rec.find("controlling_interest")))
                ann_bps.append(_rv(rec.find("bps")))
                ann_op_prof.append(_rv(rec.find("op_profit") or rec.find("operating_profit")))
                ann_pretax.append(_rv(rec.find("ebt") or rec.find("pretax_profit") or rec.find("income_before_tax")))

        # 컨센서스 ROE
        fa = soup.find("financial_highlight_ifrs_B") or soup.find("financial_highlight_annual")
        if fa:
            for rec in fa.find_all("record"):
                yr = rec.find("year_nm")
                if yr and "(E)" in yr.get_text():
                    roe_el = rec.find("roe")
                    if roe_el:
                        try: con_roe.append(float(roe_el.get_text(strip=True).replace(",","")))
                        except: con_roe.append(None)

        # ── 재무 필터 (XML 수준에서 가능한 것만)
        if not filtered:
            # 1) 영업이익 2개년 이상 연속 손실 제외
            op_vals = [v for v in ann_op_prof if v is not None]
            if len(op_vals) >= 2:
                # 최근 2년 연속 음수
                if op_vals[-1] < 0 and op_vals[-2] < 0:
                    filtered = True
                    filter_reason = f"영업이익 2년연속손실({op_vals[-2]:.0f},{op_vals[-1]:.0f})"

        if not filtered:
            # 2) 법인세차감전이익 1년 이상 손실
            # XML에서 직접 못 가져오면 순이익(ROE 기반)으로 대체
            # → 여기서는 ROE가 음수인 최근 연도 카운트
            roe_vals = [v for v in ann_roe if v is not None]
            if roe_vals and roe_vals[-1] is not None and roe_vals[-1] < 0:
                # 최근 1년 ROE 음수 → 법인세차감전 손실 가능성 높음
                # (정확한 법인세차감전이익은 SVD_Finance에서만 가능)
                pass  # 2단계에서 정밀 체크

        data = {
            "code":       code,
            "market":     market,
            "shares":     shares,
            "treasury":   treasury,
            "filtered":   filtered,
            "filter_reason": filter_reason,
            "ann_years":  ann_years[-5:],
            "ann_roe":    ann_roe[-5:],
            "ann_equity": ann_equity[-5:],
            "ann_bps":    ann_bps[-5:],
            "ann_op_prof":ann_op_prof[-5:],
            "ann_pretax": ann_pretax[-5:],
            "con_roe":    con_roe[:2],
            "cached_at":  datetime.now().isoformat(),
        }
        cache_f.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return data

    except Exception as e:
        return {"code": code, "error": str(e), "filtered": False}


# ══════════════════════════════════════════════
# 5. 2단계 정밀 재무 필터 (SVD_Finance)
# ══════════════════════════════════════════════

def fetch_detail_filter(code: str) -> dict:
    """
    SVD_Finance에서 단기차입금, 이익잉여금, 법인세차감전이익 체크
    반환: {"pass": True/False, "reason": ...}
    """
    cache_f = CACHE_DIR / f"{code}_detail.json"
    if cache_f.exists():
        age = datetime.now() - datetime.fromtimestamp(cache_f.stat().st_mtime)
        if age.days < XML_CACHE_DAYS:
            try:
                return json.loads(cache_f.read_text(encoding="utf-8"))
            except Exception:
                pass

    try:
        url  = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp"
                f"?pGB=1&gicode=A{code}&MenuYn=Y&NewMenuID=103&stkGb=701")
        r    = requests.get(url, headers=HEADERS, timeout=12)
        soup = BeautifulSoup(r.text, "html.parser")

        def _get_row(label: str, tbl) -> list:
            """테이블에서 특정 행의 값 추출"""
            for tr in tbl.find_all("tr"):
                th = tr.find("th")
                if not th: continue
                if label in th.get_text(strip=True):
                    vals = []
                    for td in tr.find_all("td"):
                        t = td.get_text(strip=True).replace(",","")
                        try: vals.append(float(t))
                        except: vals.append(None)
                    return vals
            return []

        # 재무상태표 테이블
        bs_tbl  = soup.find("div", id="divDaechaY")
        # 손익계산서 테이블
        is_tbl  = soup.find("div", id="divSonikY")

        pass_filter = True
        reason      = ""

        if bs_tbl:
            short_borrow  = _get_row("단기차입금", bs_tbl)
            retained      = _get_row("이익잉여금", bs_tbl)

            sb = next((v for v in reversed(short_borrow) if v is not None), None)
            re = next((v for v in reversed(retained)     if v is not None), None)

            # 단기차입금 > 이익잉여금
            if sb is not None and re is not None:
                if sb > re:
                    pass_filter = False
                    reason = f"단기차입금({sb:.0f}) > 이익잉여금({re:.0f})"

        if pass_filter and is_tbl:
            pretax = _get_row("법인세차감전 계속사업이익", is_tbl)
            if not pretax:
                pretax = _get_row("법인세비용차감전순이익", is_tbl)

            # 최근 1년 이상 손실
            recent = [v for v in pretax[-3:] if v is not None]
            if recent and recent[-1] < 0:
                pass_filter = False
                reason = f"법인세차감전이익 손실({recent[-1]:.0f})"

        result = {"pass": pass_filter, "reason": reason}
        cache_f.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        return result

    except Exception as e:
        return {"pass": True, "reason": f"조회실패:{e}"}  # 실패 시 통과


# ══════════════════════════════════════════════
# 6. RIM 계산
# ══════════════════════════════════════════════

def estimate_roe(ann_roe: list, con_roe: list) -> float:
    """
    ROE 추정 (최종 확정 방식):
    - 컨센서스 없음: 연간 3년 가중평균 (1:2:3) / 6
    - 컨센서스 있음: 연간 3년 + 컨센서스 1년차 (1:2:3:3) / 9
    가중치 의미: 오래된 실적=1, 중간=2, 최근실적=3, 컨센서스=3
    (최근 실적과 컨센서스를 동등하게 취급)
    """
    # 연간 ROE 정규화 (소수 단위)
    vals = []
    for v in (ann_roe or []):
        if v is not None:
            vals.append(v/100 if abs(v) > 2 else v)
    if not vals: return 0.0

    recent3 = vals[-3:]
    w3 = list(range(1, len(recent3)+1))  # 1,2,3

    # 컨센서스 1년차
    con1 = None
    for v in (con_roe or []):
        if v:
            con1 = v/100 if abs(v) > 2 else v
            break

    if con1 is not None:
        # 컨센서스 있음: 3년 + 컨센 (1:2:3:3)/9
        vals4 = recent3 + [con1]
        w4 = w3 + [3]
        return sum(v*wt for v,wt in zip(vals4, w4)) / sum(w4)
    else:
        # 컨센서스 없음: 3년 (1:2:3)/6
        return sum(v*wt for v,wt in zip(recent3, w3)) / sum(w3)


def is_roe_improving(ann_roe: list) -> bool:
    """ROE 개선 추세: 3년 연속 하락 or 최근 2년 음수면 False"""
    vals = [v/100 if v and v > 1 else v for v in (ann_roe or []) if v is not None]
    if len(vals) < 2: return True
    recent = vals[-3:]
    if len(recent) >= 3:
        a, b, c = recent[-1], recent[-2], recent[-3]
        if a < b < c: return False
        if a < 0 and b < 0: return False
    elif len(recent) == 2:
        a, b = recent[-1], recent[-2]
        if a < b and a < 0: return False
    return True


def calc_rim(equity, roe, ke, shares, treasury=0):
    float_shares = (shares or 0) - (treasury or 0)
    if not all([equity, ke]) or float_shares <= 0: return {}
    excess = equity * (roe - ke)
    unit   = 1e8 / float_shares
    if excess <= 0:
        fv = equity * unit
        return {"매도주가": round(fv), "적정주가": round(fv), "매수주가": round(fv), "배열": "역배열", "roe추정": round(roe*100,2)}
    val_sell = equity + excess / ke
    val_fair = equity + excess*0.9 / (1+ke-0.9)
    val_buy  = equity + excess*0.8 / (1+ke-0.8)
    return {
        "매도주가": round(val_sell*unit), "적정주가": round(val_fair*unit),
        "매수주가": round(val_buy*unit),  "배열": "정배열", "roe추정": round(roe*100,2)
    }


# ══════════════════════════════════════════════
# 7. 스팩/투자상품 종목명 필터
# ══════════════════════════════════════════════

def _is_excluded_by_name(name: str) -> bool:
    """종목명 기반 제외 (스팩, 리츠 등)"""
    if name.endswith("리츠"):          return True
    if name.endswith("인프라펀드"):    return True
    if "스팩" in name:                 return True
    if "SPAC" in name.upper():        return True
    if "맥쿼리인프라" in name:         return True
    return False


# ══════════════════════════════════════════════
# 8. 1단계 스크리닝
# ══════════════════════════════════════════════

def run_stage1(ke: float, tickers: list, undervalue_pct: float = 0,
               strict_mode: bool = False) -> list:
    total      = len(tickers)
    candidates = []
    filtered   = []
    done       = 0
    print(f"\n  [1단계] {total}개 병렬 스크리닝 (동시 {MAX_WORKERS}개)...")
    start = datetime.now()

    def _proc(info):
        code  = info["code"]
        name  = info["name"]
        price = info["price"]

        # 종목명 필터
        if _is_excluded_by_name(name):
            return None, f"{name}: 투자상품 제외"

        fin = fetch_xml_with_filter(code)
        if fin.get("error"):
            return None, f"{name}: XML 오류"
        if fin.get("filtered"):
            return None, f"{name}: {fin.get('filter_reason','재무필터')}"

        equities = [v for v in (fin.get("ann_equity") or []) if v and v > 0]
        if not equities: return None, f"{name}: 지배주주지분 없음"
        equity = equities[-1]

        ann_roe = fin.get("ann_roe", [])
        con_roe = fin.get("con_roe", [])

        # 엄선 모드: 컨센서스 없으면 제외
        if strict_mode and not any(v for v in (con_roe or [])):
            return None, None
        # 엄선 모드: ROE 하락 추세면 제외
        if strict_mode and not is_roe_improving(ann_roe):
            return None, None

        roe_est = estimate_roe(ann_roe, con_roe)
        shares  = info.get("shares") or fin.get("shares") or 0
        rim     = calc_rim(equity, roe_est, ke, shares)
        if not rim: return None, f"{name}: RIM 계산 불가"

        fair  = rim.get("적정주가", 0)
        if not fair: return None, f"{name}: 적정주가 0"

        ratio = (price / fair - 1) * 100
        if ratio > undervalue_pct:
            return None, None  # 고평가 → 조용히 제외

        return {
            "name": name, "code": code,
            "market": fin.get("market", info.get("market","")),
            "현재가": price, "적정주가": fair,
            "매수주가": rim.get("매수주가",0),
            "매도주가": rim.get("매도주가",0),
            "괴리율": round(ratio,1),
            "roe추정": rim.get("roe추정",0),
            "배열": rim.get("배열",""),
            "stage": 1,
        }, None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_proc, t): t for t in tickers}
        for fut in as_completed(futs):
            done += 1
            try:
                r, reason = fut.result()
                if r:   candidates.append(r)
                elif reason: filtered.append(reason)
            except Exception as e:
                filtered.append(str(e))
            if done % 300 == 0:
                elapsed = (datetime.now()-start).total_seconds()
                remain  = elapsed/done*(total-done)
                print(f"    {done}/{total} | 후보:{len(candidates)} | 잔여:{remain/60:.0f}분")

    elapsed = (datetime.now()-start).total_seconds()
    candidates.sort(key=lambda x: x["괴리율"])
    print(f"\n  [1단계 완료] {total}개 → 저평가 후보 {len(candidates)}개 ({elapsed/60:.1f}분)")
    return candidates


# ══════════════════════════════════════════════
# 9. 2단계 V33 엑셀 정밀 계산
# ══════════════════════════════════════════════

def run_stage2(candidates: list, strict_mode: bool = False) -> list:
    if not candidates: return []
    print(f"\n  [2단계] {len(candidates)}개 후보 정밀 검증 + V33 엑셀 계산...")

    sys.path.insert(0, str(BASE))
    import fnguide_collector_v4 as _col, srim_filler_v4 as _filler
    importlib.reload(_col); importlib.reload(_filler)
    import openpyxl

    template = BASE / "S-RIM_V33_ForwardBlock.xlsx"
    out_dir  = BASE / "OUTPUT"
    out_dir.mkdir(exist_ok=True)
    today    = datetime.now().strftime("%Y%m%d")
    results  = []

    for i, c in enumerate(candidates, 1):
        code = c["code"]
        name = c["name"]
        print(f"  [{i}/{len(candidates)}] {name}...", end=" ", flush=True)
        try:
            # 정밀 재무 필터 (단기차입금/법인세차감전)
            df = fetch_detail_filter(code)
            if not df.get("pass", True):
                print(f"⛔ 제외: {df.get('reason','')}")
                continue

            # FnGuide 상세 수집
            data = _col.collect(name, code)
            json_path = BASE/"WORK"/f"{code}_{name}.json"
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

            # V33 엑셀 계산
            out_path = out_dir / f"{name}_SRIM_{today}.xlsx"
            _filler.fill(str(template), str(json_path), str(out_path), strict_mode=strict_mode)

            try:
                import win32com.client as w32
                xl = w32.Dispatch("Excel.Application")
                xl.Visible = False; xl.DisplayAlerts = False
                wb = xl.Workbooks.Open(str(out_path.resolve()))
                wb.Application.CalculateFull()
                wb.Save(); wb.Close(False); xl.Quit()
            except Exception as e:
                print(f"[win32:{e}]", end=" ")

            wb2 = openpyxl.load_workbook(str(out_path), data_only=True)
            ws  = wb2["결과"]
            def _cv(coord):
                v = ws[coord].value
                return v if (v is not None and v != "") else 0

            c2 = dict(c)
            c2.update({
                "적정주가": round(_cv("C28")) if _cv("C28") else 0,
                "매도주가": round(_cv("C29")) if _cv("C29") else 0,
                "매수주가": round(_cv("C30")) if _cv("C30") else 0,
                "roe추정":  round((_cv("D20") or 0)*100, 2),
                "할인율":   round((_cv("C17") or 0)*100, 2),
                "배열":     str(_cv("F31") or ""),
                "추세":     str(_cv("I21") or ""),
                "roe수준":  str(_cv("G31") or ""),
                "stage":    2,
                "xlsx":     str(out_path),
            })
            wb2.close()
            if c2["적정주가"] and c2["현재가"]:
                c2["괴리율"] = round((c2["현재가"]/c2["적정주가"]-1)*100, 1)
            results.append(c2)
            print(f"✓ 적정주가 {c2['적정주가']:,}원 (괴리율 {c2['괴리율']:+.1f}%)")
        except Exception as e:
            c["error"] = str(e)
            c["stage"] = 2
            results.append(c)
            print(f"✗ {e}")

    results.sort(key=lambda x: x["괴리율"])
    print(f"\n  [2단계 완료] {len(results)}개")
    return results


# ══════════════════════════════════════════════
# 10. 리포트 저장
# ══════════════════════════════════════════════

def save_report(results: list, path: str):
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "저평가종목"

    headers = ["종목명","코드","시장","현재가","적정주가","매수주가","매도주가",
               "괴리율(%)","ROE추정(%)","배열","추세","계산방식","비고"]

    hf  = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    hft = Font(color="FFFFFF", bold=True, size=10)
    thin= Side(style="thin", color="DDDDDD")
    bd  = Border(left=thin, right=thin, top=thin, bottom=thin)
    ctr = Alignment(horizontal="center", vertical="center")

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill=hf; cell.font=hft; cell.alignment=ctr; cell.border=bd
    ws.row_dimensions[1].height = 22

    for row, r in enumerate(results, 2):
        stage  = r.get("stage", 1)
        방식   = "V33엑셀" if stage==2 else "Python간이"
        vals   = [
            r.get("name",""), r.get("code",""), r.get("market",""),
            r.get("현재가",0), r.get("적정주가",0),
            r.get("매수주가",0), r.get("매도주가",0),
            r.get("괴리율",0), r.get("roe추정",0),
            r.get("배열",""), r.get("추세",""),
            방식, r.get("error",""),
        ]
        bg = "E8F4EA" if not r.get("error") else "FCE8E8"
        rf = PatternFill(start_color=bg, end_color=bg, fill_type="solid")
        for col, v in enumerate(vals, 1):
            cell = ws.cell(row=row, column=col, value=v)
            cell.fill=rf; cell.border=bd; cell.alignment=ctr
            if col==8 and isinstance(v,(int,float)):
                cell.font = Font(color="185FA5" if v<0 else "C00000", bold=True)
        ws.row_dimensions[row].height = 18

    for col, w in enumerate([14,8,7,10,10,10,10,10,10,8,10,10,30,35,10,20],1):
        ws.column_dimensions[get_column_letter(col)].width = w

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"
    ws.append([])
    ws.append([f"저평가 종목 {len(results)}개 | {datetime.now().strftime('%Y-%m-%d %H:%M')}"])
    wb.save(path)
    print(f"  📊 저장: {path}")


# ══════════════════════════════════════════════
# 11. 메인
# ══════════════════════════════════════════════

def run_full(undervalue_pct: float = 0, stage2: bool = True,
             strict_mode: bool = False):
    print("\n" + "="*55)
    print("  RIM 병합 스크리닝 v2")
    print(f"  저평가 기준: 현재가/적정주가 ≤ {1+undervalue_pct/100:.0%}")
    print("="*55)
    start = datetime.now()

    ke             = get_discount_rate()
    minority_codes = get_minority_sell_codes()
    print(f"\n  종목 필터링 중...")
    tickers        = get_filtered_tickers(minority_codes)
    if strict_mode:
        print("  [엄선 모드] 컨센서스 있음 + ROE 개선 추세 종목만")
    candidates     = run_stage1(ke, tickers, undervalue_pct, strict_mode)
    # 2단계: 상위 50개만 처리 (괴리율 낮은 순)
    if stage2 and candidates:
        stage2_targets = candidates[:50]
        if len(candidates) > 50:
            print(f"  2단계 대상: {len(candidates)}개 → 상위 50개만 처리")
        final = run_stage2(stage2_targets, strict_mode=strict_mode)
    else:
        final = candidates

    today  = datetime.now().strftime("%Y%m%d_%H%M")
    report = str(BASE/"SCREENING"/f"RIM스크리닝_{today}.xlsx")
    save_report(final, report)

    elapsed = (datetime.now()-start).total_seconds()
    print(f"\n  총 소요시간: {elapsed/60:.1f}분 | 저평가 종목: {len(final)}개")
    return final


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="RIM 스크리닝")
    p.add_argument("--pct",    type=float, default=0,   help="저평가 기준 %% (0=적정가 이하)")
    p.add_argument("--stage1",  action="store_true", help="1단계만 실행")
    p.add_argument("--strict",  action="store_true", help="엄선 모드 (컨센서스+ROE개선)")
    args = p.parse_args()
    run_full(undervalue_pct=args.pct, stage2=not args.stage1, strict_mode=args.strict)
