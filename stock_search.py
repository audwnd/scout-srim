# -*- coding: utf-8 -*-
"""
stock_search.py v9 - FnGuide CompanyList.txt 활용
FnGuide가 자동완성을 위해 매번 다운로드하는 전종목 JSON 파일 사용
형식: {"Co":[{"cd":"A005930","nm":"삼성전자","gb":"701"}, ...]}
"""

import requests
import json
from pathlib import Path
from datetime import datetime, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://comp.fnguide.com",
}

BASE       = Path(__file__).parent
CACHE_FILE = BASE / "WORK" / "stock_list.json"
CACHE_DAYS = 1   # 매일 갱신


# ── 종목 맵 ───────────────────────────────────────────────

def _download_company_list() -> dict:
    """FnGuide CompanyList.txt 다운로드 → {종목명: [코드, 시장]}"""
    url = "https://comp.fnguide.com/XML/Market/CompanyList.txt"
    r   = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    # BOM 포함 UTF-8 처리
    r.encoding = "utf-8-sig"
    text = r.text.strip()
    if not text:
        raise ValueError("빈 응답")
    data = json.loads(text)
    # 스팩(SPAC) 제외 패턴
    EXCLUDE_PATTERNS = [
        "스팩", "SPAC",           # 스팩
        "리츠",                    # 리츠 (메리츠 등 일반기업 제외 위해 endswith 사용)
        "인프라펀드", "맥쿼리",    # 인프라펀드
    ]
    # 종목명이 정확히 리츠로 끝나는 경우만 제외 (메리츠금융지주 등 오필터링 방지)
    def _is_investment_product(nm: str) -> bool:
        if nm.endswith("리츠"): return True
        if nm.endswith("인프라펀드"): return True
        for pat in ["스팩", "SPAC", "맥쿼리인프라"]:
            if pat in nm: return True
        return False

    stock_map = {}
    excluded  = 0
    for item in data.get("Co", []):
        cd = item.get("cd","").lstrip("A")   # A005930 → 005930
        nm = item.get("nm","").strip()
        gb = item.get("gb","")
        if cd and nm and gb == "701" and len(cd) == 6:
            if _is_investment_product(nm):
                excluded += 1
                continue
            stock_map[nm] = [cd, "KOSPI"]    # 시장 구분은 별도 처리
    print(f"  {len(stock_map):,}개 로드 (스팩/리츠 등 {excluded}개 제외)")
    return stock_map


def get_stock_map(force=False) -> dict:
    CACHE_FILE.parent.mkdir(exist_ok=True)
    if not force and CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                cache = json.load(f)
            cached_at = datetime.fromisoformat(cache.get("cached_at","2000-01-01"))
            if datetime.now() - cached_at < timedelta(days=CACHE_DAYS):
                return cache.get("data",{})
        except Exception:
            pass

    print("  종목 리스트 갱신 중 (FnGuide)...", end=" ", flush=True)
    try:
        stock_map = _download_company_list()
        if stock_map:
            with open(CACHE_FILE,"w",encoding="utf-8") as f:
                json.dump({"cached_at":datetime.now().isoformat(),"data":stock_map},
                          f, ensure_ascii=False)
            return stock_map
    except Exception as e:
        print(f"실패: {e}")
    return {}


def resolve_stock(name: str):
    """종목명 → (종목명, 코드)"""
    print(f"  종목 검색 중...", end=" ", flush=True)
    stock_map = get_stock_map()

    # 1. 정확히 일치
    if name in stock_map:
        code = stock_map[name][0]
        print("완료")
        print(f"  종목 확인: {name} ({code})")
        return name, code

    # 1-1. 대소문자 무관 일치 (NAVER → naver 등)
    name_upper = name.upper()
    for n, v in stock_map.items():
        if n.upper() == name_upper:
            code = v[0]
            print("완료")
            print(f"  종목 확인: {n} ({code})")
            return n, code

    # 2. 부분 일치 (짧은 이름 우선, 대소문자 무관)
    cands = [(n, v[0]) for n,v in stock_map.items()
             if name.upper() in n.upper() or n.upper() in name.upper()]
    cands.sort(key=lambda x: len(x[0]))
    if cands:
        n, code = cands[0]
        print("완료")
        print(f"  종목 확인: {n} ({code})")
        return n, code

    # 3. 강제 갱신 후 재시도
    print("갱신...", end=" ", flush=True)
    stock_map = get_stock_map(force=True)
    if name in stock_map:
        code = stock_map[name][0]
        print("완료")
        print(f"  종목 확인: {name} ({code})")
        return name, code

    cands = [(n, v[0]) for n,v in stock_map.items() if name in n]
    cands.sort(key=lambda x: len(x[0]))
    if cands:
        n, code = cands[0]
        print("완료")
        print(f"  종목 확인: {n} ({code})")
        return n, code

    print("실패")
    return None, None


def get_market(code: str) -> str:
    """FnGuide 종목 페이지에서 KOSPI/KOSDAQ 구분"""
    try:
        url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&MenuYn=Y"
        r   = requests.get(url, headers=HEADERS, timeout=10)
        if "KOSDAQ" in r.text[:3000] or "코스닥" in r.text[:3000]:
            return "KOSDAQ"
        return "KOSPI"
    except Exception:
        return "KOSPI"


# ── 시세/지수 (네이버) ────────────────────────────────────

def get_stock_ohlcv(code: str) -> dict:
    try:
        r = requests.get(
            f"https://m.stock.naver.com/api/stock/{code}/basic",
            headers=HEADERS, timeout=3)
        d = r.json()
        price = int(str(d.get("closePrice","0")).replace(",",""))
        rate  = float(str(d.get("fluctuationsRatio","0")).replace(",",""))
        diff  = int(str(d.get("compareToPreviousClosePrice","0")).replace(",",""))
        cv    = d.get("compareToPreviousPrice",{})
        is_up = cv.get("code","") in ["2","RISING"] if isinstance(cv,dict) else rate >= 0
        return {"price":price,"rate":rate,"diff":diff,"up":is_up}
    except Exception as e:
        print(f"  [시세 오류] {e}")
    return {"price":0,"rate":0.0,"diff":0,"up":True}


def get_index_info() -> dict:
    result = {}
    now = datetime.now().strftime("%H:%M")
    for name, idx_code in [("KOSPI","KOSPI"),("KOSDAQ","KOSDAQ")]:
        try:
            r  = requests.get(
                f"https://m.stock.naver.com/api/index/{idx_code}/basic",
                headers=HEADERS, timeout=3)
            d  = r.json()
            idx  = float(str(d.get("closePrice","0")).replace(",",""))
            chg  = float(str(d.get("fluctuationsRatio","0")).replace(",",""))
            diff = float(str(d.get("compareToPreviousClosePrice","0")).replace(",",""))
            cv   = d.get("compareToPreviousPrice",{})
            is_up = cv.get("code","") in ["2","RISING"] if isinstance(cv,dict) else chg >= 0
            result[name] = {"index":idx,"change":chg,"diff":diff,"up":is_up,"time":now}
        except Exception as e:
            print(f"  [{name} 오류] {e}")
            result[name] = {"index":0,"change":0,"diff":0,"up":True,"time":""}
    return result
