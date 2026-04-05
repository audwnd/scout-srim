# -*- coding: utf-8 -*-
"""
kis_collector.py
KIS 한국신용평가 금리스프레드 페이지에서 BBB- 5년 수익률 수집
https://www.kisrating.com/ratingsStatistics/statics_spread.do
"""

import requests
from bs4 import BeautifulSoup
from typing import Optional

KIS_URL = "https://www.kisrating.com/ratingsStatistics/statics_spread.do"
KIS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.kisrating.com/",
}

def get_bbb_minus_5yr() -> Optional[float]:
    """
    KIS 금리스프레드 페이지에서 BBB- 5년 수익률 수집
    Returns: 소수 (예: 0.1024 = 10.24%)
    """
    r = requests.get(KIS_URL, headers=KIS_HEADERS, timeout=15)
    r.raise_for_status()
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    # 수익률 테이블 (첫번째 테이블 = 등급별 수익률)
    # 두번째 테이블 = 등급별 스프레드 (국고채 대비 bp)
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("KIS 페이지에서 테이블을 찾을 수 없음")

    tbl = tables[0]

    # 헤더에서 "5년" 열 인덱스 찾기
    header_row = tbl.find("tr")
    if not header_row:
        raise ValueError("헤더 행 없음")

    cols = [c.get_text(strip=True) for c in header_row.find_all(["th", "td"])]
    if "5년" not in cols:
        raise ValueError(f"'5년' 컬럼 없음. 발견된 컬럼: {cols}")
    idx_5yr = cols.index("5년")

    # BBB- 행 찾기
    for row in tbl.find_all("tr")[1:]:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        label = cells[0].get_text(strip=True)
        if label == "BBB-":
            raw = cells[idx_5yr].get_text(strip=True).replace(",", "")
            rate = float(raw) / 100  # % → 소수
            return rate

    raise ValueError("BBB- 행을 찾을 수 없음")


if __name__ == "__main__":
    rate = get_bbb_minus_5yr()
    print(f"BBB- 5년 수익률: {rate:.4f} ({rate*100:.2f}%)")
