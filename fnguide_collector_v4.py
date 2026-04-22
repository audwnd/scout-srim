# -*- coding: utf-8 -*-
"""
fnguide_collector_v4.py - 최종판
tbl[11] 한 곳에서 모든 재무 데이터 수집
업종비교/베타/발행주식수는 테이블 내용으로 탐색 (인덱스 미사용)
"""

import re, json, time, argparse
from datetime import datetime
from typing import Optional
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://comp.fnguide.com/",
}

def _get(url, retries=3, timeout=20):
    """FnGuide HTTP GET — 타임아웃/연결오류 시 최대 retries회 재시도"""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            # script 태그 제거 (JS 코드 안의 id 문자열이 파싱 방해 방지)
            for tag in soup.find_all("script"):
                tag.decompose()
            return soup
        except requests.exceptions.Timeout as e:
            last_err = e
            wait = attempt * 5  # 5초, 10초, 15초 간격
            print(f"  [타임아웃] {attempt}/{retries}회 — {wait}초 후 재시도...")
            time.sleep(wait)
        except requests.exceptions.ConnectionError as e:
            last_err = e
            wait = attempt * 5
            print(f"  [연결오류] {attempt}/{retries}회 — {wait}초 후 재시도...")
            time.sleep(wait)
        except requests.exceptions.HTTPError:
            raise  # HTTP 오류(404, 500 등)는 재시도 없이 바로 예외
    raise requests.exceptions.Timeout(
        f"FnGuide 응답 없음 ({retries}회 재시도 실패): {url}"
    ) from last_err

def _num(s) -> Optional[float]:
    if s is None: return None
    s = str(s).strip().replace(",","").replace("%","").replace("\xa0","").replace(" ","")
    if s in ("", "-", "N/A", "na", "NA", "적전", "흑전", "적지", "흑지"): return None
    try: return float(s)
    except: return None

def _cell_text(td):
    """
    FnGuide 셀 텍스트 추출 - 실제 HTML 구조 기반

    확인된 구조:
      일반 th:  <th><div>텍스트</div></th>
      추정연도: <th><div><dl><dt>(E):...</dt><dd>...</dd></dl><a>2026/12(E)</a></div></th>
      업종비교: <th><div><dl><dt>PER</dt><dd>설명</dd></dl></div></th>
      값 td:    <td>18.27</td>  (직접 텍스트)
    """
    # div가 있는 경우
    div = td.find("div", recursive=False) or td.find("div")

    if div:
        # div 직접 텍스트 노드 (일반 th: "2021/12", "매출액" 등)
        direct_div = "".join(t for t in div.find_all(string=True, recursive=False)).strip()
        if direct_div:
            return direct_div

        # dt 텍스트: "EPS(원)", "BPS(원)", "PER(배)", "배당수익률(%)" 등
        dt = div.find("dt")
        if dt:
            dt_text = "".join(t for t in dt.find_all(string=True, recursive=False)).strip()
            if dt_text:
                # 추정연도 셀: dt="(E) : Estimate" → a="2026/12(E)" 사용
                if dt_text.startswith("(E)") or dt_text.startswith("(P)"):
                    a = div.find("a")
                    if a:
                        a_text = a.get_text(strip=True)
                        if a_text:
                            return a_text
                return dt_text

        # fallback
        return div.get_text(strip=True)

    # div 없는 경우: 직접 텍스트 노드
    direct = "".join(t for t in td.find_all(string=True, recursive=False)).strip()
    if direct:
        return direct

    return td.get_text(strip=True)

def _parse_table(tbl):
    """테이블 → rows 변환"""
    rows = []
    for tr in tbl.find_all("tr"):
        cells = [_cell_text(td) for td in tr.find_all(["th","td"])]
        if any(c.strip() for c in cells):
            rows.append(cells)
    return rows

def collect(name, code):
    print(f"  [1/2] Snapshot 수집 중...")
    url_snap = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp"
                f"?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701")
    soup = _get(url_snap)
    all_tables = soup.find_all("table")
    time.sleep(1)

    print(f"  [2/2] 컨센서스 수집 중...")
    url_con = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Consensus.asp"
               f"?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=108&stkGb=701")
    soup_con = _get(url_con)
    time.sleep(0.5)

    result = {}

    # ── 회사명
    tag = soup.find("div", class_="corp_group1")
    result["name"] = tag.find("h1").get_text(strip=True) if tag and tag.find("h1") else name

    # ── 현재가 (네이버 증권 API - 더 정확한 실시간 가격)
    try:
        import requests as _req
        _r = _req.get(
            f"https://m.stock.naver.com/api/stock/{code}/basic",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5
        )
        _d = _r.json()
        _price = int(str(_d.get("closePrice", "0")).replace(",", ""))
        result["현재가"] = float(_price) if _price else None
    except Exception:
        # 네이버 실패 시 FnGuide 가격으로 폴백
        price_tag = soup.find("span", id="svdMainChartTxt11")
        result["현재가"] = _num(price_tag.get_text(strip=True)) if price_tag else None

    # ── 52주 고/저가 (FnGuide soup에서 추출)
    try:
        # svdMainChartTxt* ID 들의 텍스트를 확인
        _chart_txts = {}
        for i in range(11, 30):
            tag = soup.find(attrs={"id": f"svdMainChartTxt{i}"})
            if tag:
                _chart_txts[f"Txt{i}"] = tag.get_text(strip=True)
        print(f"  [FnGuide ChartTxt] {_chart_txts}")
    except Exception as _e:
        print(f"  [52주 디버그 오류] {_e}")

    # ── 투자자별 순매수 수집 (pykrx — 전일 확정 + 5일 추이 + 연기금 포함)
    def _collect_investor_krx(code_6: str) -> dict:
        """
        pykrx로 투자자별 순매수 수집 (Daum API 우회 제거)
        - get_market_trading_value_by_investor(strt5, end, code) → 5일 합산 (외국인/연기금/기관/개인)
        - get_market_trading_value_by_date(strt5, end, code, on="순매수") → 일별 추이 (바차트용)
        """
        from datetime import datetime, timedelta

        # 최근 5 영업일 범위 계산
        _dates: list = []
        _d = datetime.now() - timedelta(days=1)
        while len(_dates) < 10:
            if _d.weekday() < 5:
                _dates.append(_d.strftime("%Y%m%d"))
            _d -= timedelta(days=1)
        _dates.sort()
        strt5 = _dates[-5]
        end   = _dates[-1]

        try:
            from pykrx import stock as _s

            # ── Call A: 5일 합산 투자자별 순매수 (거래대금 기준, 연기금 포함)
            df_total = _s.get_market_trading_value_by_investor(strt5, end, code_6)
            if df_total is None or df_total.empty:
                print(f"  [pykrx 수급] 투자자 합산 데이터 없음")
                return {}

            순매수_col = next((c for c in df_total.columns if "순매수" in c), None)
            if not 순매수_col:
                print(f"  [pykrx 수급] 순매수 컬럼 없음: {list(df_total.columns)}")
                return {}

            def _inv(keys):
                for k in keys:
                    if k in df_total.index:
                        return round(float(df_total.loc[k, 순매수_col] or 0) / 1e8, 1)
                return None

            for_5일 = _inv(["외국인합계", "외국인"])
            pen_5일 = _inv(["연기금 등", "연기금등", "연기금"])
            org_5일 = _inv(["기관합계"])
            ind_5일 = _inv(["개인"])

            # ── Call B: 일별 순매수 추이 (바차트 + 전일값)
            rows_5   = []
            for_전일 = for_5일
            org_전일 = org_5일
            ind_전일 = ind_5일
            전일날짜  = f"{end[:4]}.{end[4:6]}.{end[6:]}"

            try:
                df_daily = _s.get_market_trading_value_by_date(strt5, end, code_6, on="순매수")
                if df_daily is not None and not df_daily.empty:
                    col_f = next((c for c in df_daily.columns if "외국인" in c), None)
                    col_i = next((c for c in df_daily.columns if "기관" in c), None)
                    col_p = next((c for c in df_daily.columns if "개인" in c), None)

                    for dt_idx in df_daily.index:
                        dt_str = str(dt_idx)
                        # pandas Timestamp → YYYYMMDD 문자열 변환
                        if hasattr(dt_idx, "strftime"):
                            dt_str = dt_idx.strftime("%Y%m%d")
                        if len(dt_str) == 8:
                            dt_str = f"{dt_str[:4]}.{dt_str[4:6]}.{dt_str[6:]}"
                        row = df_daily.loc[dt_idx]
                        rows_5.append({
                            "날짜":   dt_str,
                            "외국인": round(float(row[col_f] or 0) / 1e8, 1) if col_f else None,
                            "기관":   round(float(row[col_i] or 0) / 1e8, 1) if col_i else None,
                            "개인":   round(float(row[col_p] or 0) / 1e8, 1) if col_p else None,
                        })

                    # 전일(마지막 행)값 → 전일 순매수 박스에 사용
                    if rows_5:
                        last = rows_5[-1]
                        for_전일 = last["외국인"]
                        org_전일 = last["기관"]
                        ind_전일 = last["개인"]
                        전일날짜  = last["날짜"]

                    # 최신→오래된 순 (index.html에서 .reverse()로 차트 렌더링)
                    rows_5 = list(reversed(rows_5))

            except Exception as _e:
                print(f"  [pykrx 일별] {_e}")

            print(f"  수급 수집완료(pykrx): {전일날짜} "
                  f"외국인={for_전일}억 연기금={pen_5일}억(5일합산) "
                  f"기관={org_전일}억 개인={ind_전일}억")

            return {
                "전일날짜":      전일날짜,
                "외국인_순매수": for_전일,   # 전일 값
                "연기금_순매수": pen_5일,    # 5일 합산 (일별 미제공)
                "기관_순매수":   org_전일,   # 전일 값
                "개인_순매수":   ind_전일,   # 전일 값
                "연기금_5일합산": pen_5일,   # 5일 누적 박스용
                "5일":           rows_5,
            }

        except Exception as _e:
            print(f"  [pykrx 수급 오류] {_e}")
            return {}

    try:
        result["투자자"] = _collect_investor_krx(code)
    except Exception as _e:
        print(f"  [수급 수집 실패] {_e}")
        result["투자자"] = {}

    # ── 모든 테이블 파싱
    parsed = [_parse_table(tbl) for tbl in all_tables]

    # ── 베타, 발행주식수: 내용으로 테이블 탐색
    result["베타"] = None
    result["발행주식수_보통"] = None
    result["발행주식수_우선"] = 0
    result["자기주식"] = 0

    for rows in parsed:
        for row in rows:
            if len(row) < 2: continue
            # 베타 (1년) - col0 or col2 에 위치
            for ci in [0, 2]:
                if ci < len(row) and row[ci].strip().startswith("베타"):
                    val_ci = ci + 1
                    if val_ci < len(row):
                        result["베타"] = result["베타"] or _num(row[val_ci])
            # 발행주식수 (보통주/ 우선주)
            if "발행주식수" in row[0] and len(row) >= 2:
                raw = row[1].replace(" ","").replace(",","")
                if "/" in raw:
                    parts = raw.split("/")
                    result["발행주식수_보통"] = result["발행주식수_보통"] or _num(parts[0])
                    result["발행주식수_우선"] = result["발행주식수_우선"] or _num(parts[1])
            # 자기주식
            if "자기주식" in row[0] and "자사주" in row[0] and len(row) >= 3:
                result["자기주식"] = result["자기주식"] or int(_num(row[2]) or 0)

    # ── 업종비교: IFRS연결 기준 테이블만 정확히 사용
    # id="svdMainGrid10D" = IFRS연결, id="svdMainGrid10B" = IFRS별도
    industry = {}
    result["EV_EBITDA"] = None
    result["PER"] = None
    result["업종_PER"] = None

    # 업종비교 IFRS연결 테이블 탐색 (3가지 방법 시도)
    def _parse_industry(tbl_el):
        """업종비교 테이블에서 데이터 추출"""
        for row in _parse_table(tbl_el):
            if not row: continue
            label = row[0].strip()
            if label == "PER":
                result["PER"] = _num(row[1]) if len(row)>1 else None
                industry["종목_PER"] = result["PER"]
                industry["업종_PER"] = _num(row[2]) if len(row)>2 else None
                result["업종_PER"] = industry["업종_PER"]
            elif label == "EV/EBITDA":
                result["EV_EBITDA"] = _num(row[1]) if len(row)>1 else None
            elif label == "ROE":
                industry["종목_ROE"] = _num(row[1]) if len(row)>1 else None
                industry["업종_ROE"] = _num(row[2]) if len(row)>2 else None
                industry["KOSPI_ROE"] = _num(row[3]) if len(row)>3 else None
            elif label == "배당수익률":
                industry["종목_배당"] = _num(row[1]) if len(row)>1 else None
                industry["업종_배당"] = _num(row[2]) if len(row)>2 else None
                industry["KOSPI_배당"] = _num(row[3]) if len(row)>3 else None

    # 방법1: CSS 셀렉터
    tbl_d = soup.select_one("#svdMainGrid10D table")
    if tbl_d:
        _parse_industry(tbl_d)
    else:
        # 방법2: find with attrs
        div_d = soup.find(attrs={"id": "svdMainGrid10D"})
        if div_d:
            tbl_d = div_d.find("table")
            if tbl_d:
                _parse_industry(tbl_d)
        else:
            # 방법3: 전체 테이블 순서 - KOSPI+PER+ROE 포함하는 첫 번째 테이블 사용
            for tbl_el in soup.find_all("table"):
                rows = _parse_table(tbl_el)
                labels = [r[0].strip() for r in rows if r]
                if "PER" in labels and "ROE" in labels and "EV/EBITDA" in labels:
                    _parse_industry(tbl_el)
                    break

    result["industry"] = industry

    # ── tbl[10]: Net Quarter 분기 데이터 수집
    quarter = {}
    try:
        import re as _re
        # tbl[10] = Annual + Net Quarter 혼합 테이블
        for tbl_el in all_tables:
            rows_h = tbl_el.find_all("tr")
            if not rows_h:
                continue
            hdr = [_cell_text(td) for td in rows_h[0].find_all(["th","td"])]
            # "IFRS(연결)", "Annual", "Net Quarter" 모두 있는 테이블
            if "Annual" in hdr and "Net Quarter" in hdr:
                rows10 = [[_cell_text(td) for td in tr.find_all(["th","td"])]
                          for tr in rows_h]
                yr_row10 = rows10[1] if len(rows10) > 1 else []

                # 분기 열 인덱스 파악
                # tbl[10] 구조: [Annual열들(시간증가)] [Quarter열들(날짜가 역전됨)]
                # → 날짜가 역전되는 지점이 Annual→Quarter 경계
                from datetime import datetime as _dt
                def _yr_to_dt(y):
                    m = _re.search(r'(\d{4})/(\d{2})', y)
                    if m: return _dt(int(m.group(1)), int(m.group(2)), 1)
                    return None

                dts10 = [_yr_to_dt(y) for y in yr_row10]

                # 날짜가 줄어드는 첫 지점 = Quarter 시작
                q_start = len(yr_row10)
                for _i in range(1, len(yr_row10)):
                    if dts10[_i] and dts10[_i-1] and dts10[_i] < dts10[_i-1]:
                        q_start = _i
                        break

                q_idxs = [(i, yr_row10[i]) for i in range(q_start, len(yr_row10))
                          if _yr_to_dt(yr_row10[i]) is not None]
                q_idxs = q_idxs[:5]  # 최근 5분기

                if q_idxs:
                    Q_KEYS = {
                        "매출액":             "매출액",
                        "영업이익":           "영업이익",
                        "영업이익(발표기준)":  "영업이익(발표기준)",
                        "당기순이익":         "당기순이익",
                        "지배주주순이익":     "지배주주순이익",
                        "비지배주주순이익":   "비지배주주순이익",
                        "자산총계":           "자산총계",
                        "부채총계":           "부채총계",
                        "자본총계":           "자본총계",
                        "지배주주지분":       "지배주주지분",
                        "비지배주주지분":     "비지배주주지분",
                        "자본금":             "자본금",
                        "ROA(%)":             "ROA",
                        "ROE(%)":             "ROE",
                        "EPS(원)":            "EPS",
                        "BPS(원)":            "BPS",
                        "DPS(원)":            "DPS",
                    }
                    def _match_label(label):
                        return Q_KEYS.get(label.strip())
                    quarter["years"] = [y.replace("(dup)", "") for _, y in q_idxs]
                    for key in Q_KEYS.values():
                        quarter[key] = []
                    for row in rows10[2:]:
                        if not row:
                            continue
                        label = row[0].strip()
                        mapped = _match_label(label)
                        if mapped:
                            vals = []
                            for idx, _ in q_idxs:
                                raw = row[idx + 1] if idx + 1 < len(row) else ""
                                vals.append(_num(raw))
                            quarter[mapped] = vals
                    print(f"    분기 수집: {quarter['years']}")
                else:
                    print("    분기 데이터 없음")
                break
    except Exception as e:
        print(f"    [경고] 분기 수집 실패: {e}")

    # ── tbl[11]: IFRS(연결) Annual - 재무 핵심 데이터
    # 헤더행[0] = "IFRS(연결)", "Annual"
    # 헤더행[1] = "2021/12", ..., "(E):Estimate... 2026/12(E)", ...
    act_years, est_years = [], []
    act_cols,  est_cols  = [], []
    annual_act, annual_est = {}, {}

    LABEL_MAP = {
        "매출액": "매출액",
        "영업이익": "영업이익",
        "당기순이익": "당기순이익",
        "지배주주순이익": "지배주주순이익",
        "비지배주주순이익": "비지배주주순이익",
        "자산총계": "자산총계",
        "부채총계": "부채총계",
        "자본총계": "자본총계",
        "지배주주지분": "지배주주지분",
        "비지배주주지분": "비지배주주지분",
        "자본금": "자본금",
    }
    KEYWORD_MAP = {
        "ROA(%)": "ROA",        # 추가: ROA
        "ROE": "ROE",
        "EPS(원)": "EPS",
        "BPS(원)": "BPS",
        "DPS(원)": "DPS",
        "PER(배)": "PER",
        "PBR(배)": "PBR",
        "배당수익률(%)": "배당수익률",
    }

    for tbl in all_tables:
        rows = _parse_table(tbl)
        if len(rows) < 3: continue

        # 헤더 행[0] 조건: "IFRS(연결)" + "Annual" 포함, Net Quarter/별도 제외
        r0 = rows[0]
        r0_text = " ".join(r0)
        if "IFRS(연결)" not in r0_text: continue
        if "Annual" not in r0_text: continue
        if "Net Quarter" in r0_text: continue  # tbl[10] 제외
        if "별도" in r0_text: continue          # tbl[14] 제외

        # 연도 행 파싱 (행[1])
        yr_row = rows[1]
        tmp_act, tmp_est = [], []
        for i, cell in enumerate(yr_row):
            # 공백/개행 정규화
            cell_clean = re.sub(r"\s+", " ", cell).strip()
            # 실적: "2021/12" 형태 + "2025/12(P)" 잠정실적도 포함
            if re.match(r"^\d{4}/12$", cell_clean) or re.match(r"^\d{4}/12\(P\)$", cell_clean):
                yr = cell_clean[:4]
                tmp_act.append((i+1, yr))
            # 추정: "(E) ... 2026/12(E)" 형태
            m_est = re.search(r"(\d{4})/12\(E\)", cell_clean)
            if m_est:
                tmp_est.append((i+1, m_est.group(1)))

        # 실적 5개 + 추정 3개여야 정상 (tbl[11])
        if len(tmp_act) < 5 or len(tmp_est) < 1:
            continue

        act_cols  = tmp_act
        est_cols  = tmp_est
        act_years = [yr for _, yr in act_cols]
        est_years = [yr for _, yr in est_cols]
        found_keys = set()

        for row in rows[2:]:
            if not row: continue
            label = row[0].strip()

            if label in LABEL_MAP and label not in found_keys:
                key = LABEL_MAP[label]
                annual_act[key] = [_num(row[c]) if c < len(row) else None for c, _ in act_cols]
                annual_est[key] = [_num(row[c]) if c < len(row) else None for c, _ in est_cols]
                found_keys.add(label)
                continue

            for kw, key in KEYWORD_MAP.items():
                if kw in label and key not in found_keys:
                    annual_act[key] = [_num(row[c]) if c < len(row) else None for c, _ in act_cols]
                    annual_est[key] = [_num(row[c]) if c < len(row) else None for c, _ in est_cols]
                    found_keys.add(key)
                    break
        break  # 첫 번째 매칭만

    # ── 최종 조합
    d = {
        "meta": {
            "name": result.get("name", name),
            "code": code,
            "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        },
        "현재가":          result.get("현재가"),
        "베타":            result.get("베타"),
        "발행주식수_보통": int(result.get("발행주식수_보통") or 0),
        "발행주식수_우선": int(result.get("발행주식수_우선") or 0),
        "자기주식":        int(result.get("자기주식") or 0),
        "PER":             result.get("PER"),
        "12M_PER":         None,
        "업종_PER":        result.get("업종_PER"),
        "PBR":             annual_act.get("PBR", [None])[-1],
        "배당수익률":      industry.get("종목_배당"),
        "EV_EBITDA":       result.get("EV_EBITDA"),
        "annual": {
            "years":          act_years,
            "매출액":          annual_act.get("매출액", []),
            "영업이익":        annual_act.get("영업이익", []),
            "당기순이익":      annual_act.get("당기순이익", []),
            "지배주주순이익":  annual_act.get("지배주주순이익", []),
            "비지배주주순이익":annual_act.get("비지배주주순이익", []),
            "자산총계":        annual_act.get("자산총계", []),
            "부채총계":        annual_act.get("부채총계", []),
            "자본총계":        annual_act.get("자본총계", []),
            "지배주주지분":    annual_act.get("지배주주지분", []),
            "비지배주주지분":  annual_act.get("비지배주주지분", []),
            "자본금":          annual_act.get("자본금", []),
            "ROA":             annual_act.get("ROA", []),  # % 단위 그대로
            "ROE":  [v/100 if v is not None else None for v in annual_act.get("ROE", [])],
            "EPS":             annual_act.get("EPS", []),
            "BPS":             annual_act.get("BPS", []),
            "DPS":             annual_act.get("DPS", []),
            "PER":             annual_act.get("PER", []),
            "PBR":             annual_act.get("PBR", []),
            "배당수익률": [v/100 if v is not None else None for v in annual_act.get("배당수익률", [])],
        },
        "quarter": quarter,
        "consensus": {
            "years":          est_years,
            "매출액":          annual_est.get("매출액", []),
            "영업이익":        annual_est.get("영업이익", []),
            "당기순이익":      annual_est.get("당기순이익", []),
            "지배주주순이익":  annual_est.get("지배주주순이익", []),
            "지배주주지분":    annual_est.get("지배주주지분", []),
            "ROE":  [v/100 if v is not None else None for v in annual_est.get("ROE", [])],
            "EPS":             annual_est.get("EPS", []),
            "BPS":             annual_est.get("BPS", []),
            "DPS":             annual_est.get("DPS", []),
            "PER":             annual_est.get("PER", []),
            "PBR":             annual_est.get("PBR", []),
            "배당수익률": [v/100 if v is not None else None for v in annual_est.get("배당수익률", [])],
        },
        "industry": industry,
        "투자자": result.get("투자자", {}),   # ← 수급 데이터 반드시 포함
    }

    # ── SVD_Finance에서 단기차입금, 이익잉여금, 세전계속사업이익 수집
    try:
        fin_url  = (f"https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp"
                    f"?pGB=1&gicode=A{code}&MenuYn=Y&NewMenuID=103&stkGb=701")
        fin_r    = requests.get(fin_url, headers=HEADERS, timeout=12)
        fin_soup = BeautifulSoup(fin_r.text, "html.parser")

        def _get_row_vals(div_id: str, label: str) -> list:
            div = fin_soup.find("div", id=div_id)
            if not div: return []
            for tr in div.find_all("tr"):
                th = tr.find("th")
                if not th: continue
                if label in th.get_text(strip=True).replace(" ",""):
                    return [_num(td.get_text(strip=True)) for td in tr.find_all("td")]
            return []

        def _get_years(div_id: str) -> list:
            div = fin_soup.find("div", id=div_id)
            if not div: return []
            ths = div.find("thead").find_all("th") if div.find("thead") else []
            return [th.get_text(strip=True) for th in ths[1:] if "/" in th.get_text()]

        # 재무상태표 연간 (divDaechaY)
        bs_years     = _get_years("divDaechaY")
        short_borrow = _get_row_vals("divDaechaY", "단기차입금")
        retained     = _get_row_vals("divDaechaY", "이익잉여금")

        # 손익계산서 연간 (divSonikY)
        pretax_profit = _get_row_vals("divSonikY", "세전계속사업이익")
        if not pretax_profit:
            pretax_profit = _get_row_vals("divSonikY", "법인세비용차감전")

        # 이자비용 (divSonikY) — 여러 행명 후보 시도
        interest_exp = _get_row_vals("divSonikY", "이자비용")
        if not interest_exp:
            interest_exp = _get_row_vals("divSonikY", "금융비용")
        if not interest_exp:
            interest_exp = _get_row_vals("divSonikY", "이자비용및유사비용")

        # 현금흐름표 연간 — div ID 후보 순서대로 탐색
        _cf_div_ids = ["divCfY", "divCashY", "divCashFlowY"]
        cf_div_id = None
        for _did in _cf_div_ids:
            if fin_soup.find("div", id=_did):
                cf_div_id = _did
                break

        if cf_div_id:
            cf_years = _get_years(cf_div_id)
            # 영업CF: 여러 행명 후보
            op_cf = []
            for _lbl in ("영업활동으로인한현금흐름", "영업현금흐름", "영업활동현금흐름",
                         "I.영업활동으로인한현금흐름", "I.영업활동현금흐름",
                         "1.영업활동으로인한현금흐름"):
                op_cf = _get_row_vals(cf_div_id, _lbl)
                if op_cf: break
            # CAPEX: 여러 행명 후보 (FnGuide는 음수로 표기)
            capex = []
            for _lbl in ("유형자산의취득", "설비투자", "유형자산취득",
                         "유형자산의증가", "유형자산취득액", "토지및건물의취득"):
                capex = _get_row_vals(cf_div_id, _lbl)
                if capex: break
            # CAPEX 절대값 저장
            capex = [abs(v) if v is not None else None for v in capex]
        else:
            cf_years, op_cf, capex = [], [], []

        # 기준 연도: 재무상태표 연도 사용
        n = len(bs_years)
        cf_n = len(cf_years)

        d["finance"] = {
            "years":            bs_years,
            "단기차입금":       short_borrow[:n],
            "이익잉여금":       retained[:n],
            "세전계속사업이익": pretax_profit[:n],
            "이자비용":         interest_exp[:n],
            "영업CF":           op_cf[:cf_n],
            "CAPEX":            capex[:cf_n],
            "cf_years":         cf_years,
        }
        print(f"  Finance 수집: 단기차입금{short_borrow[-1:]}, 이익잉여금{retained[-1:]}, 세전{pretax_profit[-1:]}")
        print(f"  Finance 추가: 이자비용{interest_exp[-1:]}, 영업CF{op_cf[-1:]}, CAPEX{capex[-1:]}")
    except Exception as e:
        print(f"  [Finance 수집 실패] {e}")
        d["finance"] = {}

    return d


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("name")
    parser.add_argument("code")
    parser.add_argument("out")
    args = parser.parse_args()

    d = collect(args.name, args.code)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n  ✓ 저장: {args.out}")
    print(f"  현재가:          {d['현재가']}")
    print(f"  베타:            {d['베타']}")
    print(f"  발행주식수(보통): {d['발행주식수_보통']:,}")
    print(f"  자기주식:        {d['자기주식']:,}")
    print(f"  PER:             {d['PER']}")
    print(f"  업종_PER:        {d['업종_PER']}")
    print(f"  PBR:             {d['PBR']}")
    print(f"  EV_EBITDA:       {d['EV_EBITDA']}")
    print(f"  연도(실적):      {d['annual']['years']}")
    print(f"  ROE(실적):       {d['annual']['ROE']}")
    print(f"  EPS(실적):       {d['annual']['EPS']}")
    print(f"  배당수익률:      {d['annual']['배당수익률']}")
    print(f"  연도(컨센):      {d['consensus']['years']}")
    print(f"  ROE(컨센):       {d['consensus']['ROE']}")
    print(f"  업종비교:        {d['industry']}")
