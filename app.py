# VERSION: 20260319-clean
# -*- coding: utf-8 -*-
"""
app.py  -  S-RIM 웹 UI (RIM자동화)
Flask 로컬 서버: http://127.0.0.1:5000
"""

import sys, os, json, threading, importlib
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify

BASE  = Path(__file__).parent
_tmpl = BASE / "templates"
if not _tmpl.exists():
    _tmpl = BASE
app   = Flask(__name__, template_folder=str(_tmpl))

# ── 잡 상태 관리 ─────────────────────────────────────────
_status = {}
_lock   = threading.Lock()


# ── 유틸 ─────────────────────────────────────────────────

def _get_stock_change(code: str) -> dict:
    try:
        sys.path.insert(0, str(BASE))
        import stock_search as _ss
        importlib.reload(_ss)
        return _ss.get_stock_ohlcv(code)
    except Exception as e:
        return {"rate": 0, "diff": 0, "up": True}


# ── 백그라운드 잡 ─────────────────────────────────────────

def _run_job(job_id: str, stock_name: str):
    import traceback

    try:
        with _lock:
            _status[job_id] = {"state": "running", "msg": "FnGuide 데이터 수집 중..."}

        sys.path.insert(0, str(BASE))

        # 1) 종목 검색
        import stock_search as _ss
        importlib.reload(_ss)
        found_name, code = _ss.resolve_stock(stock_name)
        if not code:
            with _lock:
                _status[job_id] = {"state": "error", "msg": f"종목을 찾을 수 없습니다: {stock_name}"}
            return

        # 시장 구분
        market = "KOSPI"

        with _lock:
            _status[job_id]["msg"] = "FnGuide 수집 중..."

        # 2) FnGuide 수집
        import fnguide_collector_v4 as _col
        importlib.reload(_col)
        try:
            data = _col.collect(found_name, code)
        except Exception as e:
            err_msg = str(e)
            # 타임아웃 / 연결오류 → 사용자 친화적 메시지
            if "Timeout" in type(e).__name__ or "timed out" in err_msg or "재시도 실패" in err_msg:
                err_msg = f"FnGuide 서버 응답 시간 초과 (3회 재시도 실패)\n잠시 후 다시 검색해 주세요."
            elif "ConnectionError" in type(e).__name__ or "Connection" in err_msg:
                err_msg = f"FnGuide 서버 연결 실패\n인터넷 연결을 확인하거나 잠시 후 다시 시도해 주세요."
            with _lock:
                _status[job_id] = {"state": "error", "msg": err_msg}
            return

        # 시장 정보
        try:
            m = data.get("meta", {}).get("market")
            if m:
                market = m
        except Exception:
            pass
        data["_market"] = market

        # 등락률
        data["_stock_chg"] = _get_stock_change(code)

        # 공시 이벤트 (실패해도 계속)
        data["_events"] = []
        try:
            import event_watcher as _ew
            importlib.reload(_ew)
            ev_map = _ew.get_event_map()
            data["_events"] = ev_map.get(code, [])
        except Exception:
            pass

        # JSON 저장 전 None 값 보정 (금융주 등 최신연도 BPS/ROE가 None인 경우)
        def _fill_none_prev(lst):
            """None 항목을 직전 유효값으로 대체"""
            result, last = [], None
            for v in (lst or []):
                if v is not None: last = v
                result.append(last)
            return result

        ann_data = data.get("annual", {})
        for key in ["BPS", "ROE", "자본총계", "지배주주지분", "자본금", "EPS"]:
            if key in ann_data and isinstance(ann_data[key], list):
                ann_data[key] = _fill_none_prev(ann_data[key])

        # BPS 전부 None인 경우 → 지배주주지분으로 역산
        bps_list = ann_data.get("BPS", [])
        if all(v is None for v in bps_list) and bps_list:
            cap_list = ann_data.get("지배주주지분", [])
            shares = (data.get("발행주식수_보통", 0) or 0) - (data.get("자기주식", 0) or 0)
            if shares > 0 and cap_list:
                ann_data["BPS"] = [
                    round(v * 1e8 / shares) if v is not None else None
                    for v in cap_list
                ]
                ann_data["BPS"] = _fill_none_prev(ann_data["BPS"])

        work_dir = BASE / "WORK"
        work_dir.mkdir(exist_ok=True)
        json_path = work_dir / f"{code}_{found_name}.json"
        with open(str(json_path), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # annual.years 빈 리스트 종목 → 계산 불가 (거래정지·데이터없음)
        ann_years = data.get("annual", {}).get("years", [])
        if not ann_years:
            raise ValueError("실적 데이터 없음 (거래정지·상장예정·데이터 미수집 종목)")

        with _lock:
            _status[job_id]["msg"] = "S-RIM 계산 중..."

        # 3) ke 계산 + S-RIM Python 직접 계산 (PRIMARY — Excel 불필요)
        ke = _compute_ke(data)
        apt, sell, buy, meta = _srim_python(data, ke)

        # 4) 결과 빌드 (Excel 의존 없음)
        result = _build_result(data, found_name, code, ke, apt, sell, buy, meta)

        # 5) Excel 생성 (best-effort, 다운로드용 — 계산에 사용 안 함)
        try:
            with _lock:
                _status[job_id]["msg"] = "Excel 파일 생성 중..."
            import srim_filler_v4 as _filler
            importlib.reload(_filler)
            template = BASE / "S-RIM_V33_ForwardBlock.xlsx"
            out_dir  = BASE / "OUTPUT"
            out_dir.mkdir(exist_ok=True)
            today    = datetime.now().strftime("%Y%m%d")
            out_path = out_dir / f"{found_name}_SRIM_{today}.xlsx"
            _filler.fill(str(template), str(json_path), str(out_path))
            result["xlsx"] = str(out_path)
            print("  ✓ Excel 생성 완료 (다운로드용)")
        except Exception as e:
            print(f"  [Excel 생성 실패, 무시] {e}")
            result["xlsx"] = ""

        with _lock:
            _status[job_id] = {"state": "done", "result": result}

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[오류] job {job_id}: {e}\n{tb}")
        with _lock:
            _status[job_id] = {"state": "error", "msg": str(e), "trace": tb}


def _compute_ke(data: dict) -> float:
    """
    요구수익률(ke) = KIS BBB- 5년 수익률 (직접입력 모드)
    Excel 결과!D14 (C14="적용" 상태) 와 동일 방식:
      ke = BBB- 5년 수익률  (beta 적용 없음, 직접 사용)
    KIS 수집 실패 시 기본값 0.1031 (10.31%) 사용
    """
    try:
        sys.path.insert(0, str(BASE))
        import kis_collector as _kis
        importlib.reload(_kis)
        ke = _kis.get_bbb_minus_5yr()
    except Exception as e:
        print(f"  [BBB- 수집 실패, 기본값 사용] {e}")
        ke = 0.1031  # 기본값: 10.31%
    ke = round(max(0.05, min(ke, 0.25)), 4)
    print(f"  [ke 계산] BBB- 5년 수익률 = {ke*100:.2f}%")
    return ke


def _srim_python(data: dict, ke: float, 배열: str = "") -> tuple:
    """
    S-RIM 적정주가 Python 직접 계산 (Excel 수식 완전 재현 버전)

    Excel과의 차이점 수정:
      1. ROE:      실적3년+분기+컨센(1:2:3:3:3) / 컨센없으면(1:2:3:3) 가중평균
      2. equity_0: 컨센서스 1년차 지배주주지분 우선 (없으면 연간 최신)
      3. shares:   보통주 + 우선주 - 자기주식 (우선주 포함)
      4. fade^t:   초과이익 감쇄 지수 t 시작 (기존 t-1 → t)
      5. 기준시점-현재 할인: 최근결산 연말 → 오늘 경과일 반영
      6. 정배열/역배열 매핑: C28/C29/C30 엑셀 동일 배정

    반환: (적정주가, 매도가격, 매수가격) int (원)
    """
    from datetime import date as _date

    ann = data.get("annual", {})
    con = data.get("consensus", {})
    qtr = data.get("quarter", {})

    def _last(lst):
        return next((v for v in reversed(lst or []) if v is not None), None)

    def _first(lst):
        return next((v for v in (lst or []) if v is not None), None)

    def _norm(v):
        """ROE 값을 소수 단위로 정규화"""
        if v is None: return None
        return v/100 if abs(v) > 2 else v

    # ── 1. 분기 trailing 4Q ROE 계산 ─────────────────────────────────────
    def _trailing_4q_roe() -> float | None:
        """최근4분기 누적 ROE. Q4결산이면 연간ROE 그대로, 아니면 추정."""
        q_years = qtr.get("years", [])
        q_ni    = qtr.get("지배주주순이익", [])
        q_eq    = qtr.get("지배주주지분", [])

        # 실적 분기만 (E 제외)
        actuals = [(ni, eq, yr) for ni, eq, yr in zip(q_ni, q_eq, q_years)
                   if ni is not None and "(E)" not in str(yr)]

        def _last_ann():
            return _norm(_last(ann.get("ROE", [])))

        if not actuals:
            return _last_ann()

        last_yr_str = str(actuals[-1][2])
        # Q4(12월) 결산이면 연간 ROE가 trailing 4Q와 동일
        if last_yr_str.endswith("/12"):
            return _last_ann()

        # Q1~Q3: trailing NI 합산 추정
        actual_ni = [ni for ni, eq, yr in actuals]
        n = len(actual_ni)
        ann_ni_list = [v for v in (ann.get("지배주주순이익", []) or []) if v is not None]
        if ann_ni_list and n < 4:
            # 부족 분기 = 직전 연간 NI / 4 × 부족수
            trailing_ni = sum(actual_ni) + (ann_ni_list[-1] / 4) * (4 - n)
        elif n >= 4:
            trailing_ni = sum(actual_ni[-4:])
        else:
            return _last_ann()

        eq_end = actuals[-1][1]
        ann_eq = [v for v in (ann.get("지배주주지분", []) or []) if v is not None]
        eq_begin = ann_eq[-2] if len(ann_eq) >= 2 else eq_end
        if not eq_end or eq_end <= 0:
            return _last_ann()
        return trailing_ni / ((eq_begin + eq_end) / 2)

    # ── 2. ROE 가중평균: 실적3년 + 분기 + 컨센(가장가까운1개년) ──────────
    # 컨센서스 있음: (1:2:3:3:3) / 12
    # 컨센서스 없음: (1:2:3:3)   /  9
    ann_roe_raw = ann.get("ROE", [])
    con_roe_raw = con.get("ROE", [])

    ann_vals = [_norm(v) for v in (ann_roe_raw or []) if v is not None]
    if not ann_vals:
        return 0, 0, 0, {}

    recent3 = ann_vals[-3:]
    w3 = list(range(1, len(recent3) + 1))  # 1, 2, 3

    q_roe_val = _trailing_4q_roe()
    q = q_roe_val if q_roe_val is not None else ann_vals[-1]  # proxy

    con1 = None
    for v in (con_roe_raw or []):
        if v:
            con1 = _norm(v)
            break

    if con1 is not None:
        # 컨센서스 있음: 실적3년 + 분기 + 컨센(1:2:3:3:3) / 12
        vals5 = recent3 + [q, con1]
        w5    = w3 + [3, 3]
        roe   = sum(v*wt for v, wt in zip(vals5, w5)) / sum(w5)
        print(f"  [ROE 가중평균] 컨센有 1:2:3:3:3 = {roe*100:.2f}%")
    else:
        # 컨센서스 없음: 실적3년 + 분기(1:2:3:3) / 9
        vals4 = recent3 + [q]
        w4    = w3 + [3]
        roe   = sum(v*wt for v, wt in zip(vals4, w4)) / sum(w4)
        print(f"  [ROE 가중평균] 컨센無 1:2:3:3 = {roe*100:.2f}%")

    if roe is None:
        return 0, 0, 0, {}

    # ── ROE 추세 ────────────────────────────────────────────────────────────
    _roe_trend_raw = [v for v in (ann_roe_raw or []) if v is not None]
    if len(_roe_trend_raw) >= 2:
        _추세 = "상승추세" if _roe_trend_raw[-1] >= _roe_trend_raw[-2] else "하락추세"
    else:
        _추세 = ""

    # ROE 방식 레이블
    _roe방식 = "가중평균" + ("(컨센있음)" if con1 is not None else "(컨센없음)")

    # ── 기준 지배주주지분 (equity_0): 컨센서스 1년차 우선, 없으면 연간 최신 ──
    # 컨센서스 있으면 1순위 모드 → 컨센서스 1년차 지배주주지분 사용
    # 컨센서스 없으면 가중평균 모드 → 최근 연간 지배주주지분 사용
    equity_0 = _first(con.get("지배주주지분", [])) or _last(ann.get("지배주주지분", []))
    if not equity_0 or equity_0 <= 0:
        return 0, 0, 0, {}

    # ke 형식 통일
    if ke and ke > 1:
        ke = ke / 100
    if not ke or ke <= 0:
        ke = 0.1031

    # ── 3. 발행주식수: 보통주 + 우선주 - 자기주식 ───────────────────────
    shares = ((data.get("발행주식수_보통") or 0) +
              (data.get("발행주식수_우선") or 0) -
              (data.get("자기주식") or 0))
    if shares <= 0:
        return 0, 0, 0, {}

    # ── 4. 기준시점-현재 할인: 최근결산 연말 → 오늘 경과일 ─────────────
    try:
        _ann_years = ann.get("years", [])
        last_year  = int(_ann_years[-1]) if _ann_years else _date.today().year - 1
        fiscal_end = _date(last_year, 12, 31)
        today      = _date.today()
        days_diff  = (fiscal_end - today).days   # 음수 = 결산 이미 지남 → 복리 증가
    except Exception:
        days_diff = 0

    def _calc(fade: float) -> float:
        """초과이익 지속계수 fade 로 RIM 계산, 억원 반환"""
        pv_total = 0.0
        eq = equity_0
        for t in range(1, 11):
            ni  = eq * roe                           # 지배주주순이익 (일정 ROE)
            ri  = eq * (roe - ke) * (fade ** t)      # 초과이익: (ROE-ke)×fade^t  ← 수정
            pv  = ri / (1 + ke) ** t                 # 현재가치
            pv_total += pv
            eq  += ni                                # 지배주주지분 성장
        rim_equity = equity_0 + pv_total             # 억원 (결산기준)
        # 기준시점-현재 할인: days_diff 음수이면 나눗셈이 복리 증가
        rim_equity /= (1 + ke) ** (days_diff / 365)
        return rim_equity

    try:
        v_지속   = _calc(1.0)
        v_10감소 = _calc(0.9)
        v_20감소 = _calc(0.8)

        def _p(v: float) -> int:
            return max(0, round(v * 1e8 / shares))

        p_지속   = _p(v_지속)
        p_10감소 = _p(v_10감소)
        p_20감소 = _p(v_20감소)

        # ── 5. 정배열/역배열 매핑 ─────────────────────────────────────
        # 배열 파라미터 없으면 excess ROE 부호로 추정
        #   정배열: ROE > ke  → C28=10%감소, C29=지속, C30=20%감소
        #   역배열: ROE ≤ ke  → C28=지속,   C29=20%감소, C30=지속
        _배열 = 배열 or ("정배열" if roe > ke else "역배열")
        if _배열 == "정배열":
            apt, sell, buy = p_10감소, p_지속, p_20감소
        else:
            apt, sell, buy = p_지속, p_20감소, p_지속

        print(f"  [S-RIM Python({_배열})] 지속={p_지속:,} / 10%감소={p_10감소:,} / 20%감소={p_20감소:,}")
        print(f"    → 적정={apt:,} / 매도={sell:,} / 매수={buy:,}  (days_diff={days_diff})")
        meta = {
            "roe":    roe,
            "ke":     ke,
            "배열":   _배열,
            "추세":   _추세,
            "q_roe":  q_roe_val,
            "roe방식": _roe방식,
        }
        return apt, sell, buy, meta

    except Exception as e:
        print(f"  [S-RIM Python 오류] {e}")
        return 0, 0, 0, {}


def _build_result(data: dict, name: str, code: str,
                  ke: float, apt: int, sell: int, buy: int, meta: dict) -> dict:
    """
    S-RIM 결과 dict 빌드 — Excel 파일 의존 없음.
    ke, apt/sell/buy, meta 는 _compute_ke() + _srim_python() 에서 전달받음.
    """
    ann = data.get("annual", {})
    con = data.get("consensus", {})
    qtr = data.get("quarter", {})
    ind = data.get("industry", {})

    현재가   = data.get("현재가", 0) or 0
    # 현재가=0 → 상장폐지/거래정지 종목 → 에러 대신 안내 결과 반환
    if not 현재가:
        return {
            "name": name, "code": code,
            "market": data.get("_market", "KOSPI"),
            "현재가": 0,
            "거래정지": True,
            "거래정지_메시지": "현재가 정보가 없습니다. 거래정지·상장폐지·매매거래중단 종목일 수 있습니다.",
            "change_rate": 0, "change_diff": 0, "change_up": True,
            "collected_at": data.get("meta", {}).get("collected_at", ""),
            "적정주가": 0, "매수가격": 0, "매도가격": 0,
            "현재가대비": 0, "roe방식": "", "roe추정": 0,
            "할인율": 10.31, "추세": "", "배열": "", "roe수준": "",
            "roe_history": [], "q_roe": None, "con_history": [],
            "op_history": [], "지표": {}, "risk": [], "xlsx": "",
            "events": {"positive": [], "negative": []},
        }

    # ── 금융/보험/지주 업종 감지 (표시 목적) ─────────────────
    _is_finance = not bool(ann.get("매출액", []))

    def _last(lst):
        return next((v for v in reversed(lst or []) if v is not None), None)

    # ── S-RIM 결과: Python 계산값 직접 사용 ─────────────────
    적정주가 = max(0, apt)
    매도가격 = sell
    매수가격 = buy

    # meta에서 파생 지표 추출
    _roe_from_meta = meta.get("roe")           # 소수 (0.1111 등)
    _ke_from_meta  = meta.get("ke") or ke      # 소수
    roe방식  = meta.get("roe방식", "가중평균")
    배열     = meta.get("배열", "")
    추세     = meta.get("추세", "")
    q_roe_meta = meta.get("q_roe")             # 소수 or None

    # roe추정: meta에서 우선, 없으면 연간 최신 ROE
    _roe_last_dec = _last(ann.get("ROE", []))  # 소수 (0.0841 등)
    roe추정  = _roe_from_meta if _roe_from_meta is not None else (_roe_last_dec or 0)
    할인율   = ke                               # _compute_ke() 결과 (소수)

    # roe수준
    roe수준  = ""
    if _roe_from_meta is not None:
        roe수준 = "ROE>요구수익" if _roe_from_meta > ke else "ROE<요구수익"

    # q_roe (% 변환)
    h22 = q_roe_meta if q_roe_meta is not None else 0

    현재가대비 = round((현재가 / 적정주가 - 1) * 100, 1) if 적정주가 else 0

    # ── 적정주가 계산불가 판단 ────────────────────────────
    # 자본잠식 심화 / 극적자 종목은 S-RIM 결과가 0 또는 음수로 수렴
    _roe_last = _last(ann.get("ROE", []))
    _계산불가 = False
    _계산불가_메시지 = ""
    if 적정주가 == 0:
        if _roe_last is not None and _roe_last < -0.5:  # ROE -50% 미만 (심각한 자본잠식)
            _계산불가 = True
            _계산불가_메시지 = f"자본잠식 심화로 S-RIM 계산불가 (ROE {round(_roe_last*100,1)}%)"
        elif not ann.get("years"):
            _계산불가 = True
            _계산불가_메시지 = "실적 데이터가 없어 계산불가 (신규상장·데이터미수집 종목)"
        else:
            _계산불가 = True
            _계산불가_메시지 = "ROE가 요구수익률보다 낮아 S-RIM 적정주가 산출불가"

    # 추세/배열/ROE수준 폴백
    if not 추세:
        vals = [v for v in ann.get("ROE", [])[-3:] if v is not None]
        if len(vals) >= 2:
            추세 = "상승추세" if vals[-1] >= vals[-2] else "하락추세"
    if not 배열:
        vals = [v for v in ann.get("ROE", [])[-3:] if v is not None]
        if len(vals) >= 2:
            배열 = "정배열" if vals[-1] >= vals[0] else "역배열"
    if not roe수준:
        roe수준 = "ROE>요구수익" if (roe추정 or 0) > (할인율 or 0.1) else "ROE<요구수익"

    # ROE 추이
    roe_years  = ann.get("years", [])
    roe_values = ann.get("ROE", [])
    roe_history = [{"year": y, "roe": round((v or 0) * 100, 2)}
                   for y, v in zip(roe_years, roe_values)]

    q_roe = round(h22 * 100, 2) if isinstance(h22, (int, float)) and h22 else None

    con_years   = con.get("years", [])[:2]
    con_roes    = [round((v or 0) * 100, 2) for v in con.get("ROE", [])[:2]]
    con_history = [{"year": y + "E", "roe": r} for y, r in zip(con_years, con_roes)]

    발행주식수 = data.get("발행주식수_보통", 0) or 0
    시가총액  = round((현재가 or 0) * (발행주식수 or 0) / 1e8)
    market   = data.get("meta", {}).get("market") or data.get("_market", "KOSPI")

    risk = _check_risk(ann, 현재가, 발행주식수, market, data.get("finance", {}))
    chg  = data.get("_stock_chg") or _get_stock_change(code)

    return {
        "name": name, "code": code, "market": market,
        "change_rate": chg.get("rate") or 0,
        "change_diff": chg.get("diff") or 0,
        "change_up":   chg.get("up", True),
        "year_high":   chg.get("year_high") or 0,
        "year_low":    chg.get("year_low") or 0,
        "collected_at": data.get("meta", {}).get("collected_at", ""),
        "현재가":    현재가,
        "거래정지":  False,
        "계산불가":  _계산불가,
        "계산불가_메시지": _계산불가_메시지,
        "적정주가":  적정주가,
        "매수가격":  매수가격,
        "매도가격":  매도가격,
        "현재가대비": round(현재가대비, 1),
        "roe방식":   roe방식,
        "roe추정":   round((roe추정 or 0) * 100, 2),   # 소수→% 변환 (금융업 포함 통일)
        "할인율":    round(할인율 * 100, 2),
        "추세":      str(추세),
        "배열":      str(배열),
        "roe수준":   str(roe수준),
        "roe_history": roe_history,
        "q_roe":      q_roe,
        "con_history": con_history,
        "op_history":  [{"year": y, "op": round(v or 0)}
                        for y, v in zip(ann.get("years",[]), ann.get("영업이익",[]))],
        "지표": _build_indicators_v2(data, ann, ind, roe추정, 시가총액, ke),
        "민감도": _build_sensitivity(data, ke, meta),
        "risk": risk,
        "xlsx": "",  # _run_job에서 Excel 생성 후 덮어씀
        "events": {
            "positive": [e for e in data.get("_events", []) if e.get("type") == "positive"],
            "negative": [e for e in data.get("_events", []) if e.get("type") == "negative"],
        },
    }


def _build_sensitivity(data: dict, ke: float, meta: dict) -> dict:
    """
    ④ 적정주가 민감도 분석: ke ±1%, ±2% 변동 시 적정주가 변화량 반환
    배열(정배열/역배열)은 현재 ke 기준으로 고정 — 경계값에서 결과 왜곡 방지
    """
    try:
        fixed_배열 = meta.get("배열", "")  # 현재 배열 고정
        result = {}
        for delta_pct in [-2, -1, 1, 2]:
            ke_adj = round(ke + delta_pct / 100, 4)
            ke_adj = max(0.03, min(ke_adj, 0.30))
            apt_adj, _, _, _ = _srim_python(data, ke_adj, 배열=fixed_배열)
            result[f"ke{'+' if delta_pct>0 else ''}{delta_pct}%"] = apt_adj
        return result
    except Exception:
        return {}


def _check_risk(ann: dict, 현재가: float, 발행주식수: int,
                market: str, finance: dict = None) -> list:
    finance       = finance or {}
    매출액_list   = ann.get("매출액", [])
    영업이익_list = ann.get("영업이익", [])
    순이익_list   = ann.get("당기순이익", [])
    자본총계_list = ann.get("자본총계", [])
    자본금_list   = ann.get("자본금", [])

    def _last(lst):
        if not lst: return None
        for v in reversed(lst):
            if v is not None: return v
        return None
    매출액   = _last(매출액_list)
    영업이익 = _last(영업이익_list) or 0
    자본총계 = _last(자본총계_list) or 0
    자본금   = _last(자본금_list) or 0
    시가총액 = round((현재가 or 0) * (발행주식수 or 0) / 1e8)
    매출액_없음 = 매출액 is None
    매출액 = 매출액 or 0

    items = []
    is_kospi = (market == "KOSPI")

    # 영업이익 연속 손실 계산
    op_vals  = [v for v in 영업이익_list if v is not None]
    loss_yrs = 0
    for v in reversed(op_vals):
        if v < 0: loss_yrs += 1
        else: break

    if is_kospi:
        잠식률 = (자본금 - 자본총계) / 자본금 if 자본금 else -99
        if   잠식률 >= 1.0: s, m = "danger", f"전액잠식  (자본총계 {자본총계:,.0f}억원)"
        elif 잠식률 >= 0.5: s, m = "warn",   f"50% 이상 잠식  (잠식률 {잠식률*100:.0f}%)"
        else:               s, m = "safe",   f"해당없음  (자본총계 {자본총계:,.0f}억원)"
        items.append({"name":"자본잠식","status":s,"msg":m,"std":"자본금 50% 이상 잠식 시 관리종목"})

        if 매출액_없음:
            s, m = "safe", "해당없음 (금융업종 매출 미집계)"
        elif 매출액 < 300:
            s, m = "danger", f"{매출액:,.0f}억원  (기준 300억원 미만)"
        else:
            s, m = "safe", f"{매출액:,.0f}억원  (기준 300억원 이상)"
        items.append({"name":"매출액","status":s,"msg":m,"std":"연간 매출 300억 미만 시 관리종목"})

        if   시가총액 < 500: s, m = "danger", f"{시가총액:,}억원  (기준 500억원 미만)"
        else:                s, m = "safe",   f"{시가총액:,}억원  (기준 500억원 이상)"
        items.append({"name":"시가총액","status":s,"msg":m,"std":"30거래일 연속 500억 미만 시 관리종목"})

        if 영업이익 < 0:
            if   loss_yrs >= 3: s, m = "danger", f"{영업이익:,.0f}억원  ({loss_yrs}년 연속 영업적자)"
            elif loss_yrs >= 2: s, m = "warn",   f"{영업이익:,.0f}억원  ({loss_yrs}년 연속 영업적자)"
            else:               s, m = "warn",   f"{영업이익:,.0f}억원  (영업적자 {loss_yrs}년차)"
        else:
            s, m = "safe", f"{영업이익:,.0f}억원  (흑자)"
        items.append({"name":"영업이익","status":s,"msg":m,
                      "std":"연속 영업적자 시 상장적격성 심사","loss_yrs":loss_yrs})

    else:  # KOSDAQ
        if   매출액 < 30:   s, m = "danger", f"{매출액:,.0f}억원  (기준 30억원 미만)"
        elif 매출액 < 50:   s, m = "warn",   f"{매출액:,.0f}억원  (기준 30억원 근접)"
        else:               s, m = "safe",   f"{매출액:,.0f}억원  (기준 30억원 이상)"
        items.append({"name":"매출액","status":s,"msg":m,"std":"연간 매출 30억 미만 시 관리종목"})

        neg_cnt  = sum(1 for v in 순이익_list[-3:] if v and v < 0)
        손실초과 = any(abs(n) > abs(e)*0.5 and abs(n) > 10
                    for n, e in zip(순이익_list[-3:], 자본총계_list[-3:])
                    if n and e and n < 0)
        if   neg_cnt >= 2 and 손실초과: s, m = "danger", f"최근 3년 내 {neg_cnt}회 대규모 손실"
        elif neg_cnt >= 1:              s, m = "warn",   f"최근 3년 내 {neg_cnt}회 손실"
        else:                           s, m = "safe",   "해당없음"
        items.append({"name":"법인세차감전 계속사업손실","status":s,"msg":m,
                      "std":"3년 내 2회 이상 자기자본 50% 초과 손실"})

        잠식률 = (자본금 - 자본총계) / 자본금 if 자본금 else -99
        if   자본총계 <= 0:  s, m = "danger", f"전액잠식  (자본총계 {자본총계:,.0f}억원)"
        elif 잠식률 >= 0.5:  s, m = "warn",   f"50% 이상 잠식  (잠식률 {잠식률*100:.0f}%)"
        else:                s, m = "safe",   f"해당없음  (자본총계 {자본총계:,.0f}억원)"
        items.append({"name":"자본잠식","status":s,"msg":m,"std":"자본잠식률 50% 이상 시 관리종목"})

        if   자본총계 < 10:  s, m = "danger", f"{자본총계:,.0f}억원  (기준 10억원 미만)"
        else:                s, m = "safe",   f"{자본총계:,.0f}억원  (기준 10억원 이상)"
        items.append({"name":"자기자본 미달","status":s,"msg":m,"std":"자기자본 10억 미만 시 관리종목"})

        if   시가총액 < 40:  s, m = "danger", f"{시가총액:,}억원  (기준 40억원 미만)"
        else:                s, m = "safe",   f"{시가총액:,}억원  (기준 40억원 이상)"
        items.append({"name":"시가총액","status":s,"msg":m,"std":"30거래일 연속 40억 미만 시 관리종목"})

        if 영업이익 < 0:
            if   loss_yrs >= 3: s, m = "danger", f"{영업이익:,.0f}억원  ({loss_yrs}년 연속 영업적자)"
            elif loss_yrs >= 2: s, m = "warn",   f"{영업이익:,.0f}억원  ({loss_yrs}년 연속 영업적자)"
            else:               s, m = "warn",   f"{영업이익:,.0f}억원  (영업적자 {loss_yrs}년차)"
        else:
            s, m = "safe", f"{영업이익:,.0f}억원  (흑자)"
        items.append({"name":"영업이익","status":s,"msg":m,
                      "std":"연속 영업적자 시 상장적격성 심사","loss_yrs":loss_yrs})

    # 세전계속사업이익 (finance 데이터 있을 때만, 금융주 제외)
    pretax_list = [v for v in finance.get("세전계속사업이익", []) if v is not None]
    if pretax_list and not 매출액_없음:  # 금융주 제외
        pretax_loss = sum(1 for v in pretax_list[-3:] if v < 0)
        pretax_last = pretax_list[-1]
        if   pretax_loss >= 2: s, m = "danger", f"{pretax_last:,.0f}억원  (최근 {pretax_loss}년 손실)"
        elif pretax_loss == 1: s, m = "warn",   f"{pretax_last:,.0f}억원  (최근 1년 손실)"
        else:                  s, m = "safe",   f"{pretax_last:,.0f}억원  (이익)"
        items.append({"name":"세전계속사업이익","status":s,"msg":m,
                      "std":"법인세비용차감전 계속사업이익 손실 여부"})

    # 단기차입금 vs 이익잉여금 (금융주 제외 — 금융업은 차입금이 영업 본질)
    sb_list = [v for v in finance.get("단기차입금", []) if v is not None]
    re_list = [v for v in finance.get("이익잉여금", []) if v is not None]
    if sb_list and re_list and not 매출액_없음:  # 금융주 제외
        sb = sb_list[-1]; re = re_list[-1]
        if   sb > re:        s, m = "danger", f"단기차입금 {sb:,.0f}억 > 이익잉여금 {re:,.0f}억"
        elif sb > re * 0.7:  s, m = "warn",   f"단기차입금 {sb:,.0f}억 (이익잉여금의 {sb/re*100:.0f}%)"
        else:                s, m = "safe",   f"단기차입금 {sb:,.0f}억 ≤ 이익잉여금 {re:,.0f}억"
        items.append({"name":"단기차입금/이익잉여금","status":s,"msg":m,
                      "std":"단기차입금 > 이익잉여금 시 유동성 위험"})

    # ── 고PBR 경고 (금융주 제외) ──────────────────────────
    if not 매출액_없음:
        bps_list = ann.get("BPS", [])
        bps_last = next((v for v in reversed(bps_list) if v), None)
        if bps_last and bps_last > 0 and 현재가 and 현재가 > 0:
            pbr = 현재가 / bps_last
            if pbr >= 15:
                s, m = "danger", f"PBR {pbr:.1f}배 — S-RIM 신뢰도 낮음 (순자산 대비 현재가 과도)"
            elif pbr >= 10:
                s, m = "warn",   f"PBR {pbr:.1f}배 — S-RIM 적정주가와 시장가 괴리 클 수 있음"
            else:
                s = None
            if s:
                items.append({"name": "고PBR",
                              "status": s, "msg": m,
                              "std": "PBR 10배 이상: 미래 성장 기대 반영 주가 — 재무제표 기반 평가 한계"})

    return items


# ── 라우트 ───────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search", methods=["POST"])
def api_search():
    import uuid
    body = request.get_json()
    name = (body or {}).get("name", "").strip()
    if not name:
        return jsonify({"error": "종목명을 입력하세요"}), 400
    job_id = str(uuid.uuid4())[:8]
    t = threading.Thread(target=_run_job, args=(job_id, name), daemon=True)
    t.start()
    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def api_status(job_id):
    with _lock:
        info = dict(_status.get(job_id, {"state": "unknown"}))
    return jsonify(info)


@app.route("/api/market")
def api_market():
    try:
        sys.path.insert(0, str(BASE))
        import stock_search as _ss
        importlib.reload(_ss)
        return jsonify(_ss.get_index_info())
    except Exception as e:
        return jsonify({
            "KOSPI":  {"index":0,"change":0,"diff":0,"up":True,"time":""},
            "KOSDAQ": {"index":0,"change":0,"diff":0,"up":True,"time":""},
        })


@app.route("/api/version_check")
def api_version():
    return jsonify({"version": "20260319-clean"})


# ── 포트폴리오 API ────────────────────────────────────────

PORTFOLIO_FILE = BASE / "portfolio.json"

def _load_pf():
    if PORTFOLIO_FILE.exists():
        try:
            return json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"holdings": []}

def _save_pf(data: dict):
    PORTFOLIO_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


@app.route("/api/portfolio", methods=["GET"])
def api_portfolio():
    """포트폴리오 목록 + 보유 종목 현재가 자동 조회"""
    pf = _load_pf()
    sys.path.insert(0, str(BASE))
    import stock_search as _ss
    importlib.reload(_ss)
    for h in pf["holdings"]:
        if h.get("status") == "holding":
            try:
                ov = _ss.get_stock_ohlcv(h["code"])
                h["current_price"] = ov.get("price", 0)
                h["rate"]          = ov.get("rate", 0)
                h["up"]            = ov.get("up", True)
            except Exception:
                h.setdefault("current_price", 0)
                h.setdefault("rate", 0)
                h.setdefault("up", True)
    return jsonify(pf)


@app.route("/api/portfolio/add", methods=["POST"])
def api_portfolio_add():
    """종목 추가"""
    import uuid
    body = request.get_json() or {}
    pf   = _load_pf()
    entry = {
        "id":                str(uuid.uuid4())[:8],
        "code":              body.get("code", ""),
        "name":              body.get("name", ""),
        "buy_price":         float(body.get("buy_price", 0)),
        "apt_price":         float(body.get("apt_price", 0)),
        "sell_target":       float(body.get("sell_target", 0)),
        "quantity":          int(body.get("quantity", 1)),
        "buy_date":          body.get("buy_date", datetime.now().strftime("%Y-%m-%d")),
        "ke":                body.get("ke"),
        "roe":               body.get("roe"),
        "배열":               body.get("배열", ""),
        "status":            "holding",
        "sell_date":         None,
        "sell_actual_price": None,
        "memo":              body.get("memo", ""),
    }
    pf["holdings"].append(entry)
    _save_pf(pf)
    return jsonify({"ok": True, "id": entry["id"]})


@app.route("/api/portfolio/sell/<item_id>", methods=["POST"])
def api_portfolio_sell(item_id):
    """매도 처리 — 실제 매도가 기록"""
    body = request.get_json() or {}
    sell_price = body.get("sell_price")
    if not sell_price:
        return jsonify({"error": "매도가 필요"}), 400
    pf = _load_pf()
    for h in pf["holdings"]:
        if h["id"] == item_id:
            h["status"]            = "sold"
            h["sell_date"]         = datetime.now().strftime("%Y-%m-%d")
            h["sell_actual_price"] = float(sell_price)
            break
    _save_pf(pf)
    return jsonify({"ok": True})


@app.route("/api/portfolio/<item_id>", methods=["DELETE"])
def api_portfolio_delete(item_id):
    """항목 삭제"""
    pf = _load_pf()
    pf["holdings"] = [h for h in pf["holdings"] if h["id"] != item_id]
    _save_pf(pf)
    return jsonify({"ok": True})


def _build_indicators_v2(data, ann, ind, roe추정, 시가총액, ke=0.1031):
    def _last(lst):
        return next((v for v in reversed(lst or []) if v is not None), None)
    per=data.get("PER"); pbr=data.get("PBR"); ev=data.get("EV_EBITDA")
    eps=_last(ann.get("EPS",[])); bps=_last(ann.get("BPS",[]))
    roa=_last(ann.get("ROA",[]))
    # DPS: 마지막 연도 값 직접 참조 (None이면 해당연도 무배당)
    dps_list_raw = ann.get("DPS", [])
    dps = dps_list_raw[-1] if dps_list_raw else None  # 최근연도 값 (None=무배당)
    dps_last_valid = _last(dps_list_raw)              # 가장 최근 유효값 (배당성향 계산용)
    배당수익률=data.get("배당수익률")
    배당성향=round(dps_last_valid/eps*100,1) if dps_last_valid and eps and eps>0 else None
    dps_list=[v for v in (ann.get("DPS") or [])[-3:] if v is not None]
    배당여부=bool(dps and dps>0)           # 최근연도 배당 여부
    배당중단=bool(not dps and dps_last_valid)  # 과거엔 했는데 최근 중단
    배당3년연속=len(dps_list)>=3 and all(v>0 for v in dps_list)
    # ── 시가배당수익률 (현재가 기준 DPS) ──────────────────────
    현재가_배당 = data.get("현재가", 0) or 0
    con_dps_list = data.get("consensus", {}).get("DPS", [])
    con_dps1 = next((v for v in con_dps_list if v), None)   # 컨센서스 DPS 1년차

    시가배당수익률 = None
    예상시가배당수익률 = None
    try:
        if dps_last_valid and 현재가_배당 > 0:
            시가배당수익률 = round(dps_last_valid / 현재가_배당 * 100, 2)
        if con_dps1 and 현재가_배당 > 0:
            예상시가배당수익률 = round(con_dps1 / 현재가_배당 * 100, 2)
    except Exception:
        pass

    dcf_per_share=dcf_판정=None
    try:
        fin=data.get("finance",{})
        ann_oi=ann.get("영업이익",[])
        # 영업CF 우선, 없으면 세전이익, 없으면 영업이익 순으로 대체
        cfs=[v for v in (fin.get("영업CF") or []) if v and v>0]
        if not cfs:
            cfs=[v for v in (fin.get("세전계속사업이익") or []) if v and v>0]
        if not cfs:
            cfs=[v for v in (ann_oi or []) if v and v>0]
        shrx=data.get("발행주식수_보통",0) or 0
        if cfs and shrx>0 and ke>0:
            recent=cfs[-3:]; w=list(range(1,len(recent)+1))
            avg_cf=sum(v*wt for v,wt in zip(recent,w))/sum(w)
            fcf=avg_cf*0.7; g=min(ke*0.3,0.03)
            if ke>g:
                dcf_per_share=round(fcf/(ke-g)*1e8/shrx)
                현재가=data.get("현재가",0) or 0
                if 현재가>0 and dcf_per_share>0:
                    r=(현재가/dcf_per_share-1)*100
                    dcf_판정="저평가" if r<-20 else "고평가" if r>20 else "적정"
    except Exception:
        pass
    def _j(v,lo,hi,rev=False):
        if v is None: return None
        return "저평가" if (v<lo if not rev else v>hi) else "고평가" if (v>hi if not rev else v<lo) else "적정"
    inv=data.get("투자자",{})

    # ── 영업이익률 추세 ──────────────────────────────────────
    op_margin_msg = None
    try:
        op_list = ann.get("영업이익", [])
        rev_list = ann.get("매출액", [])
        if op_list and rev_list and any(v for v in rev_list if v):
            margins = [round(o/r*100,1) if o and r and r>0 else None
                       for o,r in zip(op_list, rev_list)]
            valid = [m for m in margins[-3:] if m is not None]
            if len(valid) >= 2:
                diff = round(valid[-1] - valid[0], 1)
                trend = "개선" if diff > 0 else "악화"
                op_margin_msg = f"영업이익률 {trend} ({valid[0]}%→{valid[-1]}%)"
    except Exception:
        pass

    # ── EPS 증감 (YoY) ──────────────────────────────────────
    eps_yoy_msg = None
    try:
        eps_vals = [v for v in (ann.get("EPS") or []) if v is not None]
        if len(eps_vals) >= 2:
            e_prev, e_last = eps_vals[-2], eps_vals[-1]
            if e_prev and e_prev > 0:
                yoy = round((e_last/e_prev-1)*100, 1)
                sign = "+" if yoy >= 0 else ""
                eps_yoy_msg = f"주당순이익(EPS) 전년 대비 {sign}{yoy}%"
            elif e_prev and e_prev < 0 and e_last > 0:
                eps_yoy_msg = "전년 적자 → 흑자 전환"
            elif e_prev and e_prev < 0:
                eps_yoy_msg = "전년도 적자 → 비교 불가"
    except Exception:
        pass

    # ── 컨센서스 EPS 성장률 ──────────────────────────────────
    con_eps_msg = None
    try:
        con_eps_list = data.get("consensus", {}).get("EPS", [])
        con_eps_1 = next((v for v in con_eps_list if v), None)
        eps_vals2 = [v for v in (ann.get("EPS") or []) if v is not None]
        e_last2 = eps_vals2[-1] if eps_vals2 else None
        if con_eps_1 and e_last2:
            if e_last2 > 0:
                con_yoy = round((con_eps_1/e_last2-1)*100, 1)
                sign = "+" if con_yoy >= 0 else ""
                con_eps_msg = f"내년 실적 전망 {sign}{con_yoy}% (애널리스트 예측)"
            elif e_last2 < 0 and con_eps_1 > 0:
                con_eps_msg = "내년 흑자 전환 기대 (애널리스트 예측)"
            elif e_last2 < 0 and con_eps_1 < 0:
                con_eps_msg = "내년에도 적자 지속 전망 (애널리스트 예측)"
    except Exception:
        pass
    bps_cagr = None
    try:
        bps_vals = [v for v in (ann.get("BPS") or []) if v and v > 0]
        if len(bps_vals) >= 2:
            n = min(len(bps_vals) - 1, 5)
            bps_cagr = round((bps_vals[-1] / bps_vals[-1-n]) ** (1/n) * 100 - 100, 1)
    except Exception:
        pass

    # ── 부채비율 ────────────────────────────────────────────
    부채비율 = None
    try:
         부채 = next((v for v in reversed(ann.get("부채총계") or []) if v), None)
         자본 = next((v for v in reversed(ann.get("자본총계") or []) if v), None)
         if 부채 and 자본 and 자본 > 0:
             부채비율 = round(부채 / 자본 * 100, 1)
    except Exception:
        pass

    # ── ① ROE 안정성 ────────────────────────────────────────
    roe_std = None
    roe_range = None
    roe_stability_msg = None
    try:
        import statistics as _stat
        roe_vals_raw = [v for v in (ann.get("ROE") or []) if v is not None]
        # 소수형(0.15) 또는 정수형(15.0) 통일 → %로 변환
        roe_vals_pct = [(v * 100 if abs(v) <= 2 else v) for v in roe_vals_raw]
        if len(roe_vals_pct) >= 2:
            roe_std   = round(_stat.stdev(roe_vals_pct), 2)
            roe_range = round(max(roe_vals_pct) - min(roe_vals_pct), 2)
            # 안정성 레이블 (표준편차 기준)
            if roe_std < 3:
                roe_stability_msg = f"안정 (표준편차 {roe_std:.1f}%p)"
            elif roe_std < 7:
                roe_stability_msg = f"보통 (표준편차 {roe_std:.1f}%p)"
            else:
                roe_stability_msg = f"변동 큼 (표준편차 {roe_std:.1f}%p)"
    except Exception:
        pass

    # ── ③ 이자보상배율 ──────────────────────────────────────
    이자보상배율 = None
    이자보상배율_msg = None
    try:
        fin = data.get("finance", {})
        interest_list = [v for v in (fin.get("이자비용") or []) if v is not None and v > 0]
        op_list_raw   = ann.get("영업이익", [])
        op_last       = _last(op_list_raw)
        interest_last = interest_list[-1] if interest_list else None
        if interest_last and interest_last > 0 and op_last is not None:
            이자보상배율 = round(op_last / interest_last, 2)
            if 이자보상배율 >= 5:
                이자보상배율_msg = f"{이자보상배율:.1f}배 (양호 — 이자 부담 낮음)"
            elif 이자보상배율 >= 1.5:
                이자보상배율_msg = f"{이자보상배율:.1f}배 (보통)"
            elif 이자보상배율 >= 1.0:
                이자보상배율_msg = f"{이자보상배율:.1f}배 (주의 — 이자 부담 높음)"
            else:
                이자보상배율_msg = f"{이자보상배율:.1f}배 (위험 — 영업이익으로 이자 미충당)"
    except Exception:
        pass

    # ── ② FCF (잉여현금흐름) 실계산 ────────────────────────
    fcf_actual      = None   # 영업CF - CAPEX (억원)
    fcf_per_share   = None   # 주당 FCF (원)
    fcf_yield       = None   # FCF 수익률 (현재가 대비)
    fcf_msg         = None
    try:
        fin2 = data.get("finance", {})
        op_cf_list  = [v for v in (fin2.get("영업CF") or []) if v is not None]
        capex_list  = [v for v in (fin2.get("CAPEX") or []) if v is not None]
        shrx2 = data.get("발행주식수_보통", 0) or 0
        현재가2 = data.get("현재가", 0) or 0
        if op_cf_list and capex_list and shrx2 > 0:
            # 공통 기간 최소값 기준
            n_fcf = min(len(op_cf_list), len(capex_list), 3)
            op_cf_r  = op_cf_list[-n_fcf:]
            capex_r  = capex_list[-n_fcf:]
            fcf_list = [o - c for o, c in zip(op_cf_r, capex_r)]
            # 가중평균 (최근 연도 높은 가중치)
            w_fcf = list(range(1, n_fcf + 1))
            fcf_avg = sum(f * w for f, w in zip(fcf_list, w_fcf)) / sum(w_fcf)
            fcf_actual = round(fcf_avg)
            fcf_per_share = round(fcf_avg * 1e8 / shrx2)
            if 현재가2 > 0 and fcf_per_share > 0:
                fcf_yield = round(fcf_per_share / 현재가2 * 100, 2)
                if fcf_yield >= 5:
                    fcf_msg = f"FCF수익률 {fcf_yield:.1f}% (양호)"
                elif fcf_yield >= 2:
                    fcf_msg = f"FCF수익률 {fcf_yield:.1f}% (보통)"
                elif fcf_yield > 0:
                    fcf_msg = f"FCF수익률 {fcf_yield:.1f}% (낮음)"
                else:
                    fcf_msg = f"FCF 음수 (잉여현금 창출 못함)"
            elif fcf_per_share is not None and fcf_per_share <= 0:
                fcf_msg = "FCF 음수 (잉여현금 창출 못함)"
        elif op_cf_list and shrx2 > 0:
            # CAPEX 없으면 영업CF * 0.7 추정 (기존 방식 유지)
            pass  # dcf_per_share 로직에서 이미 처리됨
    except Exception:
        pass

    # ── 베타 ────────────────────────────────────────────────
    베타 = data.get("베타")

    return {
        "PER":per,"업종PER":data.get("업종_PER"),"PBR":pbr,
        "배당":배당수익률,"업종ROE":ind.get("업종_ROE"),
        "KOSPI_ROE":ind.get("KOSPI_ROE"),
        "업종배당":ind.get("업종_배당"),"EVEBITDA":ev,
        "추정ROE":round(roe추정 if roe추정 > 1 else roe추정*100, 2),"시가총액":시가총액,
        "ROA":roa,"EPS":eps,"BPS":bps,"DPS":dps,
        "BPS성장률":bps_cagr,
        "베타":round(베타,2) if 베타 else None,
        "부채비율":부채비율,
        "op_margin_msg":op_margin_msg,
        "eps_yoy_msg":eps_yoy_msg,
        "con_eps_msg":con_eps_msg,
        "시가배당수익률":시가배당수익률,
        "예상시가배당수익률":예상시가배당수익률,
        "배당성향":배당성향,"배당여부":배당여부,"배당중단":배당중단,"배당3년연속":배당3년연속,
        "DCF":dcf_per_share,
        # ── ① ROE 안정성
        "ROE표준편차":roe_std,
        "ROE변동범위":roe_range,
        "ROE안정성":roe_stability_msg,
        # ── ③ 이자보상배율
        "이자보상배율":이자보상배율,
        "이자보상배율_msg":이자보상배율_msg,
        # ── ② FCF 실계산
        "FCF":fcf_actual,
        "FCF주당":fcf_per_share,
        "FCF수익률":fcf_yield,
        "FCF_msg":fcf_msg,
        "외국인순매수":inv.get("외국인_순매수"),"연기금순매수":inv.get("연기금_순매수"),
        "기관순매수":inv.get("기관_순매수"),"개인순매수":inv.get("개인_순매수"),
        "연기금5일합산":inv.get("연기금_5일합산"),
        "투자자날짜":inv.get("전일날짜",""),
        "투자자5일":inv.get("5일",[]),
        "PER판정":_j(per,10,25),"PBR판정":_j(pbr,1.0,3.0),
        "EV판정":_j(ev,6,15),"DCF판정":dcf_판정,
        "배당판정":_j(배당수익률,2.0,999,rev=True) if 배당수익률 else None,
    }


@app.route("/api/shutdown", methods=["POST"])
def api_shutdown():
    return jsonify({"ok": True})


@app.route("/api/stocklist")
def api_stocklist():
    """자동완성용 종목 리스트"""
    try:
        import json as _json
        cache = BASE / "WORK" / "stock_list.json"
        if cache.exists():
            raw = _json.loads(cache.read_text(encoding="utf-8"))
            result = []

            # 형식 1: {"cached_at":..., "data": {"종목명": ["코드","시장"], ...}}
            if isinstance(raw, dict) and "data" in raw:
                data = raw["data"]
                for name, val in data.items():
                    if isinstance(val, (list, tuple)) and len(val) >= 1:
                        result.append({
                            "name": name.strip(),
                            "code": val[0],
                            "market": val[1] if len(val) > 1 else ""
                        })
            # 형식 2: [{"name":..,"code":..,"market":..}, ...]
            elif isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        result.append({
                            "name": item.get("name",""),
                            "code": item.get("code",""),
                            "market": item.get("market","")
                        })
                    elif isinstance(item, (list, tuple)) and len(item) >= 2:
                        result.append({
                            "name": item[0], "code": item[1],
                            "market": item[2] if len(item) > 2 else ""
                        })
            return jsonify({"list": result, "count": len(result)})
        # 캐시 없으면 stock_search.get_stock_map() 으로 로딩
        sys.path.insert(0, str(BASE))
        import stock_search as _ss; importlib.reload(_ss)
        stock_map = _ss.get_stock_map()
        result = [{"name": n, "code": v[0], "market": v[1] if len(v) > 1 else ""}
                  for n, v in stock_map.items()]
        return jsonify({"list": result, "count": len(result)})
    except Exception as e:
        return jsonify({"list": [], "error": str(e)})


if __name__ == "__main__":
    print("=" * 50)
    print("  S-RIM 웹 UI 시작")
    print("  http://127.0.0.1:5000")
    print("=" * 50)

    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
