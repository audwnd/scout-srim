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
        data = _col.collect(found_name, code)

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

        # JSON 저장
        work_dir = BASE / "WORK"
        work_dir.mkdir(exist_ok=True)
        json_path = work_dir / f"{code}_{found_name}.json"
        with open(str(json_path), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        with _lock:
            _status[job_id]["msg"] = "S-RIM 계산 중..."

        # 3) 엑셀 계산
        import srim_filler_v4 as _filler
        importlib.reload(_filler)
        template = BASE / "S-RIM_V33_ForwardBlock.xlsx"
        out_dir  = BASE / "OUTPUT"
        out_dir.mkdir(exist_ok=True)
        today    = datetime.now().strftime("%Y%m%d")
        out_path = out_dir / f"{found_name}_SRIM_{today}.xlsx"
        _filler.fill(str(template), str(json_path), str(out_path))

        # 4) win32com 재계산
        try:
            import pythoncom, time
            pythoncom.CoInitialize()
            import win32com.client as win32
            xl  = win32.Dispatch("Excel.Application")
            xl.Visible      = False
            xl.DisplayAlerts= False
            wb  = xl.Workbooks.Open(str(out_path.resolve()))
            wb.Application.CalculateFull()
            wb.Save()
            wb.Close(False)
            xl.Quit()
            del wb, xl
            time.sleep(0.3)
            pythoncom.CoUninitialize()
            print("  ✓ win32com 재계산 완료")
        except Exception as e:
            print(f"  [win32com 오류] {e}")
            try: pythoncom.CoUninitialize()
            except: pass

        # 5) 결과 읽기
        result = _build_result(data, str(out_path), found_name, code)

        with _lock:
            _status[job_id] = {"state": "done", "result": result}

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[오류] job {job_id}: {e}\n{tb}")
        with _lock:
            _status[job_id] = {"state": "error", "msg": str(e), "trace": tb}


def _build_indicators_v2(data, ann, ind, roe추정, 시가총액, ke=0.1031):
    """주요 지표 + 배당성향 + 투자자별 순매수 + 배지"""
    def _last(lst):
        return next((v for v in reversed(lst or []) if v is not None), None)

    per = data.get("PER"); pbr = data.get("PBR"); ev = data.get("EV_EBITDA")
    eps = _last(ann.get("EPS",[])); bps = _last(ann.get("BPS",[]))
    roa = _last(ann.get("ROA",[])); dps = _last(ann.get("DPS",[]))
    배당수익률 = data.get("배당수익률")

    # 배당성향 = DPS / EPS × 100
    배당성향 = round(dps / eps * 100, 1) if dps and eps and eps > 0 else None

    # 배당 연속 여부
    dps_list = [v for v in (ann.get("DPS") or [])[-3:] if v is not None]
    배당여부   = bool(dps and dps > 0)
    배당3년연속 = len(dps_list) >= 3 and all(v > 0 for v in dps_list)

    # DCF (영업CF 기반)
    dcf_per_share = dcf_판정 = None
    try:
        fin = data.get("finance", {})
        cfs = [v for v in (fin.get("영업CF") or []) if v and v > 0]
        shrx = data.get("발행주식수_보통", 0) or 0
        if cfs and shrx > 0 and ke > 0:
            recent = cfs[-3:]; w = list(range(1, len(recent)+1))
            avg_cf = sum(v*wt for v,wt in zip(recent,w)) / sum(w)
            fcf = avg_cf * 0.7; g = min(ke * 0.3, 0.03)
            if ke > g:
                dcf_per_share = round(fcf / (ke - g) * 1e8 / shrx)
                현재가 = data.get("현재가", 0) or 0
                if 현재가 > 0 and dcf_per_share > 0:
                    r = (현재가 / dcf_per_share - 1) * 100
                    dcf_판정 = "저평가" if r < -20 else "고평가" if r > 20 else "적정"
    except Exception:
        pass

    def _j(v, lo, hi, rev=False):
        if v is None: return None
        ok = v < lo if not rev else v > hi
        ng = v > hi if not rev else v < lo
        return "저평가" if ok else "고평가" if ng else "적정"

    inv = data.get("투자자", {})

    return {
        "PER": per, "업종PER": data.get("업종_PER"), "PBR": pbr,
        "배당": 배당수익률, "업종ROE": ind.get("업종_ROE"),
        "업종배당": ind.get("업종_배당"), "EVEBITDA": ev,
        "추정ROE": round(roe추정 * 100, 2), "시가총액": 시가총액,
        "ROA": roa, "EPS": eps, "BPS": bps, "DPS": dps,
        "배당성향": 배당성향, "배당여부": 배당여부, "배당3년연속": 배당3년연속,
        "DCF": dcf_per_share,
        "외국인순매수": inv.get("외국인_순매수"),
        "기관순매수":   inv.get("기관_순매수"),
        "개인순매수":   inv.get("개인_순매수"),
        "외국인매수":   inv.get("외국인_매수"), "외국인매도": inv.get("외국인_매도"),
        "기관매수":     inv.get("기관_매수"),   "기관매도":   inv.get("기관_매도"),
        "개인매수":     inv.get("개인_매수"),   "개인매도":   inv.get("개인_매도"),
        "PER판정": _j(per, 10, 25), "PBR판정": _j(pbr, 1.0, 3.0),
        "EV판정":  _j(ev, 6, 15),  "DCF판정": dcf_판정,
        "배당판정": _j(배당수익률, 2.0, 999, rev=True) if 배당수익률 else None,
    }


def _build_result(data: dict, xlsx_path: str, name: str, code: str) -> dict:
    import openpyxl

    xlsx_vals = {}
    try:
        wb   = openpyxl.load_workbook(xlsx_path, data_only=True)
        ws결  = wb["결과"]
        for cell in ["C28","C29","C30","C20","D20","C17","H22","I21","F31","G31"]:
            xlsx_vals[cell] = ws결[cell].value
        wb.close()
    except Exception as e:
        print(f"  [엑셀 읽기 오류] {e}")

    def cv(coord, default=None):
        v = xlsx_vals.get(coord)
        if v is None or v == "":
            return default
        return v

    ann = data.get("annual", {})
    con = data.get("consensus", {})
    qtr = data.get("quarter", {})
    ind = data.get("industry", {})

    현재가   = data.get("현재가", 0) or 0
    적정주가  = round(cv("C28")) if cv("C28") else 0
    매도가격  = round(cv("C29")) if cv("C29") else 0
    매수가격  = round(cv("C30")) if cv("C30") else 0
    현재가대비 = (현재가 / 적정주가 - 1) * 100 if 적정주가 else 0
    roe방식  = cv("C20") or "가중평균"
    roe추정  = cv("D20") or 0
    할인율   = cv("C17") or 0
    추세    = cv("I21") or ""
    배열    = cv("F31") or ""
    roe수준  = cv("G31") or ""
    h22    = cv("H22") or 0

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

    q_roe = round(h22 * 100, 2) if isinstance(h22, float) else None

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
        "collected_at": data.get("meta", {}).get("collected_at", ""),
        "현재가":    현재가,
        "적정주가":  적정주가,
        "매수가격":  매수가격,
        "매도가격":  매도가격,
        "현재가대비": round(현재가대비, 1),
        "roe방식":   roe방식,
        "roe추정":   round(roe추정 * 100, 2),
        "할인율":    round(할인율 * 100, 2),
        "추세":      str(추세),
        "배열":      str(배열),
        "roe수준":   str(roe수준),
        "roe_history": roe_history,
        "q_roe":      q_roe,
        "con_history": con_history,
        "op_history":  [{"year": y, "op": round(v or 0)}
                        for y, v in zip(ann.get("years",[]), ann.get("영업이익",[]))],
        "지표": _build_indicators_v2(data, ann, ind, roe추정, 시가총액, cv("C17", 0.1031)),
        "risk": risk,
        "xlsx": str(xlsx_path),
        "events": {
            "positive": [e for e in data.get("_events", []) if e.get("type") == "positive"],
            "negative": [e for e in data.get("_events", []) if e.get("type") == "negative"],
        },
    }


def _check_risk(ann: dict, 현재가: float, 발행주식수: int,
                market: str, finance: dict = None) -> list:
    finance       = finance or {}
    매출액_list   = ann.get("매출액", [])
    영업이익_list = ann.get("영업이익", [])
    순이익_list   = ann.get("당기순이익", [])
    자본총계_list = ann.get("자본총계", [])
    자본금_list   = ann.get("자본금", [])

    def _last(lst): return (lst[-1] or 0) if lst else 0
    매출액   = _last(매출액_list)
    영업이익 = _last(영업이익_list)
    자본총계 = _last(자본총계_list)
    자본금   = _last(자본금_list)
    시가총액 = round((현재가 or 0) * (발행주식수 or 0) / 1e8)

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

        if   매출액 < 300:  s, m = "danger", f"{매출액:,.0f}억원  (기준 300억원 미만)"
        else:               s, m = "safe",   f"{매출액:,.0f}억원  (기준 300억원 이상)"
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

    # 세전계속사업이익 (finance 데이터 있을 때만)
    pretax_list = [v for v in finance.get("세전계속사업이익", []) if v is not None]
    if pretax_list:
        pretax_loss = sum(1 for v in pretax_list[-3:] if v < 0)
        pretax_last = pretax_list[-1]
        if   pretax_loss >= 2: s, m = "danger", f"{pretax_last:,.0f}억원  (최근 {pretax_loss}년 손실)"
        elif pretax_loss == 1: s, m = "warn",   f"{pretax_last:,.0f}억원  (최근 1년 손실)"
        else:                  s, m = "safe",   f"{pretax_last:,.0f}억원  (이익)"
        items.append({"name":"세전계속사업이익","status":s,"msg":m,
                      "std":"법인세비용차감전 계속사업이익 손실 여부"})

    # 단기차입금 vs 이익잉여금
    sb_list = [v for v in finance.get("단기차입금", []) if v is not None]
    re_list = [v for v in finance.get("이익잉여금", []) if v is not None]
    if sb_list and re_list:
        sb = sb_list[-1]; re = re_list[-1]
        if   sb > re:        s, m = "danger", f"단기차입금 {sb:,.0f}억 > 이익잉여금 {re:,.0f}억"
        elif sb > re * 0.7:  s, m = "warn",   f"단기차입금 {sb:,.0f}억 (이익잉여금의 {sb/re*100:.0f}%)"
        else:                s, m = "safe",   f"단기차입금 {sb:,.0f}억 ≤ 이익잉여금 {re:,.0f}억"
        items.append({"name":"단기차입금/이익잉여금","status":s,"msg":m,
                      "std":"단기차입금 > 이익잉여금 시 유동성 위험"})

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


@app.route("/api/shutdown", methods=["POST"])
def api_shutdown():
    return jsonify({"ok": True})


@app.route("/api/stocklist")
def api_stocklist():
    """자동완성용 종목 리스트"""
    try:
        sys.path.insert(0, str(BASE))
        import stock_search as _ss
        importlib.reload(_ss)
        # stock_search.py의 load_stock_list() 또는 검색 함수 활용
        cache = BASE / "WORK" / "stock_list.json"
        if cache.exists():
            import json as _json
            lst = _json.loads(cache.read_text(encoding="utf-8"))
            # 형식 통일: [{"name":..,"code":..,"market":..}, ...]
            result = []
            for item in lst:
                if isinstance(item, dict):
                    result.append(item)
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    result.append({"name": item[0], "code": item[1],
                                   "market": item[2] if len(item) > 2 else ""})
            return jsonify({"list": result})
        # 캐시 없으면 stock_search에서 직접 로딩
        stocks = getattr(_ss, "load_stock_list", lambda: [])()
        result = [{"name": s[0], "code": s[1], "market": s[2] if len(s) > 2 else ""}
                  for s in stocks]
        return jsonify({"list": result})
    except Exception as e:
        return jsonify({"list": [], "error": str(e)})


if __name__ == "__main__":
    print("=" * 50)
    print("  S-RIM 웹 UI 시작")
    print("  http://127.0.0.1:5000")
    print("=" * 50)

    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
