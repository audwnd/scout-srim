"""
KRX 투자자별 순매수 데이터 테스트
PC에서 실행: python test_investor_krx.py
"""
import requests, json
from datetime import datetime, timedelta

def get_recent_trading_day():
    """최근 거래일 (주말 제외)"""
    d = datetime.today()
    while d.weekday() >= 5:  # 토=5, 일=6
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")

def get_investor_data(code, date):
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "http://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }

    # 방법 1: 개별 종목 투자자별 거래
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02402",
        "locale": "ko_KR",
        "trdDd": date,
        "strtDd": date,
        "endDd": date,
        "isuCd": f"KR7{code}003",
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false"
    }

    print(f"\n=== 방법1: 개별 종목 투자자별 거래 ===")
    print(f"종목: {code} | 날짜: {date}")

    try:
        r = requests.post(url, data=data, headers=headers, timeout=10)
        print(f"Status: {r.status_code}")
        d = r.json()
        print("응답 키:", list(d.keys()))
        if "output" in d and d["output"]:
            print("컬럼:", list(d["output"][0].keys()))
            print("데이터:", json.dumps(d["output"][:2], ensure_ascii=False, indent=2)[:800])
        else:
            print("전체:", str(d)[:400])
    except Exception as e:
        print(f"오류: {e}")

    # 방법 2: 외국인 순매수 상위 (KOSPI)
    print("\n=== 방법2: 외국인 순매수 상위 종목 ===")
    data2 = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02301",
        "locale": "ko_KR",
        "trdDd": date,
        "invstTpCd": "4000",
        "mktId": "STK",
        "buySellTpCd": "1",
        "csvxls_isNo": "false"
    }
    try:
        r2 = requests.post(url, data=data2, headers=headers, timeout=10)
        print(f"Status: {r2.status_code}")
        d2 = r2.json()
        if "output" in d2 and d2["output"]:
            print("컬럼:", list(d2["output"][0].keys()))
            found = [x for x in d2["output"] if code in str(x.values())]
            if found:
                print(f"{code} 발견:", json.dumps(found[0], ensure_ascii=False))
            else:
                print("상위 3개:", json.dumps(d2["output"][:3], ensure_ascii=False, indent=2)[:600])
        else:
            print(str(d2)[:300])
    except Exception as e:
        print(f"오류2: {e}")

if __name__ == "__main__":
    date = get_recent_trading_day()
    print(f"최근 거래일: {date}")
    get_investor_data("005930", date)
