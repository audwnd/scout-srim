"""
네이버 m.stock investor API 필드명 확인 스크립트
PC에서 실행: python test_investor_api.py
"""
import requests, json

CODE = "005930"   # 삼성전자로 테스트
URL  = f"https://m.stock.naver.com/api/stock/{CODE}/investor"

try:
    r = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
    print(f"Status: {r.status_code}")
    d = r.json()

    print("\n=== 전체 응답 키 ===")
    if isinstance(d, list):
        print("리스트 응답, 첫 항목 키:", list(d[0].keys()) if d else "비어있음")
        print("첫 항목:", json.dumps(d[0], ensure_ascii=False, indent=2))
    elif isinstance(d, dict):
        print("딕셔너리 키:", list(d.keys()))
        print(json.dumps(d, ensure_ascii=False, indent=2)[:1500])
    else:
        print("기타:", type(d), str(d)[:500])

except Exception as e:
    print(f"오류: {e}")
    print("\n다른 URL 시도...")
    for url2 in [
        f"https://m.stock.naver.com/api/stock/{CODE}/investorByDay",
        f"https://m.stock.naver.com/api/stock/{CODE}/investorTrendByDay",
    ]:
        try:
            r2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            print(f"\n{url2}")
            print(f"Status: {r2.status_code}")
            if r2.status_code == 200:
                print(r2.text[:500])
        except Exception as e2:
            print(f"  실패: {e2}")
