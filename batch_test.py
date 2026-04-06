"""
batch_test.py - SCOUT 전 종목 자동 테스트
실행: python batch_test.py
결과: batch_test_result.txt 에 저장
"""
import json, time, requests
from pathlib import Path
from datetime import datetime

BASE_URL = "http://127.0.0.1:5000"
WORK_DIR = Path(__file__).parent / "WORK"
RESULT_FILE = Path(__file__).parent / "batch_test_result.txt"

# 테스트할 종목 수 (None = 전체)
MAX_TEST = None   # 전체 테스트 시 None, 빠른 테스트 시 예: 50

def load_stock_list():
    """WORK/stock_list.json에서 종목 리스트 로드"""
    path = WORK_DIR / "stock_list.json"
    if not path.exists():
        print("❌ stock_list.json 없음")
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "data" in raw:
        return [{"name": k, "code": v[0], "market": v[1] if len(v)>1 else ""}
                for k, v in raw["data"].items()]
    elif isinstance(raw, list):
        return raw
    return []

def search_stock(name):
    """종목 검색 API 호출 → job_id 반환"""
    r = requests.post(f"{BASE_URL}/api/search",
                      json={"name": name}, timeout=10)
    return r.json()

def poll_status(job_id, timeout=60):
    """결과 폴링 → (state, result/msg)"""
    for _ in range(timeout // 2):
        time.sleep(2)
        r = requests.get(f"{BASE_URL}/api/status/{job_id}", timeout=10)
        d = r.json()
        if d["state"] != "running":
            return d["state"], d.get("result") or d.get("msg", "")
    return "timeout", "60초 초과"

def check_result(result):
    """결과 데이터 검증 → 문제 목록 반환"""
    issues = []
    if not result:
        return ["result 없음"]

    # 핵심 필드 확인
    if not result.get("적정주가") or result["적정주가"] <= 0:
        issues.append(f"적정주가 이상: {result.get('적정주가')}")
    if result.get("현재가", 0) <= 0:
        issues.append(f"현재가 이상: {result.get('현재가')}")

    # 지표 확인
    idx = result.get("지표", {})
    if idx.get("추정ROE") is None:
        issues.append("추정ROE None")
    if idx.get("EPS") is None:
        issues.append("EPS None")

    # 위험체크 확인
    risk = result.get("risk", [])
    if not risk:
        issues.append("위험체크 비어있음")

    return issues

def run():
    stocks = load_stock_list()
    if MAX_TEST:
        stocks = stocks[:MAX_TEST]

    total = len(stocks)
    ok_cnt = err_cnt = warn_cnt = 0
    errors = []
    warns = []

    print(f"\n{'='*50}")
    print(f"  SCOUT 배치 테스트 시작")
    print(f"  총 {total}개 종목")
    print(f"  시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    start_time = time.time()

    for i, stock in enumerate(stocks, 1):
        name = stock["name"].strip()
        code = stock.get("code", "")
        pct = i / total * 100

        # 진행률 표시 (10개마다)
        if i % 10 == 0 or i == 1:
            elapsed = time.time() - start_time
            eta = (elapsed / i) * (total - i) if i > 0 else 0
            print(f"[{i:4d}/{total}] {pct:5.1f}% | ✓{ok_cnt} ✗{err_cnt} △{warn_cnt} "
                  f"| 경과:{elapsed/60:.1f}분 남은:{eta/60:.1f}분")

        try:
            # 1. 검색 요청
            resp = search_stock(name)
            if "error" in resp:
                err_cnt += 1
                errors.append(f"[검색오류] {name}({code}): {resp['error']}")
                continue

            job_id = resp.get("job_id")
            if not job_id:
                err_cnt += 1
                errors.append(f"[job_id없음] {name}({code})")
                continue

            # 2. 결과 폴링
            state, result = poll_status(job_id)

            if state == "error":
                err_cnt += 1
                errors.append(f"[계산오류] {name}({code}): {str(result)[:80]}")
                continue
            elif state == "timeout":
                err_cnt += 1
                errors.append(f"[타임아웃] {name}({code})")
                continue

            # 3. 결과 검증
            issues = check_result(result)
            if issues:
                warn_cnt += 1
                warns.append(f"[경고] {name}({code}): {', '.join(issues)}")
            else:
                ok_cnt += 1

        except requests.exceptions.ConnectionError:
            err_cnt += 1
            errors.append(f"[연결오류] {name}({code}): 서버 연결 실패")
            print("  ⚠ 서버 연결 실패 - 서버가 실행 중인지 확인하세요")
            break
        except Exception as e:
            err_cnt += 1
            errors.append(f"[예외] {name}({code}): {str(e)[:80]}")

    # 결과 저장
    elapsed_total = time.time() - start_time
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    lines = [
        f"SCOUT 배치 테스트 결과",
        f"실행일시: {now}",
        f"소요시간: {elapsed_total/60:.1f}분",
        f"{'='*50}",
        f"총 종목: {total}개",
        f"✓ 정상:  {ok_cnt}개",
        f"✗ 오류:  {err_cnt}개",
        f"△ 경고:  {warn_cnt}개",
        f"{'='*50}",
        "",
    ]

    if errors:
        lines.append(f"[오류 목록] {err_cnt}건")
        lines.extend(errors)
        lines.append("")

    if warns:
        lines.append(f"[경고 목록] {warn_cnt}건")
        lines.extend(warns)
        lines.append("")

    report = "\n".join(lines)
    RESULT_FILE.write_text(report, encoding="utf-8")

    print(f"\n{'='*50}")
    print(f"  테스트 완료!")
    print(f"  ✓ 정상: {ok_cnt}  ✗ 오류: {err_cnt}  △ 경고: {warn_cnt}")
    print(f"  소요시간: {elapsed_total/60:.1f}분")
    print(f"  결과파일: batch_test_result.txt")
    print(f"{'='*50}\n")

    return ok_cnt, err_cnt, warn_cnt

if __name__ == "__main__":
    # 서버 확인
    try:
        r = requests.get(f"{BASE_URL}/api/market", timeout=5)
        print("✓ 서버 연결 확인")
    except:
        print("❌ 서버에 연결할 수 없습니다. 3_web.bat을 먼저 실행하세요.")
        exit(1)

    run()
