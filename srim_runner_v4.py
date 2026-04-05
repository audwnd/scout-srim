# -*- coding: utf-8 -*-
"""srim_runner_v4.py"""
import os, sys, subprocess, argparse
from pathlib import Path

BASE   = Path(__file__).parent
WORK   = BASE / "WORK"
OUTPUT = BASE / "OUTPUT"
TMPL   = BASE / "S-RIM_V33_ForwardBlock.xlsx"

WORK.mkdir(exist_ok=True)
OUTPUT.mkdir(exist_ok=True)

def run(cmd, desc):
    print(f"\n{'='*50}\n  {desc}\n{'='*50}")
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        print(f"[오류] {desc} 실패")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("name_pos", nargs="?", default=None, help="종목명 (위치인수)")
    parser.add_argument("--name", default=None)
    parser.add_argument("--code", default=None)
    args = parser.parse_args()
    # 위치인수 우선, 그 다음 --name
    if args.name_pos and not args.name:
        args.name = args.name_pos

    print("\n" + "="*50)
    print("  S-RIM 마법사 자동화 v4")
    print("="*50)

    if not args.name:
        print("[오류] 종목명을 입력하세요.")
        print("  사용법: python srim_runner_v4.py 삼성전자")
        sys.exit(1)
    input_name = args.name

    # 종목코드 자동 검색
    if args.code:
        # 직접 코드 입력한 경우
        name  = input_name
        code  = args.code.zfill(6)
    else:
        try:
            from stock_search import resolve_stock
            found_name, found_code = resolve_stock(input_name)
            if not found_code:
                print(f"[오류] '{input_name}' 종목을 찾을 수 없습니다.")
                print("  종목명을 정확히 입력하거나 종목코드를 직접 입력해주세요.")
                sys.exit(1)
            name = found_name
            code = found_code
        except ImportError:
            print("[경고] stock_search.py가 없습니다. 종목코드를 직접 입력하세요.")
            code = input("종목코드 (예: 005930): ").strip().zfill(6)
            name = input_name

    json_path = WORK / f"{code}_{name}.json"
    out_path  = OUTPUT / f"{name}_SRIM_{__import__('datetime').date.today().strftime('%Y%m%d')}.xlsx"
    py = sys.executable

    run(f'"{py}" "{BASE}/fnguide_collector_v4.py" "{name}" "{code}" "{json_path}"',
        "Step 1: FnGuide 데이터 수집")

    run(f'"{py}" "{BASE}/srim_filler_v4.py" "{TMPL}" "{json_path}" "{out_path}"',
        "Step 2: 엑셀 입력 + KIS BBB- 할인율 자동 적용")

    print(f"\n{'='*50}\n  Step 3: 수식 재계산\n{'='*50}")
    try:
        import win32com.client
        xl = win32com.client.Dispatch("Excel.Application")
        xl.Visible = False
        xl.DisplayAlerts = False
        wb = xl.Workbooks.Open(str(out_path.resolve()))
        xl.CalculateFull()
        wb.Save()
        wb.Close()
        xl.Quit()
        print("  재계산 완료")
    except Exception as e:
        print(f"  [경고] win32com 실패: {e}")
        print("  엑셀에서 Ctrl+Alt+F9 누르세요")

    print(f"\n{'='*50}")
    print(f"  완료! → {out_path}")
    print('='*50)
    try: os.startfile(str(out_path))
    except: pass

if __name__ == "__main__":
    main()
