# -*- coding: utf-8 -*-
"""test_rim.py - RIM 계산 진단 (fnguide_collector 기반)"""
import sys
from pathlib import Path
BASE = Path(__file__).parent
sys.path.insert(0, str(BASE))

CODE = "005930"
NAME = "삼성전자"

print(f"\n{'='*50}")
print(f"  RIM 계산 진단: {NAME} ({CODE})")
print(f"{'='*50}")

# 1. fnguide_collector로 데이터 수집
print("\n[1] FnGuide 데이터 수집...")
import fnguide_collector_v4 as _col
data = _col.collect(NAME, CODE)

ann = data.get("annual", {})
con = data.get("consensus", {})

print(f"  연도(실적): {ann.get('years', [])}")
print(f"  ROE(실적):  {ann.get('ROE', [])}")
print(f"  지배주주지분: {ann.get('지배주주지분', [])}")
print(f"  BPS: {ann.get('BPS', [])}")
print(f"  영업이익: {ann.get('영업이익', [])}")
print(f"  ROE(컨센): {con.get('ROE', [])}")

# 2. RIM 계산
print("\n[2] RIM 계산...")
ann_roe = ann.get("ROE", [])
ann_eq  = ann.get("지배주주지분", [])
con_roe = con.get("ROE", [])
shares  = data.get("발행주식수_보통", 0)
price   = data.get("현재가", 0)
ke      = 0.1026

# equity
equities = [v for v in ann_eq if v and v > 0]
equity = equities[-1] if equities else 0
print(f"  equity: {equity:,.0f}억원")

# roe 추정
roe_est = 0
for v in (con_roe or []):
    if v:
        roe_est = v/100 if v > 1 else v
        print(f"  roe_est: {roe_est:.4f} (컨센서스)")
        break
if not roe_est:
    vals = [v/100 if v and v > 1 else v for v in ann_roe if v is not None]
    if vals:
        recent = vals[-3:]
        w = list(range(1, len(recent)+1))
        roe_est = sum(v*wt for v,wt in zip(recent,w)) / sum(w)
        print(f"  roe_est: {roe_est:.4f} (가중평균)")

print(f"  ke: {ke:.4f}")
print(f"  shares: {shares:,}")
print(f"  현재가: {price:,}원")

# RIM
if equity and shares > 0 and roe_est:
    excess = equity * (roe_est - ke)
    unit   = 1e8 / shares
    print(f"\n  excess = {excess:,.0f}")
    if excess <= 0:
        fv = equity * unit
        print(f"  → 역배열 / 적정주가: {fv:,.0f}원")
    else:
        fair = equity + excess*0.9 / (1+ke-0.9)
        buy  = equity + excess*0.8 / (1+ke-0.8)
        print(f"  → 적정주가: {fair*unit:,.0f}원")
        print(f"  → 매수주가: {buy*unit:,.0f}원")
        if price:
            ratio = (price/(fair*unit)-1)*100
            print(f"  → 괴리율: {ratio:+.1f}% ({'저평가' if ratio<0 else '고평가'})")
else:
    print(f"  ❌ 계산 불가: equity={equity}, shares={shares}, roe={roe_est}")
