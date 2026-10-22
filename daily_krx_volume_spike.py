def build_report():
    import unicodedata

    def disp_width(s: str) -> int:
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    def center_align(s: str, width: int) -> str:
        s_len = disp_width(s)
        if s_len >= width: return s
        left_pad = (width - s_len) // 2
        right_pad = width - s_len - left_pad
        return " " * left_pad + s + " " * right_pad

    # 1. 날짜 선정 (자동 보정)
    d1_date, d0_date = pick_latest_valid_days()
    
    if d1_date is None:
        return None

    d1_str = yyyymmdd(d1_date)
    d0_str = yyyymmdd(d0_date)

    print(f"⏳ 데이터 수집 시작 ({d1_str} vs {d0_str})...")
    
    # 여기서 새로운 함수 사용!
    dfs_1, dfs_0 = [], []
    for mkt in ["KOSPI", "KOSDAQ"]:
        market_d1 = get_market_data(d1_str, mkt)
        market_d0 = get_market_data(d0_str, mkt)
        if market_d1.empty or market_d0.empty:
            raise RuntimeError(f"{mkt} 데이터가 없어 보고서를 발송하지 않습니다.")
        dfs_1.append(market_d1)
        dfs_0.append(market_d0)

    df_1 = pd.concat(dfs_1, ignore_index=True)
    df_0 = pd.concat(dfs_0, ignore_index=True)

    if df_1.empty or df_0.empty or d1_str == d0_str:
        raise RuntimeError("데이터가 비었거나 동일 날짜가 선택되어 보고서를 중단합니다.")

    # 데이터 병합
    df_1_sub = df_1[["티커", "거래대금", "시가총액"]]
    df_0_sub = df_0[["티커", "거래대금"]]
    
    merged = pd.merge(df_1_sub, df_0_sub, on="티커", how="inner", suffixes=("_1", "_0"))
    
    # [필터링]
    merged = merged[merged["거래대금_0"] > 0]
    merged["배수"] = merged["거래대금_1"] / merged["거래대금_0"]
    
    target = merged[merged["배수"] >= 5.0].copy()
    print(f"📊 조건 만족 종목: {len(target)}개")

    # 정렬
    target = target.sort_values(
        by=["시가총액", "거래대금_1"], ascending=[False, False]
    ).head(30)

    # 포맷팅
    names = []
    tickers = target["티커"].tolist()
    for t in tickers:
        try: names.append(stock.get_market_ticker_name(t))
        except: names.append(t)

    amts = []
    for val in target["거래대금_1"].tolist():
        amts.append(f"{val/100000000:,.1f}")

    # 메시지 작성
    header = (
        f"[SK증권]\n"
        f"안녕하십니까 \n"
        f"sk 김수민입니다\n"
        f"전일거래대금 급증 종목 공유드립니다!\n"
        f"[기준일: {yyyy_mm_dd(d1_date)} vs {yyyy_mm_dd(d0_date)}]\n"
    )

    if target.empty:
        return header + "\n(조건 만족 종목 없음)"

    W_NUM = 3; W_NAME = 12; W_AMT = 12; W_CHART = 10; GAP = "   "

    h_line = (
        f"{' '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align('거래대금(억)', W_AMT)}{GAP}"
        f"{center_align('차트', W_CHART)}"
    )
    
    lines = []
    lines.append(f"<code>{html.escape(h_line)}</code>")
    lines.append("-" * 48)

    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), 1):
        rank_s = f"{str(i)+')':<{W_NUM}}"
        name_s = center_align(nm[:5], W_NAME)
        amt_s = center_align(av, W_AMT)
        stat = get_chart_status(t_code, d1_date)
        chart_s = center_align(stat, W_CHART)

        row = f"{rank_s}{GAP}{name_s}{GAP}{amt_s}{GAP}{chart_s}"
        lines.append(f"<code>{html.escape(row)}</code>")

    return header + "\n".join(lines)

# ---------- 실행부 ----------
if __name__ == "__main__":
    print("🚀 스크립트 실행 시작")
    
    try:
        msg = build_report()
        if msg:
            print("📨 메시지 전송 시도...")
            tg_send(msg)
            print("✅ 전송 완료")
        else:
            print("⚠️ 보낼 메시지가 없습니다.")
    except Exception as e:
        print(f"❌ [치명적 에러] {e}")
        sys.exit(1)


