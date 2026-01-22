    W_NUM = 1; W_NAME = 11; W_AMT = 11; W_CHART = 9; GAP = "   "

    h_line = (
        f"{'   '*W_NUM}{GAP}"
        f"{center_align('종목', W_NAME)}{GAP}"
        f"{center_align(' 거래대금(억)', W_AMT)}{GAP}"
        f"{center_align(' 차트', W_CHART)}"
    )
    
    lines = []
    lines.append(f"<code>{html.escape(h_line)}</code>")
    lines.append("-" * 48)

    for i, (nm, av, t_code) in enumerate(zip(names, amts, tickers), 1):
        rank_s = f"{str(i)+')':<{W_NUM}}"
        name_s = center_align(nm[:5], W_NAME)
        amt_s = center_align(av, W_AMT)
        stat = get_chart_status(t_code)
        chart_s = center_align(stat, W_CHART)

        row = f"{rank_s}{GAP}{name_s}{GAP}{amt_s}{GAP}{chart_s}"
        lines.append(f"<code>{html.escape(row)}</code>")
