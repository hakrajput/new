import argparse
import datetime
import os
import pandas as pd
import numpy as np
import calendar

TCH_TEXT_COLUMNS = [
    "Site NAME",
    "Built/Guest",
    "OMO",
    "Stations",
    "Belt",
    "Power Status",
    "TCH Cat",
]

benchmark_date: str | None = None
benchmark_value: float | None = None

def convert_column_headers_to_date_fmt(df: pd.DataFrame) -> pd.DataFrame:
    base = datetime.datetime(1899, 12, 30)
    new_columns: list[str] = []
    for col in df.columns:
        if isinstance(col, (int, float)) and 40000 < col < 60000:
            new_columns.append((base + datetime.timedelta(days=int(col))).strftime("%d-%B"))
        else:
            try:
                parsed = pd.to_datetime(col, errors="coerce")
                if pd.notna(parsed):
                    new_columns.append(parsed.strftime("%d-%B"))
                else:
                    new_columns.append(col)
            except Exception:
                new_columns.append(col)
    df.columns = new_columns
    return df

def remove_blank_column_after_power_status(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    new_cols = []
    skip_next = False
    for i, col in enumerate(cols):
        if skip_next:
            skip_next = False
            continue
        if col == "Power Status" and i + 1 < len(cols) and (cols[i + 1] == "" or pd.isna(cols[i + 1])):
            new_cols.append(col)
            skip_next = True
        else:
            new_cols.append(col)
    return df.loc[:, new_cols]

def clean_dataframe(df: pd.DataFrame, text_columns: list[str]) -> pd.DataFrame:
    for col in df.columns:
        if col not in text_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(1)
        else:
            df[col] = df[col].astype(str)
    return df

def insert_average_here(df: pd.DataFrame) -> pd.DataFrame:
    avg_row = pd.DataFrame(
        [["Average Here"] + [""] * (df.shape[1] - 1)], columns=df.columns
    )
    return pd.concat([avg_row, df], ignore_index=True)

def get_month_date_cols(df: pd.DataFrame):
    months = [f"-{month}" for month in calendar.month_name if month]
    return [c for c in df.columns if isinstance(c, str) and any(m in c for m in months)]

def read_and_prepare(
    path: str,
    sheet_name: str,
    key_col: str,
    text_cols: list[str],
    site_df: pd.DataFrame,
    merge_key: str | None = None,
    skiprows: int = 0,
) -> pd.DataFrame:
    _, ext = os.path.splitext(path)
    engine = "pyxlsb" if ext.lower() == ".xlsb" else None
    df = pd.read_excel(path, sheet_name=sheet_name, engine=engine, skiprows=skiprows)
    df[key_col] = df[key_col].astype(str).str.strip()
    df = df[df[key_col].isin(site_df["SiteName"].astype(str).str.strip())].copy()
    if merge_key:
        df = pd.merge(
            df,
            site_df.rename(columns={"SiteName": merge_key}),
            on=merge_key,
            how="left",
        )
    df = convert_column_headers_to_date_fmt(df)
    df = remove_blank_column_after_power_status(df)
    df.dropna(axis=1, how="all", inplace=True)
    date_cols = get_month_date_cols(df)
    def sort_key(c: str) -> int:
        try:
            return int(c.split("-")[0])
        except Exception:
            return 0
    date_cols_sorted = sorted(date_cols, key=sort_key)
    if sheet_name == "2G-Site Level stats":
        last3 = date_cols_sorted[-3:] if len(date_cols_sorted) >= 3 else date_cols_sorted
        df[last3] = df[last3].apply(pd.to_numeric, errors="coerce")
        df["Last 3 Days Avg"] = df[last3].mean(axis=1).round(1)
        df["3 Days Bin"] = df["Last 3 Days Avg"].apply(
            lambda x: "-" if pd.isna(x) else ("<90" if x < 90 else ("90-95" if x < 95 else ">95"))
        )
        ordered_cols: list[str] = []
        for col in TCH_TEXT_COLUMNS:
            ordered_cols.append(col)
            if col == "Power Status":
                ordered_cols.extend(["Last 3 Days Avg", "3 Days Bin"])
        if "TCH Cat" not in ordered_cols:
            ordered_cols.append("TCH Cat")
        if "Avg MTD" in df.columns and "Avg MTD" not in ordered_cols:
            ordered_cols.append("Avg MTD")
        ordered_cols.extend(date_cols_sorted)
        cols_to_keep = [c for c in ordered_cols if c in df.columns and c != "Commercial Priority"]
        df = df[cols_to_keep].copy()
        df = clean_dataframe(df, text_cols + ["3 Days Bin"])
    else:
        additional_cols = [c for c in ["Belt", "Built/Guest", "Power Status"] if c in df.columns]
        cols_to_keep = [key_col] + additional_cols + date_cols_sorted
        df = df[cols_to_keep].copy()
        df = clean_dataframe(df, [key_col] + additional_cols)
    df = insert_average_here(df)
    return df

def add_site_details_columns(tch_df: pd.DataFrame, site_details_path: str, columns: list[str]) -> pd.DataFrame:
    site_details = pd.read_excel(site_details_path)
    site_details['SiteName'] = site_details['SiteName'].astype(str).str.strip()
    site_details = site_details[['SiteName'] + columns]
    tch_df['Site NAME'] = tch_df['Site NAME'].astype(str).str.strip()
    merged = pd.merge(
        tch_df,
        site_details,
        left_on='Site NAME',
        right_on='SiteName',
        how='left'
    )
    merged.drop(columns=['SiteName'], inplace=True)
    # Move imported columns after Site NAME
    cols = list(merged.columns)
    for col in reversed(columns):
        if col in cols:
            cols.insert(cols.index('Site NAME') + 1, cols.pop(cols.index(col)))
    merged = merged[cols]
    return merged

def write_with_formatting(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    workbook = writer.book
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet
    font_format = workbook.add_format({"font_size": 9})
    num_format = workbook.add_format({"font_size": 9, "num_format": "0.0"})
    header_format = workbook.add_format({
        "font_size": 9,
        "bold": True,
        "bg_color": "#BDD7EE",
        "border": 1,
        "align": "center",
    })
    formula_format = workbook.add_format({"font_size": 9, "bold": True, "bottom": 1, "top": 1, "num_format": "0.0"})
    n_rows, n_cols = df.shape
    for c, col_name in enumerate(df.columns):
        val = df.iloc[0, c]
        if col_name not in TCH_TEXT_COLUMNS and col_name not in ["3 Days Bin", "TCH Cat"]:
            col_idx = c
            col_letter = ""
            temp = col_idx
            while True:
                col_letter = chr(65 + temp % 26) + col_letter
                temp = temp // 26 - 1
                if temp < 0:
                    break
            formula = f"=IFERROR(SUBTOTAL(101,{col_letter}3:{col_letter}{n_rows+1}),\"-\")"
            worksheet.write_formula(0, c, formula, formula_format)
        else:
            if isinstance(val, (int, float, np.number)) and (pd.isna(val) or not np.isfinite(val)):
                worksheet.write(0, c, "-", formula_format)
            else:
                worksheet.write(0, c, val if val != "" else "-", formula_format)
    worksheet.write_row(1, 0, df.columns.tolist(), header_format)
    for r in range(1, n_rows):
        for c, value in enumerate(df.iloc[r]):
            if isinstance(value, (int, float, np.number)) and pd.notna(value) and np.isfinite(value):
                worksheet.write_number(r + 1, c, float(value), num_format)
            else:
                display_val = "-" if (pd.isna(value) or (isinstance(value, float) and not np.isfinite(value))) else (str(value) if value != "" else "-")
                worksheet.write(r + 1, c, display_val, font_format)
    for idx, col_name in enumerate(df.columns):
        max_len = max(df[col_name].astype(str).map(len).max(), len(str(col_name)), len("Average Here")) + 2
        worksheet.set_column(idx, idx, 9, font_format)
    worksheet.freeze_panes(2, 1)

def generate_station_summary(writer: pd.ExcelWriter, df: pd.DataFrame) -> None:
    date_cols = get_month_date_cols(df)
    def sort_key(c: str) -> int:
        try:
            return int(c.split("-")[0])
        except Exception:
            return 0
    date_cols_sorted = sorted(date_cols, key=sort_key)
    date_cols = date_cols_sorted[-5:] if len(date_cols_sorted) >= 5 else date_cols_sorted
    working = df.iloc[1:].loc[:, ["Stations", "Power Status"] + date_cols].copy()
    working = working[working["Stations"].notna() & working["Power Status"].notna()].copy()
    working[date_cols] = working[date_cols].apply(pd.to_numeric, errors="coerce")
    working["Power Status"] = working["Power Status"].replace({
        "Emergency Pouring": "DG Sites",
        "Regular Pouring": "DG Sites",
    })
    def is_valid(cat: str) -> bool:
        if pd.isna(cat):
            return False
        s = str(cat).strip()
        return not (s.isdigit() or (s.startswith("(") and s.endswith(")") and s[1:-1].isdigit()))
    working = working[working["Power Status"].apply(is_valid)].copy()
    workbook = writer.book
    worksheet = workbook.add_worksheet("Station Summary")
    writer.sheets["Station Summary"] = worksheet
    font_format = workbook.add_format({"font_size": 9})
    header_format = workbook.add_format({"font_size": 9, "bold": True, "bg_color": "#BDD7EE", "border": 1})
    current_row = 0
    for power in working["Power Status"].dropna().unique():
        subset = working[working["Power Status"] == power]
        pivot = subset.groupby("Stations")[date_cols].mean().round(1).reset_index()
        station_counts = subset["Stations"].value_counts().to_dict()
        unique_station_count = len(pivot)
        worksheet.write(current_row, 0, "Power Status", font_format)
        worksheet.write(current_row, 2, power, font_format)
        current_row += 1
        worksheet.write_row(current_row, 0, ["Stations", "Site Count"] + date_cols, header_format)
        table_start_row = current_row + 1
        current_row += 1
        for _, row_data in pivot.iterrows():
            station_name = row_data["Stations"]
            worksheet.write(current_row, 0, station_name, font_format)
            count_val = station_counts.get(station_name, 0)
            if isinstance(count_val, (int, float, np.number)) and pd.notna(count_val) and np.isfinite(count_val):
                worksheet.write_number(current_row, 1, count_val, font_format)
            else:
                display_val = "-" if (pd.isna(count_val) or (isinstance(count_val, float) and not np.isfinite(count_val))) else (str(count_val) if count_val != "" else "-")
                worksheet.write(current_row, 1, display_val, font_format)
            for idx, col_name in enumerate(date_cols):
                val = row_data[col_name]
                col_idx = 2 + idx
                if isinstance(val, (int, float, np.number)) and pd.notna(val) and np.isfinite(val):
                    worksheet.write_number(current_row, col_idx, float(val), font_format)
                else:
                    display_val = "-" if (pd.isna(val) or (isinstance(val, float) and not np.isfinite(val))) else (str(val) if val != "" else "-")
                    worksheet.write(current_row, col_idx, display_val, font_format)
            current_row += 1
        table_end_row = current_row - 1
        if date_cols:
            for idx, col_name in enumerate(date_cols):
                col_idx = 2 + idx
                worksheet.conditional_format(
                    table_start_row, col_idx, table_end_row, col_idx,
                    {
                        'type': '3_color_scale',
                        'min_color': '#F8696B',
                        'mid_color': '#FFEB84',
                        'max_color': '#63BE7B',
                    }
                )
        current_row += 1
    total_cols = 2 + len(date_cols)
    for c in range(total_cols):
        worksheet.set_column(c, c, 12, font_format)

def generate_deep_dive(writer: pd.ExcelWriter, df: pd.DataFrame) -> None:
    working = df.iloc[1:].copy()
    if "Last 3 Days Avg" in working.columns:
        working["Last 3 Days Avg"] = pd.to_numeric(working["Last 3 Days Avg"], errors="coerce")
    if "Avg MTD" in working.columns:
        working["Avg MTD"] = pd.to_numeric(working["Avg MTD"], errors="coerce")
    working["Power Status"] = working["Power Status"].replace({
        "Emergency Pouring": "DG Sites",
        "Regular Pouring": "DG Sites",
    })
    dg_all = working[working["Power Status"] == "DG Sites"].copy()
    total_dg_sites = len(dg_all)
    current_avg = dg_all["Last 3 Days Avg"].dropna().mean() if total_dg_sites > 0 else np.nan
    dg_under = dg_all[(dg_all["Last 3 Days Avg"].notna()) & (dg_all["Last 3 Days Avg"] < 90)].copy()
    def expected_gain_pct(last_avg: float) -> float:
        if pd.isna(current_avg) or total_dg_sites == 0:
            return 0.0
        gain = (90.0 - last_avg) / total_dg_sites
        return round((gain / current_avg) * 100, 2)
    dg_under["Expected Gain (%)"] = dg_under["Last 3 Days Avg"].apply(expected_gain_pct)
    def classify_priority(x: float) -> str:
        if x >= 0.2:
            return "P0"
        elif x >= 0.1:
            return "P1"
        else:
            return "P2"
    dg_under["Priority"] = dg_under["Expected Gain (%)"].apply(classify_priority)
    column_order: list[str] = []
    if "Site NAME" in dg_under.columns:
        column_order.append("Site NAME")
    if "Stations" in dg_under.columns:
        column_order.append("Stations")
    column_order.append("Last 3 Days Avg")
    if "Avg MTD" in dg_under.columns:
        column_order.append("Avg MTD")
    if "3 Days Bin" in dg_under.columns:
        column_order.append("3 Days Bin")
    column_order.append("Expected Gain (%)")
    column_order.append("Priority")
    final_cols = [c for c in column_order if c in dg_under.columns]
    deep_df = dg_under[final_cols].copy()
    deep_df.sort_values([
        "Priority",
        "Expected Gain (%)",
    ], ascending=[True, False], inplace=True)
    workbook = writer.book
    worksheet = workbook.add_worksheet("Deep Dive")
    writer.sheets["Deep Dive"] = worksheet
    font_format = workbook.add_format({"font_size": 9})
    header_format = workbook.add_format({"font_size": 9, "bold": True, "bg_color": "#BDD7EE", "border": 1})
    num_format = workbook.add_format({"font_size": 9, "num_format": "0.0"})
    worksheet.write_row(0, 0, deep_df.columns.tolist(), header_format)
    for r in range(len(deep_df)):
        for c, value in enumerate(deep_df.iloc[r]):
            if isinstance(value, (int, float, np.number)) and pd.notna(value) and np.isfinite(value):
                worksheet.write_number(r + 1, c, float(value), num_format)
            else:
                display_val = "-" if (pd.isna(value) or (isinstance(value, float) and not np.isfinite(value))) else (str(value) if value != "" else "-")
                worksheet.write(r + 1, c, display_val, font_format)
    summary_start_row = len(deep_df) + 2
    for idx, col_name in enumerate(deep_df.columns):
        max_len = max(deep_df[col_name].astype(str).map(len).max(), len(str(col_name))) + 2
        worksheet.set_column(idx, idx, 9, font_format)

def generate_motivation_summary(writer: pd.ExcelWriter, df: pd.DataFrame, tchvsnar_df: pd.DataFrame = None) -> None:
    import calendar
    months = [f"-{month}" for month in calendar.month_name if month]
    date_cols = [c for c in df.columns if isinstance(c, str) and any(m in c for m in months)]
    date_cols_sorted = sorted(date_cols, key=lambda c: int(c.split("-")[0]) if c.split("-")[0].isdigit() else 0)
    last5 = date_cols_sorted[-5:] if len(date_cols_sorted) >= 5 else date_cols_sorted
    latest_day_col = date_cols_sorted[-1] if date_cols_sorted else None

    working = df.iloc[1:].copy()
    dg_mask = working["Power Status"].isin(["Regular Pouring", "Emergency Pouring"])
    working["Last 3 Days Avg"] = pd.to_numeric(working["Last 3 Days Avg"], errors="coerce")
    working["Avg MTD"] = pd.to_numeric(working["Avg MTD"], errors="coerce") if "Avg MTD" in working.columns else np.nan

    dg_all = working[dg_mask].copy()
    total_dg = len(dg_all)
    dg_avg_3days = dg_all["Last 3 Days Avg"].dropna().mean() if total_dg else np.nan
    dg_under_3days = dg_all[(dg_all["Last 3 Days Avg"].notna()) & (dg_all["Last 3 Days Avg"] < 90)].copy()
    total_region = len(working)
    region_avg_3days = working["Last 3 Days Avg"].dropna().mean() if total_region else np.nan

    improved_3days = working["Last 3 Days Avg"].copy()
    mask_3days = dg_mask & (working["Last 3 Days Avg"] < 90)
    improved_3days[mask_3days] = 90
    region_improved_avg_3days = improved_3days.dropna().mean() if total_region else np.nan

    dg_improved_avg_3days = dg_all["Last 3 Days Avg"].copy()
    dg_mask_3days = dg_improved_avg_3days < 90
    dg_improved_avg_3days[dg_mask_3days] = 90
    dg_improved_avg_3days = dg_improved_avg_3days.dropna().mean() if total_dg else np.nan

    gain_dg_3days = ((dg_improved_avg_3days - dg_avg_3days) / dg_avg_3days * 100) if dg_avg_3days else 0
    gain_region_3days = ((region_improved_avg_3days - region_avg_3days) / region_avg_3days * 100) if region_avg_3days else 0

    summary_lines = []
    summary_lines.append(f"Current DG average availability Last 3 Days: {dg_avg_3days:.1f}%")
    summary_lines.append(f"Current overall region average availability Last 3 Days: {region_avg_3days:.1f}%")
    summary_lines.append(f"Underperforming DG sites  Last 3 Days(<90): {len(dg_under_3days)}")
    summary_lines.append(f"If all underperforming DG sites reach 90%, DG average would be {dg_improved_avg_3days:.1f}% (gain of {gain_dg_3days:.1f}%).")
    summary_lines.append(
        f"Overall region average would rise to {region_improved_avg_3days:.1f}% (gain of {gain_region_3days:.1f}%) "
        f"by fixing degraded DG sites ({len(dg_under_3days)})."
    )

    if latest_day_col:
        dg_avg_lastday = dg_all[latest_day_col].dropna().mean() if total_dg else np.nan
        dg_under_lastday = dg_all[(dg_all[latest_day_col].notna()) & (dg_all[latest_day_col] < 90)].copy()
        region_avg_lastday = working[latest_day_col].dropna().mean() if total_region else np.nan

        improved_lastday = working[latest_day_col].copy()
        mask_lastday = dg_mask & (working[latest_day_col] < 90)
        improved_lastday[mask_lastday] = 90
        region_improved_avg_lastday = improved_lastday.dropna().mean() if total_region else np.nan

        dg_improved_avg_lastday = dg_all[latest_day_col].copy()
        dg_mask_lastday = dg_improved_avg_lastday < 90
        dg_improved_avg_lastday[dg_mask_lastday] = 90
        dg_improved_avg_lastday = dg_improved_avg_lastday.dropna().mean() if total_dg else np.nan

        gain_dg_lastday = ((dg_improved_avg_lastday - dg_avg_lastday) / dg_avg_lastday * 100) if dg_avg_lastday else 0
        gain_region_lastday = ((region_improved_avg_lastday - region_avg_lastday) / region_avg_lastday * 100) if region_avg_lastday else 0

        summary_lines.append("")
        summary_lines.append(f"Current DG average availability Last Day: {dg_avg_lastday:.1f}%")
        summary_lines.append(f"Current overall region average availability Last Day: {region_avg_lastday:.1f}%")
        summary_lines.append(f"Underperforming DG sites  Last Day(<90): {len(dg_under_lastday)}")
        summary_lines.append(f"If all underperforming DG sites reach 90%, DG average would be {dg_improved_avg_lastday:.1f}% (gain of {gain_dg_lastday:.1f}%).")
        summary_lines.append(
            f"Overall region average would rise to {region_improved_avg_lastday:.1f}% (gain of {gain_region_lastday:.1f}%) "
            f"by fixing degraded DG sites ({len(dg_under_lastday)})."
        )

        if tchvsnar_df is not None and "Delta Bin" in tchvsnar_df.columns and "TCH" in tchvsnar_df.columns and "NAR" in tchvsnar_df.columns:
            dg_mask_tchvsnar = tchvsnar_df["Power Status"].isin(["Regular Pouring", "Emergency Pouring"])
            telco_mask = (tchvsnar_df["Delta Bin"] == ">10") & (~dg_mask_tchvsnar)
            telco_sites = tchvsnar_df[telco_mask].copy()
            telco_site_names = set(telco_sites["Site NAME"])

            improved_telco_lastday = working[latest_day_col].copy()
            for idx, row in working.iterrows():
                if row["Site NAME"] in telco_site_names and row["Power Status"] not in ["Regular Pouring", "Emergency Pouring"]:
                    nar_val = telco_sites[telco_sites["Site NAME"] == row["Site NAME"]]["NAR"]
                    if not nar_val.empty:
                        improved_telco_lastday.at[idx] = nar_val.values[0]
            region_telco_improved_avg_lastday = improved_telco_lastday.dropna().mean() if total_region else np.nan
            gain_telco_lastday = ((region_telco_improved_avg_lastday - region_avg_lastday) / region_avg_lastday * 100) if region_avg_lastday else 0
            summary_lines.append(
                f"Overall region average would rise to {region_telco_improved_avg_lastday:.1f}% (gain of {gain_telco_lastday:.1f}%) "
                f"by fixing Telco issues ({len(telco_sites)} sites with Delta >10)."
            )

    top10_by_station: dict[str, pd.DataFrame] = {}
    for station in sorted(dg_under_3days["Stations"].dropna().unique()):
        subset = dg_under_3days[dg_under_3days["Stations"] == station].copy()
        top10_by_station[station] = subset.nsmallest(10, "Last 3 Days Avg").copy()

    workbook = writer.book
    sheet_name = "Motivation Summary"
    worksheet = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = worksheet
    font_format = workbook.add_format({"font_size": 9})
    header_format = workbook.add_format({"font_size": 9, "bold": True, "bg_color": "#BDD7EE", "border": 1})
    num_format = workbook.add_format({"font_size": 9, "num_format": "0.0"})
    wrap_format = workbook.add_format({"font_size": 9, "text_wrap": True})

    common_columns: list[str] = []
    if "Site NAME" in dg_under_3days.columns:
        common_columns.append("Site NAME")
    common_columns.append("Last 3 Days Avg")
    if "Avg MTD" in dg_under_3days.columns:
        common_columns.append("Avg MTD")
    common_columns.extend(last5)

    num_summary_cols = len(common_columns) if common_columns else 1
    row = 0
    for line in summary_lines:
        worksheet.merge_range(row, 0, row, num_summary_cols - 1, line, wrap_format)
        row += 1
    row += 1
    col_widths = [len(str(c)) for c in common_columns]

    table_ranges = []

    for station, table_df in top10_by_station.items():
        table_start_row = row
        worksheet.write(row, 0, f"Station: {station}", header_format)
        row += 1
        worksheet.write_row(row, 0, common_columns, header_format)
        row += 1
        value_start_row = row
        for i in range(len(table_df)):
            col_idx = 0
            for col_name in common_columns:
                value = table_df.iloc[i].get(col_name, "-")
                if isinstance(value, (int, float, np.number)) and pd.notna(value) and np.isfinite(value):
                    worksheet.write_number(row, col_idx, float(value), num_format)
                else:
                    display_val = "-" if (pd.isna(value) or (isinstance(value, float) and not np.isfinite(value))) else (str(value) if value != "" else "-")
                    worksheet.write(row, col_idx, display_val, font_format)
                col_widths[col_idx] = max(col_widths[col_idx], len(str(value)))
                col_idx += 1
            row += 1
        value_end_row = row - 1
        table_ranges.append((value_start_row, value_end_row))
        row += 1

    for idx, width in enumerate(col_widths):
        max_len = max(width, max(len(line) for line in summary_lines)) + 2
        worksheet.set_column(idx, idx, 11, font_format)

    for (value_start_row, value_end_row) in table_ranges:
        value_cols = [i for i, col in enumerate(common_columns) if col != "Site NAME"]
        for col_idx in value_cols:
            worksheet.conditional_format(
                value_start_row, col_idx, value_end_row, col_idx,
                {
                    'type': '3_color_scale',
                    'min_color': '#F8696B',
                    'mid_color': '#FFEB84',
                    'max_color': '#63BE7B',
                }
            )

def generate_tch_vs_nar(writer: pd.ExcelWriter, tch_df: pd.DataFrame, nar_df: pd.DataFrame) -> pd.DataFrame:
    tch_data = tch_df.iloc[1:].copy()
    nar_data = nar_df.iloc[1:].copy()
    date_cols_tch = get_month_date_cols(tch_data)
    date_cols_nar = get_month_date_cols(nar_data)
    common_dates = sorted(set(date_cols_tch) & set(date_cols_nar), key=lambda d: int(d.split("-")[0]))
    if not common_dates:
        print("❌ No common date columns found between TCH and NAR")
        return None
    latest_date = common_dates[-1]
    tch_extract = tch_data[["Site NAME", "Power Status", "Last 3 Days Avg", "3 Days Bin", latest_date]].copy()
    tch_extract.rename(columns={latest_date: "TCH"}, inplace=True)
    nar_site_col = "Site NAME" if "Site NAME" in nar_data.columns else "SiteName"
    nar_extract = nar_data[[nar_site_col, latest_date]].copy()
    nar_extract.rename(columns={nar_site_col: "Site NAME", latest_date: "NAR"}, inplace=True)
    merged = pd.merge(nar_extract, tch_extract, on="Site NAME", how="left")
    merged["Delta"] = merged["NAR"] - merged["TCH"]
    def classify_bin(row):
        if pd.isna(row["TCH"]) or pd.isna(row["NAR"]):
            return "Missing"
        elif row["Delta"] < 0:
            return "<10"
        elif 0 < row["Delta"] < 10:
            return "<10"
        elif row["Delta"] >= 10:
            return ">10"
        else:
            return "<10"
    merged["Delta Bin"] = merged.apply(classify_bin, axis=1)
    final_cols = ["Site NAME", "TCH", "NAR", "Delta", "Delta Bin", "Power Status", "Last 3 Days Avg", "3 Days Bin"]
    merged = merged[final_cols]
    workbook = writer.book
    worksheet = workbook.add_worksheet("TCHvsNAR")
    writer.sheets["TCHvsNAR"] = worksheet
    header_format = workbook.add_format({"font_size": 9, "bold": True, "bg_color": "#BDD7EE", "border": 1, "align": "center"})
    font_format = workbook.add_format({"font_size": 9})
    num_format = workbook.add_format({"font_size": 9, "num_format": "0.0"})
    highlight_format = workbook.add_format({"bg_color": "#F4CCCC", "font_size": 9})
    worksheet.merge_range(0, 0, 0, len(final_cols) - 1, f"Latest : {latest_date}", header_format)
    worksheet.write_row(1, 0, final_cols, header_format)
    for r in range(len(merged)):
        row_data = merged.iloc[r]
        highlight = pd.isna(row_data["TCH"]) or pd.isna(row_data["NAR"])
        for c, value in enumerate(row_data):
            cell_format = highlight_format if highlight else (num_format if isinstance(value, (int, float)) and pd.notna(value) and np.isfinite(value) else font_format)
            if isinstance(value, (int, float, np.number)) and (pd.isna(value) or not np.isfinite(value)):
                worksheet.write(r + 2, c, "-", cell_format)
            else:
                worksheet.write(r + 2, c, value if value != "" else "-", cell_format)
    for idx, col in enumerate(final_cols):
        max_len = max(merged[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, 9, font_format)
    worksheet.freeze_panes(2, 1)
    return merged

# ...all your previous code and functions...

def generate_belt_overview(writer, tch_df, fuel_df=None, tchvsnar_df=None):
    import numpy as np
    import pandas as pd

    working = tch_df.iloc[1:].copy()
    working["Last 3 Days Avg"] = pd.to_numeric(working["Last 3 Days Avg"], errors="coerce")
    working["Avg MTD"] = pd.to_numeric(working.get("Avg MTD", np.nan), errors="coerce")
    belts = working["Belt"].dropna().unique()
    built_types = ["TP as Guest", "Tower Co", "TP Built"]


    # --- Modern formats (font size 8 everywhere) ---
    workbook = writer.book
    worksheet = workbook.add_worksheet("BeltOverview")
    writer.sheets["BeltOverview"] = worksheet
    header_format = workbook.add_format({"font_size": 8, "bold": True, "bg_color": "#305496", "font_color": "white", "border": 1, "align": "center"})
    even_row = workbook.add_format({"bg_color": "#E7E6E6", "border": 1, "font_size": 8})
    odd_row = workbook.add_format({"bg_color": "#FFFFFF", "border": 1, "font_size": 8})
    section_title = workbook.add_format({"font_size": 9, "bold": True, "bg_color": "#4472C4", "font_color": "white", "align": "center"})
    font_format = workbook.add_format({"font_size": 8, "border": 1})

    # --- Belt Summary (no impact/factor columns, add Total row) ---
    summary_headers = [
        "Belt", "Total Sites", "Avg Last 3 Days", "Avg MTD",
        "<90% Count", "90-95% Count", ">95% Count", "DG Sites"
    ]
    summary_rows = []
    total_sites = total_avg_3d = total_avg_mtd = total_90 = total_9095 = total_95 = total_dg = 0
    for belt in sorted(belts):
        belt_df = working[working["Belt"] == belt]
        n_sites = len(belt_df)
        avg_3days = belt_df["Last 3 Days Avg"].mean()
        avg_mtd = belt_df["Avg MTD"].mean()
        bin_degraded = (belt_df["Last 3 Days Avg"] < 90)
        bin_90_95 = ((belt_df["Last 3 Days Avg"] >= 90) & (belt_df["Last 3 Days Avg"] < 95))
        bin_healthy = (belt_df["Last 3 Days Avg"] > 95)
        dg_sites = belt_df["Power Status"].isin(["DG Sites", "Emergency Pouring", "Regular Pouring"]).sum()
        summary_rows.append([
            belt, n_sites, round(avg_3days,1) if n_sites else "-", round(avg_mtd,1) if n_sites else "-",
            bin_degraded.sum(), bin_90_95.sum(), bin_healthy.sum(), dg_sites
        ])
        total_sites += n_sites
        total_avg_3d += avg_3days * n_sites if not np.isnan(avg_3days) else 0
        total_avg_mtd += avg_mtd * n_sites if not np.isnan(avg_mtd) else 0
        total_90 += bin_degraded.sum()
        total_9095 += bin_90_95.sum()
        total_95 += bin_healthy.sum()
        total_dg += dg_sites
    # Add Total row
    total_row = [
        "Total", total_sites,
        round(total_avg_3d/total_sites,1) if total_sites else "-",
        round(total_avg_mtd/total_sites,1) if total_sites else "-",
        total_90, total_9095, total_95, total_dg
    ]
    summary_rows.append(total_row)

    row = 0
    worksheet.merge_range(row, 0, row, len(summary_headers)-1, "Belt Summary", section_title)
    row += 1
    worksheet.write_row(row, 0, summary_headers, header_format)
    row += 1
    for i, summary in enumerate(summary_rows):
        fmt = even_row if i % 2 == 0 else odd_row
        for col_idx, val in enumerate(summary):
            if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                worksheet.write(row, col_idx, "-", fmt)
            else:
                worksheet.write(row, col_idx, val, fmt)
        row += 1

    # --- Beltwise Fueling Summary with Remarks ---
    if fuel_df is not None:
        fuel_df = fuel_df.copy()
        fuel_df["Visit Date"] = pd.to_datetime(fuel_df["Visit Date"])
        fuel_df["Total Diesel Added on site"] = pd.to_numeric(fuel_df["Total Diesel Added on site"], errors="coerce")
        site_belt_map = working.set_index("Site NAME")["Belt"].to_dict()
        fuel_df["Belt"] = fuel_df["Site ID"].map(site_belt_map)
        fuel_df["Visit Type"] = fuel_df["Visit Type (Scheduled/\nEmergecny)"].astype(str).str.strip().str.lower()
        fuel_df = fuel_df.sort_values(["Site ID", "Visit Date"])

        belt_fuel_summary = []
        for belt in sorted(belts):
            belt_fuel = fuel_df[fuel_df["Belt"] == belt]
            total_visits = len(belt_fuel)
            total_diesel = belt_fuel["Total Diesel Added on site"].sum()
            avg_diesel = belt_fuel["Total Diesel Added on site"].mean() if total_visits else 0
            scheduled_visits = (belt_fuel["Visit Type"].str.contains("sched|plan")).sum()
            emergency_visits = (belt_fuel["Visit Type"].str.contains("emerg")).sum()
            # Trend remarks
            if total_visits == 0:
                remarks = "No fueling activity"
            elif avg_diesel > 100:
                remarks = "High fueling trend"
            elif avg_diesel > 50:
                remarks = "Moderate fueling"
            else:
                remarks = "Low fueling"
            if emergency_visits > scheduled_visits:
                remarks += ", more emergency visits"
            elif scheduled_visits > 0:
                remarks += ", mostly scheduled"
            belt_fuel_summary.append([
                belt, total_visits, total_diesel, round(avg_diesel,1), emergency_visits, scheduled_visits, remarks
            ])
        # Add Total Row
        if belt_fuel_summary:
            arr = np.array([x[1:6] for x in belt_fuel_summary], dtype=float)
            total_row = [
                "Total",
                int(arr[:,0].sum()),
                arr[:,1].sum(),
                round(arr[:,1].sum()/arr[:,0].sum(),1) if arr[:,0].sum() else 0,
                int(arr[:,3].sum()),
                int(arr[:,4].sum()),
                ""
            ]
            belt_fuel_summary.append(total_row)
        fuel_headers = ["Belt", "Total Visits", "Total Diesel", "Avg Diesel/Visit", "Emergency Visits", "Scheduled Visits", "Remarks"]
        row += 2
        worksheet.merge_range(row, 0, row, len(fuel_headers)-1, "Beltwise Fueling Summary", section_title)
        row += 1
        worksheet.write_row(row, 0, fuel_headers, header_format)
        row += 1
        for i, vals in enumerate(belt_fuel_summary):
            fmt = even_row if i % 2 == 0 else odd_row
            for col_idx, val in enumerate(vals):
                if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    worksheet.write(row, col_idx, "-", fmt)
                else:
                    worksheet.write(row, col_idx, val, fmt)
            row += 1

        # --- High Fueling & Low Availability Sites ---
        high_fuel_threshold = fuel_df["Total Diesel Added on site"].quantile(0.9)
        high_fuel_sites = set(fuel_df[fuel_df["Total Diesel Added on site"] >= high_fuel_threshold]["Site ID"])
        low_avail_sites = set(working[working["Last 3 Days Avg"] < 90]["Site NAME"])
        highfuel_lowavail = high_fuel_sites & low_avail_sites
        hfla_rows = []
        for site in highfuel_lowavail:
            total_visit = fuel_df[fuel_df["Site ID"] == site].shape[0]
            avg_mtd = working[working["Site NAME"] == site]["Avg MTD"].mean()
            total_fuel = fuel_df[fuel_df["Site ID"] == site]["Total Diesel Added on site"].sum()
            hfla_rows.append([site, total_visit, avg_mtd, total_fuel])
        hfla_headers = ["Site ID", "Total Visit", "Avg MTD", "Total Fuel"]
        row += 2
        worksheet.merge_range(row, 0, row, len(hfla_headers)-1, "High Fueling & Low Availability Sites", section_title)
        row += 1
        worksheet.write_row(row, 0, hfla_headers, header_format)
        row += 1
        for i, vals in enumerate(hfla_rows):
            fmt = even_row if i % 2 == 0 else odd_row
            for col_idx, val in enumerate(vals):
                if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    worksheet.write(row, col_idx, "-", fmt)
                else:
                    worksheet.write(row, col_idx, val, fmt)
            row += 1

        # --- Club Repeated & Daily Fueling Sites, only Avg MTD <95, add Total Fuel ---
        repeated_sites = set(fuel_df.groupby("Site ID").size()[lambda x: x > 1].index)
        daily_fueling_sites = set()
        for site, group in fuel_df.groupby("Site ID"):
            group = group.sort_values("Visit Date")
            if any((group["Visit Date"].diff().dt.days == 1).fillna(False)):
                daily_fueling_sites.add(site)
        clubbed_sites = repeated_sites | daily_fueling_sites
        clubbed_rows = []
        for site in clubbed_sites:
            avg_mtd = working[working["Site NAME"] == site]["Avg MTD"].mean()
            if avg_mtd >= 95 or np.isnan(avg_mtd):
                continue
            total_visit = fuel_df[fuel_df["Site ID"] == site].shape[0]
            total_fuel = fuel_df[fuel_df["Site ID"] == site]["Total Diesel Added on site"].sum()
            clubbed_rows.append([site, total_visit, avg_mtd, total_fuel])
        clubbed_headers = ["Site ID", "Total Visit", "Avg MTD", "Total Fuel"]
        row += 2
        worksheet.merge_range(row, 0, row, len(clubbed_headers)-1, "Repeated & Daily Fueling Sites (Avg MTD <95)", section_title)
        row += 1
        worksheet.write_row(row, 0, clubbed_headers, header_format)
        row += 1
        for i, vals in enumerate(clubbed_rows):
            fmt = even_row if i % 2 == 0 else odd_row
            for col_idx, val in enumerate(vals):
                if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    worksheet.write(row, col_idx, "-", fmt)
                else:
                    worksheet.write(row, col_idx, val, fmt)
            row += 1

    # --- Belt Level Daily Availability by Built/Guest with degradation metrics ---
    beltava_ws = workbook.add_worksheet("BeltAva")
    writer.sheets["BeltAva"] = beltava_ws
    built_types = sorted(set(working["Built/Guest"].dropna().unique()) | {"TP as Guest", "Tower Co", "TP Built", "UAF"})
    date_cols = [c for c in working.columns if "-" in str(c) and c.split("-")[0].isdigit()]
    date_cols_sorted = sorted(date_cols, key=lambda c: int(c.split("-")[0]))
    pivot_headers = ["Built/Guest"] + date_cols_sorted

    # Determine benchmark date/value for degradation
    global benchmark_date, benchmark_value
    overall_series = working[date_cols_sorted].mean()
    benchmark_date = overall_series.idxmax()
    benchmark_value = overall_series.max()
    latest_date = date_cols_sorted[-1] if date_cols_sorted else None

    beltava_row = 0

    # --- Overall Daily Availability by Built/Guest (across all belts) ---
    beltava_ws.merge_range(
        beltava_row,
        0,
        beltava_row,
        len(pivot_headers) - 1,
        "Overall - Daily Availability by Built/Guest",
        section_title,
    )
    beltava_row += 1
    beltava_ws.write_row(beltava_row, 0, pivot_headers, header_format)
    beltava_row += 1

    for built in built_types:
        mask = working["Built/Guest"] == built
        if not mask.any():
            continue
        row_vals = [built]
        for date in date_cols_sorted:
            vals = working.loc[mask, date]
            avg = vals.mean() if not vals.empty else np.nan
            row_vals.append(round(avg, 1) if not np.isnan(avg) else "-")
        fmt = even_row if (beltava_row % 2 == 0) else odd_row
        for col_idx, val in enumerate(row_vals):
            beltava_ws.write(beltava_row, col_idx, val, fmt)
        beltava_row += 1

    overall_vals = ["Overall"]
    for date in date_cols_sorted:
        vals = working[date]
        avg = vals.mean() if not vals.empty else np.nan
        overall_vals.append(round(avg, 1) if not np.isnan(avg) else "-")
    fmt = even_row if (beltava_row % 2 == 0) else odd_row
    for col_idx, val in enumerate(overall_vals):
        beltava_ws.write(beltava_row, col_idx, val, fmt)
    beltava_row += 1

    first_data_row = 2  # Skip title and header
    last_data_row = beltava_row - 1
    if last_data_row >= first_data_row:
        beltava_ws.conditional_format(
            first_data_row,
            1,
            last_data_row,
            len(pivot_headers) - 1,
            {
                'type': '3_color_scale',
                'min_color': "#FF0000",
                'mid_color': "#FFFFFF",
                'max_color': "#4472C4",
            },
        )

    beltava_row += 1  # Blank row before belt-specific tables

    for belt in sorted(belts):
        start_row = beltava_row
        beltava_ws.merge_range(beltava_row, 0, beltava_row, len(pivot_headers)-1, f"{belt} - Daily Availability by Built/Guest", section_title)
        beltava_row += 1
        beltava_ws.write_row(beltava_row, 0, pivot_headers, header_format)
        beltava_row += 1

        for built in built_types:
            mask = (working["Belt"] == belt) & (working["Built/Guest"] == built)
            if not mask.any():
                continue
            row_vals = [built]
            for date in date_cols_sorted:
                vals = working.loc[mask, date]
                avg = vals.mean() if not vals.empty else np.nan
                row_vals.append(round(avg, 1) if not np.isnan(avg) else "-")
            fmt = even_row if (beltava_row % 2 == 0) else odd_row
            for col_idx, val in enumerate(row_vals):
                beltava_ws.write(beltava_row, col_idx, val, fmt)
            beltava_row += 1

        overall_vals = ["Overall"]
        for date in date_cols_sorted:
            vals = working.loc[working["Belt"] == belt, date]
            avg = vals.mean() if not vals.empty else np.nan
            overall_vals.append(round(avg, 1) if not np.isnan(avg) else "-")
        fmt = even_row if (beltava_row % 2 == 0) else odd_row
        for col_idx, val in enumerate(overall_vals):
            beltava_ws.write(beltava_row, col_idx, val, fmt)
        beltava_row += 1

        # --- Conditional formatting for this belt table ---
        first_data_row = start_row + 2
        last_data_row = beltava_row - 1
        if last_data_row >= first_data_row:
            beltava_ws.conditional_format(
                first_data_row,
                1,
                last_data_row,
                len(pivot_headers)-1,
                {
                    'type': '3_color_scale',
                    'min_color': '#FF0000',
                    'mid_color': '#FFFFFF',
                    'max_color': '#4472C4',
                }
            )
        beltava_row += 1  # Blank row after each belt table

    # --- Degradation Since Max Availability ---
    beltava_ws.merge_range(beltava_row, 0, beltava_row, 5, "Degradation Since Max Availability", section_title)
    beltava_row += 1
    deg_headers = ["Belt", "Built/Guest", "Benchmark Date", "Benchmark Availability", "Current Availability", "Degradation"]
    beltava_ws.write_row(beltava_row, 0, deg_headers, header_format)
    beltava_row += 1

    deg_rows = []
    for belt in sorted(belts):
        for built in built_types:
            mask = (working["Belt"] == belt) & (working["Built/Guest"] == built)
            if not mask.any():
                continue
            bench_val = working.loc[mask, benchmark_date].mean() if benchmark_date in working.columns else np.nan
            latest_val = working.loc[mask, latest_date].mean() if latest_date in working.columns else np.nan
            deg_rows.append([
                belt,
                built,
                benchmark_date,
                round(bench_val, 1) if not np.isnan(bench_val) else np.nan,
                round(latest_val, 1) if not np.isnan(latest_val) else np.nan,
                round(bench_val - latest_val, 1) if (not np.isnan(bench_val) and not np.isnan(latest_val)) else np.nan,
            ])

    overall_latest = working[latest_date].mean() if latest_date in working.columns else np.nan
    deg_rows.append([
        "Overall",
        "Overall",
        benchmark_date,
        round(benchmark_value, 1) if not np.isnan(benchmark_value) else np.nan,
        round(overall_latest, 1) if not np.isnan(overall_latest) else np.nan,
        round(benchmark_value - overall_latest, 1) if (not np.isnan(benchmark_value) and not np.isnan(overall_latest)) else np.nan,
    ])

    for i, vals in enumerate(deg_rows):
        fmt = even_row if i % 2 == 0 else odd_row
        for col_idx, val in enumerate(vals):
            if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                beltava_ws.write(beltava_row, col_idx, "-", fmt)
            else:
                beltava_ws.write(beltava_row, col_idx, val, fmt)
        beltava_row += 1

    if deg_rows:
        first_deg_row = beltava_row - len(deg_rows)
        last_deg_row = beltava_row - 1
        deg_col = deg_headers.index("Degradation")
        beltava_ws.conditional_format(first_deg_row, deg_col, last_deg_row, deg_col, {
            'type': '3_color_scale',
            'min_color': "#FFFFFF",
            'mid_color': "#FFE699",
            'max_color': "#FF0000",
        })

    beltava_row += 1

    # --- Auto column width for all sections in BeltAva ---
    for col in range(len(pivot_headers)):
        max_len = len(str(pivot_headers[col]))
        for r in range(beltava_row):
            try:
                val = beltava_ws.table.get((r, col), [None, None, None, None])[2]
                if val is not None:
                    max_len = max(max_len, len(str(val)))
            except Exception:
                pass
        beltava_ws.set_column(col, col, max(10, min(max_len + 2, 40)))
    # --- 2G_4G DELTA BLOCK ---
def generate_2g_4g_delta(writer, tch_df, lte_path):
    """Create a 2G-4G Delta by comparing latest *filled* daily TCH vs LTE stats."""
    import re
    from datetime import datetime, timedelta

    # ---- helpers ----
    def excel_serial_to_ddmonth(x):
        try:
            if isinstance(x, (int, float)) and 40000 < float(x) < 60000:
                base = datetime(1899, 12, 30)
                return (base + timedelta(days=int(x))).strftime("%d-%B")
        except Exception:
            pass
        return str(x)

    def clean_to_numeric(series: pd.Series) -> pd.Series:
        s = series.copy()
        if s.dtype == "O":
            s = (
                s.astype(str)
                 .str.strip()
                 .str.replace("%", "", regex=False)
                 .str.replace(",", "", regex=False)
                 .replace({"-": np.nan, "": np.nan})
            )
        vals = pd.to_numeric(s, errors="coerce")
        nonnull = vals.dropna()
        if len(nonnull) and (nonnull.between(0, 1).mean() > 0.8):
            vals = vals * 100.0
        return vals

    def pick_latest_filled_col(df: pd.DataFrame, candidates: list) -> tuple[str | int | None, str]:
        """Return (col_id, nice_label) where col_id is the column key and nice_label is dd-Month."""
        for col in reversed(candidates):
            if col not in df.columns:
                continue
            vals = clean_to_numeric(df.iloc[1:][col])  # skip 'Average Here' row
            if vals.notna().sum() > 0:
                # label for banner
                label = str(col)
                # if LTE kept serials, pretty-print them
                pretty = excel_serial_to_ddmonth(col)
                return col, pretty
        return None, ""

    print("Running 2G-4G Delta sheet generation...")
    lte_df = pd.read_excel(lte_path, sheet_name="LTE-Site Level stats", header=3)
    print("LTE columns:", lte_df.columns.tolist())

    # --- Find LTE Site ID column (e.g., "2G Site ID") ---
    site_id_col = next((c for c in lte_df.columns if "Site ID" in str(c)), None)
    if not site_id_col:
        print("❌ Could not find a column containing 'Site ID' in LTE file.")
        return
    lte_df[site_id_col] = lte_df[site_id_col].astype(str).str.strip()

    # --- Build candidate date columns ---
    # LTE: prefer numeric Excel serial headers
    lte_numeric_headers = [c for c in lte_df.columns if isinstance(c, (int, float)) and 40000 < float(c) < 60000]
    if lte_numeric_headers:
        lte_candidates = sorted(lte_numeric_headers)
    else:
        tmp = convert_column_headers_to_date_fmt(lte_df.copy())
        lte_df = tmp
        lte_candidates = sorted(
            get_month_date_cols(lte_df),
            key=lambda s: int(str(s).split("-")[0]) if str(s).split("-")[0].isdigit() else 0
        )

    # TCH candidates (already dd-Month strings)
    tch_candidates = sorted(
        get_month_date_cols(tch_df),
        key=lambda s: int(str(s).split("-")[0]) if str(s).split("-")[0].isdigit() else 0
    )

    # --- Pick the latest *filled* column on each side ---
    lte_latest, lte_latest_label = pick_latest_filled_col(lte_df, lte_candidates)
    tch_latest, tch_latest_label = pick_latest_filled_col(tch_df, tch_candidates)

    print(f"Chosen latest *filled* columns -> TCH: {tch_latest_label} | LTE: {lte_latest_label}")
    if not lte_latest or not tch_latest:
        print("❌ Could not find a filled latest date column in LTE or TCH data.")
        print("   LTE candidates (rightmost 6):", lte_candidates[-6:])
        print("   TCH candidates (rightmost 6):", tch_candidates[-6:])
        return

    # --- Build slim frames & parse numerics ---
    tch = tch_df.iloc[1:].copy()
    tch["Site NAME"] = tch["Site NAME"].astype(str).str.strip()
    tch["TCH Latest"] = clean_to_numeric(tch[tch_latest])

    lte = lte_df.copy()
    lte["4G Stats Latest"] = clean_to_numeric(lte[lte_latest])

    # --- Merge: exact then normalized if needed ---
    merged = pd.merge(
        tch[["Site NAME", "TCH Latest"]],
        lte[[site_id_col, "4G Stats Latest"]],
        left_on="Site NAME",
        right_on=site_id_col,
        how="inner",
    )
    if merged.empty:
        def norm(x): return re.sub(r"[^A-Za-z0-9]", "", str(x)).upper()
        tch["_KEY"] = tch["Site NAME"].map(norm)
        lte["_KEY"] = lte[site_id_col].map(norm)
        merged = pd.merge(
            tch[["_KEY", "Site NAME", "TCH Latest"]],
            lte[["_KEY", site_id_col, "4G Stats Latest"]],
            on="_KEY",
            how="inner",
        ).drop(columns=["_KEY"])

    merged["Delta"] = merged["TCH Latest"] - merged["4G Stats Latest"]

    # ---- Excel writing (two sheets), with a merged banner row for dates ----
    wb = writer.book
    hfmt = wb.add_format({"font_size": 9, "bold": True, "bg_color": "#BDD7EE", "border": 1, "align": "center"})
    nfmt = wb.add_format({"font_size": 9, "num_format": "0.0"})
    tfmt = wb.add_format({"font_size": 9})

    banner_text = f"Latest Filled Dates — TCH: {tch_latest_label}  |  LTE: {lte_latest_label}"

    # (All) sheet
    ws_all = wb.add_worksheet("2G-4G Delta (All)")
    writer.sheets["2G-4G Delta (All)"] = ws_all
    all_cols = ["Site NAME", site_id_col, "TCH Latest", "4G Stats Latest", "Delta"]

    # merged banner in row 0 across all columns
    ws_all.merge_range(0, 0, 0, len(all_cols) - 1, banner_text, hfmt)
    # column headers on row 1
    for c, name in enumerate(all_cols):
        ws_all.write(1, c, name, hfmt)
    # data starts on row 2
    for r in range(len(merged)):
        row = merged.iloc[r]
        ws_all.write(r + 2, 0, row.get("Site NAME", ""), tfmt)
        ws_all.write(r + 2, 1, row.get(site_id_col, ""), tfmt)
        v = row.get("TCH Latest");      ws_all.write_number(r + 2, 2, float(v), nfmt) if pd.notna(v) else ws_all.write(r + 2, 2, "-", tfmt)
        v = row.get("4G Stats Latest"); ws_all.write_number(r + 2, 3, float(v), nfmt) if pd.notna(v) else ws_all.write(r + 2, 3, "-", tfmt)
        v = row.get("Delta");           ws_all.write_number(r + 2, 4, float(v), nfmt) if pd.notna(v) else ws_all.write(r + 2, 4, "-", tfmt)
    ws_all.freeze_panes(2, 0)

    # Filtered sheet (Delta > 10, non-null)
    filtered = merged[(merged["TCH Latest"].notna()) &
                      (merged["4G Stats Latest"].notna()) &
                      (merged["Delta"] > 10)].copy()

    ws = wb.add_worksheet("2G-4G Delta")
    writer.sheets["2G-4G Delta"] = ws
    cols = ["Site NAME", "TCH Latest", "4G Stats Latest", "Delta"]

    # merged banner row
    ws.merge_range(0, 0, 0, len(cols) - 1, banner_text, hfmt)
    # headers on row 1
    for c, name in enumerate(cols):
        ws.write(1, c, name, hfmt)
    # data on row 2+
    for r in range(len(filtered)):
        row = filtered.iloc[r]
        ws.write(r + 2, 0, row["Site NAME"], tfmt)
        for j, col in enumerate(["TCH Latest", "4G Stats Latest", "Delta"], start=1):
            v = row[col]
            ws.write_number(r + 2, j, float(v), nfmt) if pd.notna(v) else ws.write(r + 2, j, "-", tfmt)
    ws.freeze_panes(2, 0)


    # --- EXTRA ---

def generate_degradation_analysis(writer, df, outage_path, sheet_name="Degradation Analysis"):
    """Analyze site availability degradation and annotate with outage info."""
    import pandas as pd

    global benchmark_date, benchmark_value

    date_cols = get_month_date_cols(df)
    date_cols_sorted = sorted(
        date_cols, key=lambda c: int(str(c).split("-")[0]) if str(c).split("-")[0].isdigit() else 0
    )
    if not date_cols_sorted:
        print("❌ No date columns found for degradation analysis")
        return

    latest_date = date_cols_sorted[-1]

    numeric_avgs = df[date_cols_sorted].apply(pd.to_numeric, errors="coerce").mean()
    if numeric_avgs.dropna().empty:
        print("❌ No numeric data found for degradation analysis")
        return
    benchmark_value = numeric_avgs.max()
    benchmark_date = numeric_avgs.idxmax()
    if benchmark_date not in df.columns:
        print(f"⚠️ Benchmark date '{benchmark_date}' not found in data; skipping degradation analysis")
        return

    working = df.iloc[1:].copy()
    if sheet_name == "NAR Degradation Analysis":
        last3 = date_cols_sorted[-3:] if len(date_cols_sorted) >= 3 else date_cols_sorted
        working[last3] = working[last3].apply(pd.to_numeric, errors="coerce")
        working["3Days (NAR)"] = working[last3].mean(axis=1).round(1)
        working[latest_date] = pd.to_numeric(working[latest_date], errors="coerce")
        working[benchmark_date] = pd.to_numeric(working[benchmark_date], errors="coerce")
        working["Degradation"] = working[benchmark_date] - working["3Days (NAR)"]
        flagged = working[working["3Days (NAR)"] < 95].copy()
    else:
        working[latest_date] = pd.to_numeric(working[latest_date], errors="coerce")
        working[benchmark_date] = pd.to_numeric(working[benchmark_date], errors="coerce")
        working["Degradation"] = working[benchmark_date] - working[latest_date]
        flagged = working[working[latest_date] < 95].copy()

    outage_df = pd.read_excel(outage_path, sheet_name="Outages Summary")
    outage_df["Site"] = outage_df["Site"].astype(str).str.strip()
    outage_df["Date"] = pd.to_datetime(outage_df["Date"])
    latest_dt = datetime.datetime.strptime(
        f"{latest_date}-{datetime.date.today().year}", "%d-%B-%Y"
    )

    def fetch_outage(site: str) -> pd.Series:
        subset = outage_df[(outage_df["Site"] == site) & (outage_df["Date"] == latest_dt)]
        if subset.empty:
            return pd.Series({"Category": ""})
        cats = subset["Category"].dropna().astype(str).str.strip()
        cats = cats[cats != ""].drop_duplicates()
        cat_str = ", ".join(cats)
        return pd.Series({"Category": cat_str})

    outage_info = flagged["Site NAME"].apply(fetch_outage)
    result = pd.concat([flagged, outage_info], axis=1)
    result.rename(
        columns={
            benchmark_date: "Benchmark Availability",
            latest_date: "Current Availability",
            "Category": "Outage Category",
        },
        inplace=True,
    )

    def classify_mains_category(cat: str, ps: str) -> str:
        cat_raw = "" if pd.isna(cat) else str(cat).strip()
        ps_raw = "" if pd.isna(ps) else str(ps).strip()
        lowc = cat_raw.lower()
        lowp = ps_raw.lower()

        impact = lowc.startswith("impact") or "impact of" in lowc or "impac of" in lowc
        zero_pour = "zero pouring" in lowc
        fuel_finished = "fuel finished" in lowc
        non_dg = "non dg" in lowc
        tp_guest = "tp as guest" in lowc
        fuel_theft = "fuel theft" in lowc or "fuel  theft" in lowc
        dg_issue = "dg issue" in lowc
        dg_mech = "dg mechanical issue" in lowc
        dg_on_moh = "dg on moh" in lowc
        poh = "poh issue" in lowc or "poh" in lowc
        txn_telco = "txn" in lowc or "telco" in lowc
        guard = any(x in lowc for x in ["guard intervention", "owner intervention", "guard salary", "gurad salary"])
        access = any(x in lowc for x in ["engro access issue", "access issue", "site access", "no access", "access denied"])
        water = any(x in lowc for x in ["water logging", "water-logging", "waterlogging", "waterlogged"])
        ui = "ui" in lowc
        ns_flag = guard or access or water or ui

        ps_pouring = lowp in ("regular pouring", "emergency pouring")
        ps_non_dg = "non dg" in lowp
        ps_tp_guest = "tp as guest" in lowp
        ps_tower_co = "tower co" in lowp

        if cat_raw == "":
            if ps_pouring:
                return "Fueling Outage"
            if ps_non_dg:
                return "Non DG"
            if ps_tp_guest or ps_tower_co:
                return "TP as Guest"
        else:
            if guard and fuel_finished:
                return "Fuel Theft"
            if guard and dg_issue:
                return "NS Issue"
            if ps_pouring and "bad weather" in lowc:
                return "Fueling Outage"
            if impact and zero_pour:
                return "Link Outages"
            if non_dg and impact:
                return "Non DG"
            if non_dg and dg_issue:
                return "Non DG"

        if fuel_finished:
            return "Fueling Outage"
        if impact:
            return "Link Outages"
        if tp_guest:
            return "TP as Guest"
        if non_dg:
            return "Non DG"
        if fuel_theft:
            return "Fuel Theft"
        if zero_pour:
            return "Fueling Outage"
        if dg_mech:
            return "POH Issue"
        if dg_on_moh:
            return "DG on MOH"
        if poh:
            return "POH Issue"
        if ui and dg_issue:
            return "DG Issue"
        if dg_issue:
            return "DG Issue"
        if txn_telco:
            return "Txn / Telco"
        if ns_flag:
            return "NS Issue"
        if "tower co" in lowc:
            return "Tower CO"
        return cat_raw

    result["Mains Category"] = result.apply(
        lambda r: classify_mains_category(r.get("Outage Category", ""), r.get("Power Status", "")),
        axis=1,
    )
    final_cols = [
        "Site NAME",
        "Belt",
        "Built/Guest",
        "Power Status",
        "Benchmark Availability",
        "Current Availability",
    ]
    if sheet_name == "NAR Degradation Analysis":
        final_cols.append("3Days (NAR)")
    final_cols.extend(["Degradation", "Outage Category", "Mains Category"])
    result = result[final_cols]

    workbook = writer.book
    ws = workbook.add_worksheet(sheet_name)
    writer.sheets[sheet_name] = ws
    header_fmt = workbook.add_format(
        {"font_size": 9, "bold": True, "bg_color": "#BDD7EE", "border": 1, "align": "center"}
    )
    num_fmt = workbook.add_format({"font_size": 9, "num_format": "0.0"})
    text_fmt = workbook.add_format({"font_size": 9})

    banner_text = (
        f"Benchmark Date: {benchmark_date} | Current Availability Date: {latest_date}"
    )
    ws.merge_range(0, 0, 0, len(final_cols) - 1, banner_text, header_fmt)
    header_row = 1
    ws.write_row(header_row, 0, final_cols, header_fmt)
    numeric_cols = {"Benchmark Availability", "Current Availability", "Degradation"}
    if sheet_name == "NAR Degradation Analysis":
        numeric_cols.add("3Days (NAR)")
    for r in range(len(result)):
        row = result.iloc[r]
        for c, col in enumerate(final_cols):
            value = row[col]
            excel_row = header_row + 1 + r
            if col in numeric_cols:
                if pd.notna(value):
                    ws.write_number(excel_row, c, float(value), num_fmt)
                else:
                    ws.write(excel_row, c, "-", text_fmt)
            else:
                ws.write(excel_row, c, value if pd.notna(value) else "-", text_fmt)

    if len(result) > 0:
        deg_idx = final_cols.index("Degradation")
        ws.conditional_format(
            header_row + 1,
            deg_idx,
            header_row + len(result),
            deg_idx,
            {"type": "3_color_scale", "min_color": "#F8696B", "mid_color": "#FFEB84", "max_color": "#FFFFFF"},
        )
    ws.freeze_panes(header_row + 1, 1)
    for idx in range(len(final_cols)):
        ws.set_column(idx, idx, 18)

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily availability report with deep dive on DG sites")
    parser.add_argument("--tch", required=True, help="Path to TCH (2G-Site Level stats) file")
    parser.add_argument("--nar", required=True, help="Path to NAR file")
    parser.add_argument("--sites", required=True, help="Path to Site details file")
    parser.add_argument("--outdir", required=True, help="Output directory for the generated report")
    args = parser.parse_args()
    site_df = pd.read_excel(args.sites)
    required_cols = ["SiteName", "Built/Guest", "OMO", "Stations", "Belt", "Power Status"]
    site_df = site_df[required_cols]
    tch_df = read_and_prepare(
        args.tch,
        "2G-Site Level stats",
        "Site NAME",
        TCH_TEXT_COLUMNS,
        site_df,
        merge_key="Site NAME",
        skiprows=3,
    )
    tch_df = add_site_details_columns(tch_df, args.sites, ['Belt', 'Built/Guest'])
    nar_df = read_and_prepare(
        args.nar,
        "NAR",
        "SiteName",
        ["SiteName", "Belt", "Built/Guest", "Power Status"],
        site_df,
        merge_key="SiteName",
    )
    nar_df.rename(columns={"SiteName": "Site NAME"}, inplace=True)
    fuel_df = pd.read_excel(r"E:\Python\Degraded Sites\Fueling Report.xlsx", sheet_name="RAN Refueling", skiprows=1)
    today = datetime.date.today().strftime("%Y-%m-%d")
    out_path = os.path.join(args.outdir, f"DailyAvailability_{today}.xlsx")

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        write_with_formatting(writer, "2G-Site Level stats", tch_df)
        write_with_formatting(writer, "NAR", nar_df)
        generate_station_summary(writer, tch_df)
        generate_deep_dive(writer, tch_df)
        tchvsnar_df = generate_tch_vs_nar(writer, tch_df, nar_df)
        generate_motivation_summary(writer, tch_df, tchvsnar_df)
        generate_belt_overview(writer, tch_df, fuel_df, tchvsnar_df)
        generate_2g_4g_delta(writer, tch_df, args.tch)
        generate_degradation_analysis(writer, tch_df, r"E:\Python\Degraded Sites\Outage Report.xlsx")
        generate_degradation_analysis(writer, nar_df, r"E:\Python\Degraded Sites\Outage Report.xlsx", sheet_name="NAR Degradation Analysis")
    print(f"✅ Full report generated: {out_path}")

if __name__ == "__main__":
    main()