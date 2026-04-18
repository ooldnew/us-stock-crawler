# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
import os

# ====================== 配置 ======================
START_YEAR = 2021
END_YEAR = 2025
TOP_COUNT = 1000  # 每年取前1000成交额
SAVE_DIR = r"G:\回测\美股年度前1000_前复权K线"

# 东财美股接口（官方、稳定、前复权）
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://quote.eastmoney.com/"
}

# 自动创建目录
os.makedirs(SAVE_DIR, exist_ok=True)

# ====================== 1. 获取美股列表（按成交额排序） ======================
def get_us_stock_rank(year):
    print(f"正在获取 {year} 年美股成交额排名...")
    all_stocks = []

    # 东财美股列表分页（最多取60页 = 6000只，足够筛前1000）
    for page in range(1, 61):
        try:
            url = f"https://62.push2.eastmoney.com/api/qt/clist/get"
            params = {
                "pn": page,
                "pz": 100,
                "fs": "m:105,m:106",
                "fields": "f12,f14,f62",
                "fld": "f62",
                "cmd": "1"
            }
            res = requests.get(url, headers=HEADERS, params=params, timeout=10)
            data = res.json()
            if not data["data"]["diff"]:
                break

            for item in data["data"]["diff"]:
                code = item["f12"]
                name = item["f14"]
                amount = item["f62"]
                all_stocks.append({
                    "代码": code,
                    "名称": name,
                    "成交额": float(amount) if amount else 0
                })

            time.sleep(0.3)
        except Exception as e:
            print(f"第{page}页失败: {e}")
            time.sleep(2)

    # 排序 → 取前1000
    df = pd.DataFrame(all_stocks)
    df = df.sort_values("成交额", ascending=False).head(TOP_COUNT)
    df["排名"] = range(1, len(df)+1)
    df = df[["排名", "代码", "名称", "成交额"]]
    return df

# ====================== 2. 下载单只股票 前复权日线 ======================
def get_daily_kline(code, year):
    try:
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"105.{code}" if code.isdigit() else f"106.{code}",
            "beg": f"{year}0101",
            "end": f"{year}1231",
            "klt": 101,        # 日线
            "fqt": 1,          # 前复权（回测必须）
            "fields1": "f1,f2,f3,f4,f5,f6,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60"
        }

        res = requests.get(url, headers=HEADERS, params=params, timeout=8)
        data = res.json()

        klines = data["data"]["klines"]
        rows = []
        for line in klines:
            arr = line.split(",")
            rows.append({
                "股票代码": code,
                "日期": arr[0],
                "开盘": float(arr[1]),
                "收盘": float(arr[2]),
                "最高": float(arr[3]),
                "最低": float(arr[4]),
                "成交量": int(arr[5]),
                "成交额": float(arr[6]),
                "涨跌幅": float(arr[8])
            })
        return pd.DataFrame(rows)
    except:
        return None

# ====================== 3. 按年度下载（主函数） ======================
def run_year(year):
    print(f"\n==================================")
    print(f"          处理 {year} 年数据         ")
    print(f"==================================")

    # 1. 获取年度前1000成交额股票
    top_df = get_us_stock_rank(year)
    top_path = os.path.join(SAVE_DIR, f"{year}_美股成交额前1000.csv")
    top_df.to_csv(top_path, index=False, encoding="utf-8-sig")
    print(f"✅ {year} 年前1000名单已保存")

    # 2. 下载每只K线
    all_kline = []
    codes = top_df["代码"].tolist()

    for i, code in enumerate(codes):
        print(f"[{year}][{i+1}/{len(codes)}] 下载 {code}...", end=" ")
        df = get_daily_kline(code, year)

        if df is not None and len(df) > 0:
            all_kline.append(df)
            print("✅")
        else:
            print("❌")

        time.sleep(1)  # 防封IP（必须）

    # 3. 保存年度K线
    if all_kline:
        full_kline = pd.concat(all_kline, ignore_index=True)
        kline_path = os.path.join(SAVE_DIR, f"{year}_全部K线(前复权).csv")
        full_kline.to_csv(kline_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ {year} 年K线全部下载完成！")

# ====================== 主程序 ======================
if __name__ == "__main__":
    print("🚀 东财美股 年度成交额前1000 + 前复权日线 下载工具")
    print("📁 保存路径:", SAVE_DIR)
    print("="*50)

    for year in range(START_YEAR, END_YEAR + 1):
        run_year(year)
        time.sleep(10)

    print("\n🎉 2021-2025 全部完成！")
    input("按回车键退出")
