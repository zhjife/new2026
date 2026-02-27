import requests
from curl_cffi import requests as cffi_requests

import threading
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
import xlsxwriter
import random
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

requests.get = lambda url, **kwargs: cffi_requests.get(url, impersonate="chrome120", verify=False, **kwargs)
requests.post = lambda url, **kwargs: cffi_requests.post(url, impersonate="chrome120", verify=False, **kwargs)


# ==========================================
# 模块〇：数据源切换（雪球为主，东方财富为辅）
# ==========================================
token_lock = threading.Lock()
XUEQIU_TOKEN = ""

def get_xueqiu_token():
    global XUEQIU_TOKEN
    if XUEQIU_TOKEN:
        return XUEQIU_TOKEN
    with token_lock:
        if XUEQIU_TOKEN:
            return XUEQIU_TOKEN
        try:
            print(">>> 正在启动 Playwright 模拟真实浏览器获取 Token...", flush=True)
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                # 【修复卡死】改用 domcontentloaded，不去死等网络空闲
                page.goto("https://xueqiu.com/", wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000) # 强制给它2秒钟写入Cookie
                
                cookies = context.cookies()
                for c in cookies:
                    if c['name'] == 'xq_a_token':
                        XUEQIU_TOKEN = c['value']
                        print(f">>> 成功突破拦截，获取到 Token: {XUEQIU_TOKEN[:15]}...", flush=True)
                        break
                browser.close()
        except Exception as e:
            print(f">>> [警告] Playwright 获取 Token 失败: {e}", flush=True)
            
    return XUEQIU_TOKEN

def get_market_spot_data():
    print("   ↳ [Xueqiu] 启动主数据源 (自动翻页获取模式 + 宽进严出)...", flush=True)
    data_list = list()
    try:
        token = get_xueqiu_token()
        headers = {"Cookie": f"xq_a_token={token}"} if token else {}
        
        current_page = 1
        max_page = 70
        page_size = 90
        
        while current_page <= max_page:
            url = "https://stock.xueqiu.com/v5/stock/screener/quote/list.json"
            params = {
                "page": current_page,
                "size": page_size,
                "order": "desc",
                "order_by": "amount",
                "exchange": "CN",
                "market": "CN",
                "type": "sha,sza,bja"
            }
            res_data = requests.get(url, params=params, headers=headers, timeout=10)
            
            if res_data.status_code != 200:
                print(f"     ⚠️ 第 {current_page} 页请求失败，状态码: {res_data.status_code}，停止翻页。", flush=True)
                break
                
            json_data = res_data.json()
            if 'data' not in json_data or 'list' not in json_data['data']:
                print(f"     ⚠️ 第 {current_page} 页数据格式异常，停止翻页。", flush=True)
                break
                
            raw_list = json_data['data']['list']
            if not raw_list:
                print("     ✅ 已读取到空页，所有页面读取完毕。", flush=True)
                break
                
            page_valid_count = 0
            for item in raw_list:
                try:
                    raw_code = str(item.get('symbol', ''))
                    code = re.sub(r'^[A-Za-z]+', '', raw_code)
                    name = str(item.get('name', ''))
                    price = float(item.get('current') or 0)
                    turnover = float(item.get('turnover_rate') or 0)
                    cap = float(item.get('market_capital') or 0)
                    amount = float(item.get('amount') or 0)
                    vol_ratio = float(item.get('volume_ratio') or 0)
                    chg = float(item.get('percent') or 0)
                    
                    if (not code.startswith(('30', '688', '8', '4'))) and \
                       ('ST' not in name) and ('退' not in name):
                        data_list.append({
                            "代码": code,
                            "名称": name,
                            "最新价": price,
                            "涨跌幅": chg,
                            "成交额": amount,
                            "总市值": cap,
                            "量比": vol_ratio,
                            "换手率": turnover
                        })
                        page_valid_count += 1
                except Exception:
                    continue
                    
            print(f"     📄 进度 [{current_page}/{max_page}] 页 | 获取原始数据: {len(raw_list)} 条 ➜ 初筛合格 (仅主板): {page_valid_count} 条", flush=True)    
            
            current_page += 1
            time.sleep(random.uniform(0.3, 1.0))
            
        print(f"   ✅ [Xueqiu] 翻页数据获取结束: 累计获取 {len(data_list)} 只初筛标的。", flush=True)
        if not data_list: raise Exception("抓取到的有效数据为空")
        return pd.DataFrame(data_list)
        
    except Exception as e:
        print(f"[数据源告警] 雪球行情获取失败({e})，切换至东方财富备份...", flush=True)
        return ak.stock_zh_a_spot_em()

def get_stock_hist_data(symbol, code):
    try:
        token = get_xueqiu_token()
        headers = {"Cookie": f"xq_a_token={token}"} if token else {}
        url = "https://stock.xueqiu.com/v5/stock/chart/kline.json"
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),
            "begin": ts,
            "period": "day",
            "type": "before",
            "count": -250,
            "indicator": "kline"
        }
        res_data = requests.get(url, params=params, headers=headers, timeout=10)
        if res_data.status_code != 200:
            raise Exception(f"HTTP Status {res_data.status_code}")
        
        data = res_data.json()["data"]
        df = pd.DataFrame(data["item"], columns=data["column"])
        df = df.rename(columns={"timestamp": "日期", "open": "开盘", "high": "最高", "low": "最低", "close": "收盘", "volume": "成交量"})
        df['日期'] = pd.to_datetime(df['日期'], unit='ms').dt.strftime('%Y-%m-%d')
        cols_to_numeric =['开盘', '最高', '最低', '收盘', '成交量']
        df[cols_to_numeric] = df[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
        return df
    except Exception:
        return ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")

def analyze_market_environment():
    print(">>> [Step 1] 正在分析市场风格与环境...", flush=True)
    score = 0
    details = list()
    summary = {} 

    try:
        spot_df = get_market_spot_data()
        df_active = spot_df[spot_df['成交额'] > 0].sort_values(by='成交额', ascending=False).head(2000)
        market_caps = df_active['总市值'] / 1e8 
        
        bins =[0, 80, 200, 500, np.inf]
        labels =['微盘', '小盘', '中盘', '大盘']
        cats = pd.cut(market_caps, bins=bins, labels=labels, right=False)
        counts = cats.value_counts(normalize=True).sort_values(ascending=False)
        
        top1_label = counts.index[0]
        top1_ratio = counts.iloc[0]
        
        index_map = {'微盘': ('sz399303', '国证2000'), '小盘': ('sh000852', '中证1000'), '中盘': ('sh000905', '中证500'), '大盘': ('sh000300', '沪深300')}
        
        if top1_ratio >= 0.60:
            final_style = top1_label
            style_desc = f"单一风格 ({top1_label})"
        else:
            top2_label = counts.index[1]
            order =['微盘', '小盘', '中盘', '大盘']
            final_style = top1_label if order.index(top1_label) < order.index(top2_label) else top2_label
            style_desc = f"混合 ({top1_label}/{top2_label}) -> 锚定偏小"
            
        anchor_code, anchor_name = index_map[final_style]
        print(f"\n   风格判定: {style_desc} | 锚定指数: {anchor_name}", flush=True)
        
        summary['市场风格'] = style_desc
        summary['风险锚点'] = f"{anchor_name} ({anchor_code})"

        total_amt = spot_df['成交额'].sum()
        if total_amt >= 1e12:
            score += 8
            details.append("主板成交额充足")
        else:
            details.append("主板成交额一般")
        summary['全市场成交(仅主板估算)'] = f"{int(total_amt/1e8)} 亿"

        try:
            idx_df = ak.stock_zh_index_daily(symbol=anchor_code)
            ma20 = idx_df['close'].rolling(20).mean().iloc[-1]
            if idx_df['close'].iloc[-1] > ma20: score += 6; details.append(f"{anchor_name}站上MA20")
            else: details.append(f"{anchor_name}跌破MA20")
        except:
            details.append("指数数据缺失")

        up = len(spot_df[spot_df['涨跌幅'] > 0])
        down = len(spot_df[spot_df['涨跌幅'] < 0])
        down = 1 if down == 0 else down
        ratio = up / down
        if ratio >= 1.2: score += 6; details.append("赚钱效应强")
        else: details.append("赚钱效应弱")
        
        summary['涨跌家数比'] = f"{ratio:.2f}"
        summary['总分'] = score
        summary['评分细节'] = " | ".join(details)
        
        if score <= 10: sugg = "空仓休息"
        elif score <= 15: sugg = "轻仓防守"
        else: sugg = "积极参与"
        summary['系统建议'] = sugg

        return score, summary, spot_df

    except Exception as e:
        print(f"环境分析出错: {e}", flush=True)
        return 0, {}, pd.DataFrame()

def calculate_indicators(df):
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA5_VOL'] = df['vol'].rolling(5).mean()
    df['MA10_VOL'] = df['vol'].rolling(10).mean()
    df['MA20_VOL'] = df['vol'].rolling(20).mean()
    
    change = df['close'].diff()
    direction = np.sign(change)
    df['OBV'] = (direction * df['vol']).fillna(0).cumsum()
    
    high_low = df['high'] - df['low']
    high_low = high_low.replace(0, np.nan)
    mf_multiplier = ((df['close'] - df['low']) - (df['high'] - df['close'])) / high_low
    mf_multiplier = mf_multiplier.fillna(0)
    mf_volume = mf_multiplier * df['vol']
    df['CMF'] = mf_volume.rolling(20).sum() / df['vol'].rolling(20).sum()
    return df

def process_single_stock(args):
    symbol, name, market_score, vol_ratio = args
    code = symbol[2:] if symbol.startswith(('sh', 'sz')) else symbol
    
    df = pd.DataFrame()
    max_retries = 3 
    
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(0.1, 0.4)) 
            df = get_stock_hist_data(symbol, code)
            if df is not None and not df.empty and len(df) >= 30:
                break
        except Exception:
            if attempt == max_retries - 1: return None
            time.sleep(1)
            
    if df is None or df.empty or len(df) < 30: 
        return None
        
    try:
        df = df.rename(columns={'日期':'date','开盘':'open','收盘':'close','最高':'high','最低':'low','成交量':'vol'})
        df = calculate_indicators(df)
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        score_a = 0
        if curr['MA5'] > curr['MA10'] > curr['MA20']: score_a += 10
        if curr['close'] > curr['MA20']: score_a += 8
        if curr['close'] > df.iloc[-21:-1]['high'].max(): score_a += 6
        if curr['vol'] >= 2 * curr['MA20_VOL']: score_a += 6

        score_b = 0
        if curr['MA20'] > prev['MA20']: score_b += 8
        touched = (curr['low'] <= curr['MA10']) or (curr['low'] <= curr['MA20'])
        held = (curr['close'] > curr['MA10']) and (curr['close'] > curr['MA20'])
        if touched and held: score_b += 8
        if curr['vol'] <= 0.7 * curr['MA5_VOL']: score_b += 8
        if (curr['high'] - curr['low']) / prev['close'] <= 0.06: score_b += 6

        score_c = 0
        p20_h = df['high'].iloc[-20:].max()
        p20_l = df['low'].iloc[-20:].min()
        if p20_l > 0 and (p20_h - p20_l)/p20_l <= 0.15: score_c += 6
        if curr['MA5_VOL'] < curr['MA10_VOL'] < curr['MA20_VOL']: score_c += 6
        if ((df['high'][-10:]-df['low'][-10:])/df['close'].shift(1)[-10:]).mean() <= 0.05: score_c += 4
        if curr['close'] >= p20_h * 0.95: score_c += 4

        total_score = market_score + score_a + score_b + score_c
        
        pass_filter = (score_a + score_b >= 30) or (score_b >= 18)
        if not pass_filter: return None
        
        decision = "放弃"
        if total_score >= 80: decision = "极高确定性"
        elif total_score >= 70: decision = "高胜率"
        elif total_score >= 60: decision = "观察"
        else: return None

        res = {
            "代码": symbol, "名称": name, "现价": curr['close'], 
            "涨幅%": round((curr['close']-prev['close'])/prev['close']*100, 2),
            "量比": vol_ratio, "总分": total_score, "决策": decision,
            "趋势分(A)": score_a, "回踩分(B)": score_b, "吸筹分(C)": score_c, "市场分": market_score
        }
        
        for label, idx in [('今天', -1), ('昨天', -2), ('前天', -3)]:
            if len(df) >= abs(idx):
                row = df.iloc[idx]
                res[f'CMF_{label}'] = round(row['CMF'], 3) if not np.isnan(row['CMF']) else 0
                res[f'OBV_{label}'] = int(row['OBV'])
            else:
                res[f'CMF_{label}'] = 0
                res[f'OBV_{label}'] = 0
                
        return res
    except Exception:
        return None

def generate_report(market_summary, stock_data, filename):
    print(f"\n>>> 正在生成专业报表: {filename} ...", flush=True)
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    workbook = writer.book
    
    fmt_title = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#DDEBF7'})
    fmt_header = workbook.add_format({'bold': True, 'font_size': 10, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
    fmt_center = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})
    fmt_good = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1, 'align': 'center'}) 
    fmt_up = workbook.add_format({'font_color': 'red', 'align': 'center', 'border': 1})
    fmt_down = workbook.add_format({'font_color': 'green', 'align': 'center', 'border': 1})

    ws_m = workbook.add_worksheet("市场环境")
    ws_m.merge_range('A1:B1', f"市场环境评分看板 ({datetime.datetime.now().strftime('%Y-%m-%d')})", fmt_title)
    row = 1
    for k, v in market_summary.items():
        ws_m.write(row, 0, k, fmt_header)
        cell_fmt = fmt_good if (k == '总分' and v >= 16) or (k == '系统建议' and "积极" in str(v)) else fmt_center
        ws_m.write(row, 1, v, cell_fmt)
        row += 1
    ws_m.set_column('A:A', 25)
    ws_m.set_column('B:B', 50)

    if stock_data:
        df = pd.DataFrame(stock_data)
        base_cols =['代码', '名称', '现价', '涨幅%', '量比', '总分', '决策', '趋势分(A)', '回踩分(B)', '吸筹分(C)', '市场分']
        final_cols =[c for c in base_cols +['CMF_前天', 'CMF_昨天', 'CMF_今天', 'OBV_前天', 'OBV_昨天', 'OBV_今天'] if c in df.columns]
        
        df = df[final_cols].sort_values(by="总分", ascending=False)
        df.to_excel(writer, sheet_name='选股池', index=False, startrow=0)
        ws_s = writer.sheets['选股池']
        
        for i, col in enumerate(final_cols):
            ws_s.write(0, i, col, fmt_header)
            ws_s.set_column(i, i, 12 if "名称" in col else (15 if "决策" in col else 10))

        for r in range(len(df)):
            x_row = r + 1
            dec = df.iloc[r]['决策']
            ws_s.write(x_row, final_cols.index('决策'), dec, fmt_good if "高" in dec else fmt_center)
            
            pct = df.iloc[r]['涨幅%']
            ws_s.write(x_row, final_cols.index('涨幅%'), pct, fmt_up if pct > 0 else (fmt_down if pct < 0 else fmt_center))
            
            for col in final_cols:
                col_idx = final_cols.index(col)
                if col not in ['决策', '涨幅%']:
                    ws_s.write(x_row, col_idx, df.iloc[r][col], fmt_center)

    writer.close()
    print(f"✅ 报表已生成: {filename}", flush=True)

def main():
    print("==========================================", flush=True)
    print("   A股全市场量化扫描 (GitHub Actions版)   ", flush=True)
    print("==========================================", flush=True)
    
    get_xueqiu_token()
    
    m_score, m_summary, spot_df = analyze_market_environment()
    if m_score <= 5:
        print("❌ 市场评分过低，强制空仓，停止运行。", flush=True)
        return

    print("\n>>> [Step 2] 准备股票列表...", flush=True)
    valid_stocks = spot_df[spot_df['成交额'] >= 50000000]
    tasks = list()
    for _, row in valid_stocks.iterrows():
        c = str(row['代码'])
        tasks.append((f"{'sh' if c.startswith('6') else ('sz' if c.startswith(('0','3')) else 'bj')}{c}", row['名称'], m_score, row.get('量比', 0)))
        
    print(f"   待多线程扫描股票: {len(tasks)} 只\n", flush=True)

    print(">>> [Step 3] 启动多线程扫描 (预计 3-5 分钟)...", flush=True)
    results = list()
    start_t = time.time()
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_single_stock, t): t for t in tasks}
        done_count = 0
        for future in as_completed(futures):
            res = future.result()
            if res: results.append(res)
            
            done_count += 1
            # 【修复日志被吞】每完成 50 只股票，就明确打印一行新进度，保证能看清！
            if done_count % 50 == 0 or done_count == len(tasks):
                elapsed = time.time() - start_t
                speed = done_count / elapsed if elapsed > 0 else 0
                print(f"   ➤ 进度: {done_count}/{len(tasks)} | 命中: {len(results)} 只 | 速度: {speed:.1f} 只/秒", flush=True)
                
    print(f"\n✅ 扫描完成! 耗时: {int(time.time() - start_t)} 秒", flush=True)

    if results:
        fname = f"Quant_Final_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        generate_report(m_summary, results, fname)
    else:
        print("未发现符合条件的标的。", flush=True)

if __name__ == "__main__":
    main()
