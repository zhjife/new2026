import requests
from curl_cffi import requests as cffi_requests

# 【魔法代码】将 Python 标准库的底层悄悄替换为带有真实 Chrome 浏览器指纹的请求
requests.get = lambda url, **kwargs: cffi_requests.get(url, impersonate="chrome120", verify=False, **kwargs)
requests.post = lambda url, **kwargs: cffi_requests.post(url, impersonate="chrome120", verify=False, **kwargs)

import os
import threading

# 【核心修复】强制 Python 忽略电脑上的一切 VPN 和代理设置，使用本地纯净网络直连！
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
import xlsxwriter
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright

# ==========================================
# 模块〇：数据源切换（雪球为主，东方财富为辅）
# ==========================================
token_lock = threading.Lock()
XUEQIU_TOKEN = ""

def get_xueqiu_token():
    """使用 Playwright 无头浏览器获取雪球接口调用必须的授权 Cookie，强力防封IP"""
    global XUEQIU_TOKEN
    if XUEQIU_TOKEN:
        return XUEQIU_TOKEN
    with token_lock:
        if XUEQIU_TOKEN:
            return XUEQIU_TOKEN
        try:
            print(">>> 正在启动 Playwright 模拟真实浏览器获取 Token...")
            with sync_playwright() as p:
                # 启动无头浏览器，注入真实 User-Agent
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
                page = context.new_page()
                # 访问雪球主页并等待网络空闲，确保防御检测通过
                page.goto("https://xueqiu.com/", wait_until="networkidle", timeout=30000)
                
                # 提取 Cookie
                cookies = context.cookies()
                for c in cookies:
                    if c['name'] == 'xq_a_token':
                        XUEQIU_TOKEN = c['value']
                        print(f">>> 成功突破拦截，获取到 Token: {XUEQIU_TOKEN[:15]}...")
                        break
                browser.close()
        except Exception as e:
            print(f">>> [警告] Playwright 获取 Token 失败: {e}")
            
    return XUEQIU_TOKEN

def get_market_spot_data():
    """获取全市场实时行情数据（雪球优先，东财备份）"""
    try:
        token = get_xueqiu_token()
        headers = {"Cookie": f"xq_a_token={token}"} if token else {}
        url = "https://stock.xueqiu.com/v5/stock/screener/quote/list.json"
        params = {
            "page": 1,
            "size": 6000,
            "order": "desc",
            "order_by": "amount",
            "exchange": "CN",
            "market": "CN",
            "type": "sha,sza,bja"
        }
        res_data = requests.get(url, params=params, headers=headers, timeout=15)
        if res_data.status_code != 200:
            raise Exception(f"HTTP Status {res_data.status_code}")
        
        data_list = res_data.json()["data"]["list"]
        df = pd.DataFrame(data_list)
        
        # 映射为与原有东方财富(Akshare)一致的列名
        df = df.rename(columns={
            "symbol": "代码",
            "name": "名称",
            "current": "最新价",
            "percent": "涨跌幅",
            "amount": "成交额",
            "market_capital": "总市值",
            "volume_ratio": "量比",
            "chg": "涨跌额",
            "volume": "成交量",
            "amplitude": "振幅",
            "high": "最高",
            "low": "最低",
            "open": "今开",
            "last_close": "昨收",
            "turnover_rate": "换手率"
        })
        # 去除雪球代码自带的SH/SZ前缀，保持与原逻辑一致（纯数字）
        df['代码'] = df['代码'].astype(str).str.replace(r'^[A-Za-z]+', '', regex=True)
        return df
    except Exception as e:
        print(f"   [数据源告警] 雪球实时行情获取失败({e})，自动切换至东方财富备份...")
        return ak.stock_zh_a_spot_em()

def get_stock_hist_data(symbol, code):
    """获取个股历史K线数据（雪球优先，东财备份）"""
    try:
        token = get_xueqiu_token()
        headers = {"Cookie": f"xq_a_token={token}"} if token else {}
        url = "https://stock.xueqiu.com/v5/stock/chart/kline.json"
        ts = int(time.time() * 1000)
        params = {
            "symbol": symbol.upper(),  # 雪球接口需要大写前缀如 SH600000
            "begin": ts,
            "period": "day",
            "type": "before",          # 前复权
            "count": -250,             # 抓取最近约1年数据足以计算指标
            "indicator": "kline"
        }
        res_data = requests.get(url, params=params, headers=headers, timeout=10)
        if res_data.status_code != 200:
            raise Exception(f"HTTP Status {res_data.status_code}")
        
        data = res_data.json()["data"]
        df = pd.DataFrame(data["item"], columns=data["column"])
        
        # 映射列名
        df = df.rename(columns={
            "timestamp": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
        })
        # 转换时间戳
        df['日期'] = pd.to_datetime(df['日期'], unit='ms').dt.strftime('%Y-%m-%d')
        
        # 确保关键计算列是数值类型
        cols_to_numeric =['开盘', '最高', '最低', '收盘', '成交量']
        df[cols_to_numeric] = df[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
        return df
    except Exception:
        # 回退东方财富接口
        return ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")

# ==========================================
# 模块一：市场环境与风格动态锚定
# ==========================================

def analyze_market_environment():
    """
    分析全市场风格，确定风险锚点，计算市场环境分
    """
    print(">>> [Step 1] 正在分析市场风格与环境...")
    score = 0
    details =[]
    summary = {} 

    try:
        # 1. 获取全市场实时数据 (使用新的雪球封装函数)
        spot_df = get_market_spot_data()
        
        # 2. 风格判定 (取成交额前2000只活跃股作为样本)
        df_active = spot_df[spot_df['成交额'] > 0].sort_values(by='成交额', ascending=False).head(2000)
        market_caps = df_active['总市值'] / 1e8 
        
        # 市值分类
        bins =[0, 80, 200, 500, np.inf]
        labels =['微盘', '小盘', '中盘', '大盘']
        cats = pd.cut(market_caps, bins=bins, labels=labels, right=False)
        counts = cats.value_counts(normalize=True).sort_values(ascending=False)
        
        top1_label = counts.index[0]
        top1_ratio = counts.iloc[0]
        
        # 锚定映射
        index_map = {
            '微盘': ('sz399303', '国证2000'),
            '小盘': ('sh000852', '中证1000'),
            '中盘': ('sh000905', '中证500'),
            '大盘': ('sh000300', '沪深300')
        }
        
        if top1_ratio >= 0.60:
            final_style = top1_label
            style_desc = f"单一风格 ({top1_label})"
        else:
            top2_label = counts.index[1]
            order =['微盘', '小盘', '中盘', '大盘']
            # 混合风格取偏小
            final_style = top1_label if order.index(top1_label) < order.index(top2_label) else top2_label
            style_desc = f"混合 ({top1_label}/{top2_label}) -> 锚定偏小"
            
        anchor_code, anchor_name = index_map[final_style]
        print(f"   风格判定: {style_desc} | 锚定指数: {anchor_name}")
        
        summary['市场风格'] = style_desc
        summary['风险锚点'] = f"{anchor_name} ({anchor_code})"

        # 3. 计算环境分
        total_amt = spot_df['成交额'].sum()
        if total_amt >= 1e12:
            score += 8
            details.append("成交额充足")
        else:
            details.append("成交额一般")
        summary['全市场成交'] = f"{int(total_amt/1e8)} 亿"

        try:
            idx_df = ak.stock_zh_index_daily(symbol=anchor_code)
            ma20 = idx_df['close'].rolling(20).mean().iloc[-1]
            if idx_df['close'].iloc[-1] > ma20:
                score += 6
                details.append(f"{anchor_name}站上MA20")
            else:
                details.append(f"{anchor_name}跌破MA20")
        except:
            details.append("指数数据缺失")

        up = len(spot_df[spot_df['涨跌幅'] > 0])
        down = len(spot_df[spot_df['涨跌幅'] < 0])
        down = 1 if down == 0 else down
        ratio = up / down
        if ratio >= 1.2:
            score += 6
            details.append("赚钱效应强")
        else:
            details.append("赚钱效应弱")
        
        summary['涨跌家数比'] = f"{ratio:.2f}"
        summary['总分'] = score
        summary['评分细节'] = " | ".join(details)
        
        if score <= 10: sugg = "空仓休息"
        elif score <= 15: sugg = "轻仓防守"
        else: sugg = "积极参与"
        summary['系统建议'] = sugg

        return score, summary, spot_df

    except Exception as e:
        print(f"环境分析出错: {e}")
        return 0, {}, pd.DataFrame()

# ==========================================
# 模块二：技术指标计算
# ==========================================

def calculate_indicators(df):
    """
    计算基础均线、OBV、CMF
    """
    # 基础均线
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA5_VOL'] = df['vol'].rolling(5).mean()
    df['MA10_VOL'] = df['vol'].rolling(10).mean()
    df['MA20_VOL'] = df['vol'].rolling(20).mean()
    
    # --- OBV (能量潮) ---
    change = df['close'].diff()
    direction = np.sign(change)
    df['OBV'] = (direction * df['vol']).fillna(0).cumsum()
    
    # --- CMF (查金资金流) ---
    high_low = df['high'] - df['low']
    high_low = high_low.replace(0, np.nan)
    mf_multiplier = ((df['close'] - df['low']) - (df['high'] - df['close'])) / high_low
    mf_multiplier = mf_multiplier.fillna(0)
    mf_volume = mf_multiplier * df['vol']
    df['CMF'] = mf_volume.rolling(20).sum() / df['vol'].rolling(20).sum()
    
    return df

# ==========================================
# 模块三：个股全模型扫描 (多线程单元)
# ==========================================

def process_single_stock(args):
    """
    线程工作函数: 接收 (代码, 名称, 市场分, 量比)
    """
    symbol, name, market_score, vol_ratio = args
    code = symbol[2:] if symbol.startswith(('sh', 'sz')) else symbol
    
    df = pd.DataFrame()
    max_retries = 3  # 最大重试次数
    
    # ---------------- 核心修改区：增加重试与防爬休眠 ----------------
    for attempt in range(max_retries):
        try:
            # 每次请求前随机休眠 0.1 ~ 0.5 秒，打乱请求节奏，伪装成人类
            time.sleep(random.uniform(0.1, 0.5)) 
            
            # 【更换为雪球数据，失败走东财备份】获取K线
            df = get_stock_hist_data(symbol, code)
            
            # 如果成功获取且不为空，跳出重试循环
            if df is not None and not df.empty and len(df) >= 30:
                break
        except Exception as e:
            # 如果是最后一次重试仍失败，则放弃
            if attempt == max_retries - 1:
                return None
            # 失败后多等一会儿再重试 (1秒、2秒...)
            time.sleep(1 * (attempt + 1))
            
    # 如果经过重试依然没有拿到合格数据，直接返回
    if df is None or df.empty or len(df) < 30: 
        return None
    # ----------------------------------------------------------------
        
    try:
        df = df.rename(columns={'日期':'date','开盘':'open','收盘':'close','最高':'high','最低':'low','成交量':'vol'})
        
        # 计算指标
        df = calculate_indicators(df)
        
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        
        # --- 模型打分 ---
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
        
        # 决策过滤
        pass_filter = (score_a + score_b >= 30) or (score_b >= 18)
        if not pass_filter: return None
        
        decision = "放弃"
        if total_score >= 80: decision = "极高确定性"
        elif total_score >= 70: decision = "高胜率"
        elif total_score >= 60: decision = "观察"
        else: return None

        # --- 结果组装 ---
        res = {
            "代码": symbol, "名称": name, 
            "现价": curr['close'], 
            "涨幅%": round((curr['close']-prev['close'])/prev['close']*100, 2),
            "量比": vol_ratio,  # 新增量比
            "总分": total_score, "决策": decision,
            "趋势分(A)": score_a, "回踩分(B)": score_b, "吸筹分(C)": score_c,
            "市场分": market_score
        }
        
        # 提取最近3天指标 (Day-3, Day-2, Day-1) -> 对应 前天, 昨天, 今天
        days_map =[('今天', -1), ('昨天', -2), ('前天', -3)]
        
        for label, idx in days_map:
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

# ==========================================
# 模块四：Excel 报表生成 (分组与注释)
# ==========================================

def generate_report(market_summary, stock_data, filename):
    print(f">>> 正在生成专业报表: {filename} ...")
    
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    workbook = writer.book
    
    # --- 样式定义 ---
    fmt_title = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#DDEBF7'})
    fmt_header = workbook.add_format({'bold': True, 'font_size': 10, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center', 'valign': 'vcenter', 'text_wrap': True})
    fmt_center = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})
    fmt_good = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1, 'align': 'center'}) 
    fmt_up = workbook.add_format({'font_color': 'red', 'align': 'center', 'border': 1})
    fmt_down = workbook.add_format({'font_color': 'green', 'align': 'center', 'border': 1})
    fmt_obv = workbook.add_format({'num_format': '#,##0', 'align': 'center', 'border': 1})

    # Sheet 1: 市场环境
    ws_m = workbook.add_worksheet("市场环境")
    ws_m.merge_range('A1:B1', f"市场环境评分看板 ({datetime.datetime.now().strftime('%Y-%m-%d')})", fmt_title)
    
    row = 1
    for k, v in market_summary.items():
        ws_m.write(row, 0, k, fmt_header)
        cell_fmt = fmt_center
        if k == '总分' and v >= 16: cell_fmt = fmt_good
        if k == '系统建议' and "积极" in str(v): cell_fmt = fmt_good
        ws_m.write(row, 1, v, cell_fmt)
        row += 1
    ws_m.set_column('A:A', 20)
    ws_m.set_column('B:B', 50)

    # Sheet 2: 选股结果池
    if stock_data:
        df = pd.DataFrame(stock_data)
        
        base_cols =['代码', '名称', '现价', '涨幅%', '量比', '总分', '决策', '趋势分(A)', '回踩分(B)', '吸筹分(C)', '市场分']
        cmf_cols =['CMF_前天', 'CMF_昨天', 'CMF_今天']
        obv_cols =['OBV_前天', 'OBV_昨天', 'OBV_今天']
        
        final_cols = base_cols + cmf_cols + obv_cols
        final_cols =[c for c in final_cols if c in df.columns]
        
        df = df[final_cols] 
        df = df.sort_values(by="总分", ascending=False)
        
        df.to_excel(writer, sheet_name='选股池', index=False, startrow=0)
        ws_s = writer.sheets['选股池']
        
        comments = {
            "总分": "公式 = 市场分(20) + A(30) + B(30) + C(20)\n分数越高确定性越强。",
            "决策": "≥80: 极高确定性\n70-79: 高胜率\n60-69: 观察\n<60: 放弃",
            "趋势分(A)": "满分30分。关注趋势多头与倍量突破。",
            "回踩分(B)": "满分30分。关注缩量回踩均线不破。",
            "吸筹分(C)": "满分20分。关注箱体振幅<15%及量能萎缩。",
            "量比": "衡量相对成交量。\n>1: 放量\n>2: 明显活跃\n<0.7: 缩量\n结合价格看：低位放量为佳。",
            "CMF_今天": "Chaikin Money Flow (资金流向)。\n>0.1: 资金流入\n>0.25: 强势流入\n观察技巧：看前天->昨天->今天是否由负转正或连续递增。",
            "OBV_今天": "能量潮 (成交量趋势)。\n绝对值无意义，重点看趋势。\n买点：股价横盘震荡，而OBV曲线一路向上（底背离吸筹）。"
        }
        
        for i, col in enumerate(final_cols):
            ws_s.write(0, i, col, fmt_header)
            cmt_text = None
            if col in comments: cmt_text = comments[col]
            elif "CMF" in col: cmt_text = comments.get("CMF_今天")
            elif "OBV" in col: cmt_text = comments.get("OBV_今天")
            if cmt_text:
                ws_s.write_comment(0, i, cmt_text, {'x_scale': 2.5, 'y_scale': 1.5})
            
            width = 10
            if "名称" in col: width = 12
            if "决策" in col: width = 15
            if "OBV" in col: width = 13
            if "CMF" in col: width = 11
            ws_s.set_column(i, i, width)

        for r in range(len(df)):
            x_row = r + 1
            dec = df.iloc[r]['决策']
            d_fmt = fmt_good if ("极高" in dec or "高胜率" in dec) else fmt_center
            ws_s.write(x_row, final_cols.index('决策'), dec, d_fmt)
            
            pct = df.iloc[r]['涨幅%']
            p_fmt = fmt_up if pct > 0 else (fmt_down if pct < 0 else fmt_center)
            ws_s.write(x_row, final_cols.index('涨幅%'), pct, p_fmt)
            
            vr = df.iloc[r].get('量比', 0)
            vr = 0 if pd.isna(vr) else vr
            v_fmt = workbook.add_format({'num_format': '0.00', 'align': 'center', 'border': 1})
            if vr >= 2: v_fmt.set_font_color('red')
            elif vr <= 0.7: v_fmt.set_font_color('blue')
            ws_s.write(x_row, final_cols.index('量比'), vr, v_fmt)

            for col in final_cols:
                col_idx = final_cols.index(col)
                if "CMF" in col:
                    val = df.iloc[r][col]
                    c_fmt = workbook.add_format({'num_format': '0.000', 'align': 'center', 'border': 1})
                    if val > 0.1: c_fmt.set_font_color('red')
                    ws_s.write(x_row, col_idx, val, c_fmt)
                elif "OBV" in col:
                    ws_s.write(x_row, col_idx, df.iloc[r][col], fmt_obv)
                elif col == "总分":
                     ws_s.write(x_row, col_idx, df.iloc[r][col], fmt_center)

    writer.close()
    print(f"✅ 报表已生成: {filename}")

# ==========================================
# 主程序
# ==========================================

def main():
    print("==========================================")
    print("   A股全市场量化扫描 (GitHub Actions版)   ")
    print("==========================================")
    
    # 提前初始化好雪球Token，防阻塞
    get_xueqiu_token()
    
    # 1. 市场环境分析
    m_score, m_summary, spot_df = analyze_market_environment()
    
    if m_score <= 5:
        print("❌ 市场评分过低，强制空仓，停止运行。")
        return

    # 2. 准备任务列表
    print("\n>>>[Step 2] 准备股票列表...")
    valid_stocks = spot_df[spot_df['成交额'] > 0]
    valid_stocks = valid_stocks[~valid_stocks['名称'].str.contains("ST")]
    valid_stocks = valid_stocks[valid_stocks['成交额'] >= 50000000]
    
    tasks =[]
    for _, row in valid_stocks.iterrows():
        c = str(row['代码'])
        name = row['名称']
        vol_ratio = row.get('量比', 0) 
        
        if c.startswith('6'): full = f"sh{c}"
        elif c.startswith(('0','3')): full = f"sz{c}"
        else: full = f"bj{c}"
        
        tasks.append((full, name, m_score, vol_ratio))
        
    print(f"   待扫描股票: {len(tasks)} 只")

    # 3. 多线程扫描
    print("\n>>> [Step 3] 启动多线程扫描 (预计 3-5 分钟)...")
    
    # 【修复截断Bug】：换一种写法初始化列表
    results = list()
    start_t = time.time()
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_single_stock, t): t for t in tasks}
        
        done_count = 0
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            
            done_count += 1
            if done_count % 100 == 0:
                elapsed = time.time() - start_t
                speed = done_count / elapsed
                print(f"   进度: {done_count}/{len(tasks)} | 命中: {len(results)} | 速度: {speed:.1f} 只/秒", end='\r')
                
    print(f"\n✅ 扫描完成! 耗时: {int(time.time() - start_t)} 秒")

    # 4. 生成 Excel
    if results:
        fname = f"Quant_Final_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        generate_report(m_summary, results, fname)
    else:
        print("未发现符合条件的标的。")

if __name__ == "__main__":
    main()
