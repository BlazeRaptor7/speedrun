import streamlit as st
import pandas as pd
import os
from pymongo import MongoClient
from collections import defaultdict, deque
from dotenv import load_dotenv
import altair as alt

# Streamlit Page Setup - MUST be first command
st.set_page_config(page_title="Sniper PnL Dashboard", layout="wide")
# ───── Global Styling ─────
st.markdown("""
    <style>
    /* Wrapper container for scrollable table */
    .scrollable {
        max-height: 400px;
        overflow-y: auto;
        overflow-x: auto;
        width: 78vw;
        box-sizing: border-box;
        display: block;
        font-family: 'Epilogue', sans-serif;
        font-size: 14px;
        color: #222;
    }
    
    /* Table styling */
    .scrollable table {
        width: 100%;
        backdrop-filter: blur(10px);
        background: rgba(255, 255, 255, 0.8);
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        margin: 0 auto 0 0;
        table-layout: auto;
        text-align: center;
        border-collapse: separate;
        border-spacing: 0;
    }
    
    /* Table headers and cells */
    .scrollable th,
    .scrollable td {
        padding: 1px 3px;
        text-align: center;
        font-size: 15px;
        font-weight: 300;
    }
    
    /* Header styling */
    .scrollable th {
        background: rgba(70, 70, 70, 0.8);
        color: #fff;
        text-transform: uppercase;
        font-weight: 400;
    }
    
    /* Rounded top corners on first and last th */
    .scrollable th:first-child {
        border-top-left-radius: 8px;
    }
    .scrollable th:last-child {
        border-top-right-radius: 8px;
    }
    /* Remove top padding in main block */
    .block-container {
        padding-top: 0rem !important;
    }
    header[data-testid="stHeader"] {
        background: transparent;
        visibility: visible;
    }
    [data-testid="stSidebarNav"] {
        display: none;
    }
    section[data-testid="stSidebar"] {
        background: rgba(255, 255, 255, 0.15);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border-right: 1px solid rgba(255, 255, 255, 0.3);
        box-shadow: 2px 0 10px rgba(0, 0, 0, 0.1);
    }
    section[data-testid="stSidebar"] .markdown-text-container,
    div[role="radiogroup"] label span,
    h1 {
        color: white !important;
    }
    html, body {
        overflow-x: hidden !important;
    }
    .stApp {
        width: 100vw;
        box-sizing: border-box;
        background: radial-gradient(circle, rgba(18, 73, 97, 1) 0%, rgba(5, 27, 64, 1) 100%);
    }
    .glass-kpi {
        background: rgba(255, 255, 255, 0.12);
        border-radius: 12px;
        padding: 1rem;
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.25);
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
        color: white;
        text-align: center;
    }
    .glass-kpi h4 {
        font-size: 14px;
        font-weight: 600;
        text-align: center;
        margin-bottom: 0; line-height: 1;
        text-transform: uppercase;
        color: #ffffffcc;
    }
    .glass-kpi p {
        font-size: 35px;
        font-weight: bold;
        text-align: center;
        margin: 0; line-height: 1;
    }
    .kpi-inner {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
    }
    # Wrap headers in dataframe 
    [data-testid="stDataFrame"] th div {
        white-space: normal !important;
        word-wrap: break-word;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)


load_dotenv()

# MongoDB Connection
@st.cache_resource
def get_mongo_client():
    """Cache MongoDB client connection"""
    return MongoClient(st.secrets["MONGO_URI"])

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_swap_data():
    """Load and cache swap data from MongoDB"""
    client = get_mongo_client()
    db = client["genesis_tokens_swap_info"]
    
    # swap_collections = [col for col in db.list_collection_names() if col.endswith('_swap')]
    swap_collections = ['jarvis_swap', 'tian_swap', 'badai_swap', 'aispace_swap', 'wint_swap']
    combined_df = pd.DataFrame()

    for col_name in swap_collections:
        token_name = col_name.replace('_swap', '')  
        token_prefix = token_name.upper() + "_"     
        # Build projection dict with correct prefixed column names
        projection = {
            f"{token_prefix}OUT_BeforeTax": 1,
            f"{token_prefix}OUT_AfterTax": 1,
            f"{token_prefix}IN_BeforeTax": 1,
            f"{token_prefix}IN_AfterTax": 1,
            "maker": 1,
            "token_name": 1,
            "swapType": 1,
            "timestamp": 1,
            "timestampReadable": 1,
            "blockNumber": 1,
            "genesis_usdc_price": 1,
            "transactionFee": 1,
            "Tax_1pct": 1
        }
        collection = db[col_name]
        data = list(collection.find({}, projection))
        if not data:
            continue
        df = pd.DataFrame(data)
        df.drop(columns=['_id'], errors='ignore', inplace=True)
        # Remove token prefix from relevant columns
        df.columns = [col.replace(token_prefix, '') if col.startswith(token_prefix) else col for col in df.columns]
        df["token_name"] = token_name.upper()
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    if combined_df.empty:
        return None

    combined_df = combined_df.dropna(subset=['transactionFee'])
    combined_df = combined_df.dropna(subset=['genesis_usdc_price'])
    combined_df['timestampReadable'] = pd.to_datetime(combined_df['timestampReadable'])
    
    return combined_df

@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_launch_blocks():
    """Load and cache launch block information"""
    client = get_mongo_client()
    db = client["genesis_tokens_swap_info"]
    db_persona = client["virtualgenesis"]
    
    try:
        launch_info = list(db["Personas"].find({}, {"symbol": 1, "blockNumber": 1})) 
        launch_df = pd.DataFrame(launch_info)
        
        # Check if the DataFrame has the expected columns
        if not launch_df.empty and 'symbol' in launch_df.columns and 'blockNumber' in launch_df.columns:
            token_launch_blocks = dict(zip(launch_df['symbol'], launch_df['blockNumber']))
        else:
            # use the combined_df to get launch blocks
            combined_df = load_swap_data()
            if combined_df is not None:
                token_launch_blocks = combined_df.sort_values(by='blockNumber').groupby('token_name')['blockNumber'].first().to_dict()
            else:
                token_launch_blocks = {}
            print("Warning: Could not fetch launch info from Personas collection, using fallback method")
    except Exception as e:
        print(f"Error fetching launch info: {e}")
        # use the combined_df to get launch blocks
        combined_df = load_swap_data()
        if combined_df is not None:
            token_launch_blocks = combined_df.sort_values(by='blockNumber').groupby('token_name')['blockNumber'].first().to_dict()
        else:
            token_launch_blocks = {}
    
    return token_launch_blocks

@st.cache_data(ttl=300)  # Cache for 5 minutes
def process_sniper_data(combined_df, token_launch_blocks):
    """Process and cache sniper identification logic"""
    # Identify Large Buys
    buy_df = combined_df[combined_df['swapType'] == 'buy'].copy()
    buy_df = buy_df.sort_values(by=['maker', 'token_name', 'timestampReadable'])
    chunked_buys = []
    time_threshold = pd.Timedelta(minutes=10)

    for (maker, token), group in buy_df.groupby(['maker', 'token_name']):
        current_chunk = []
        current_sum = 0
        chunk_start_time = None
        for row in group.itertuples():
            if not current_chunk:
                chunk_start_time = row.timestampReadable
                current_chunk = [row]
                current_sum = row.OUT_BeforeTax
            else:
                if row.timestampReadable - chunk_start_time <= time_threshold:
                    current_chunk.append(row)
                    current_sum += row.OUT_BeforeTax
                else:
                    if current_sum > 100000:
                        chunked_buys.extend(current_chunk)
                    chunk_start_time = row.timestampReadable
                    current_chunk = [row]
                    current_sum = row.OUT_BeforeTax
        if current_sum > 100000:
            chunked_buys.extend(current_chunk)

    df_chunked_large_buys = pd.DataFrame(chunked_buys).drop_duplicates()
    df_high_gas = df_chunked_large_buys[df_chunked_large_buys['transactionFee'] > 0.000002]

    def is_sniper_buy(row):
        launch_block = token_launch_blocks.get(row['token_name'])
        return launch_block is not None and row['blockNumber'] <= launch_block + 100

    df_sniper_buys = df_high_gas[df_high_gas.apply(is_sniper_buy, axis=1)]

    sells = combined_df[combined_df['swapType'] == 'sell'][['maker', 'timestampReadable', 'token_name']]

    merged = pd.merge(
        df_sniper_buys[['maker', 'timestampReadable', 'token_name']],
        sells,
        on=['maker', 'token_name'],
        suffixes=('_buy', '_sell')
    )

    merged['time_diff'] = (merged['timestampReadable_sell'] - merged['timestampReadable_buy']).dt.total_seconds()
    quick_sells = merged[merged['time_diff'].between(0, 20 * 60)]
    quick_sells_pairs = set(zip(quick_sells['maker'], quick_sells['token_name']))
    potential_sniper_df = df_sniper_buys[
        df_sniper_buys.apply(lambda row: (row['maker'], row['token_name']) in quick_sells_pairs, axis=1)
    ].copy()
    
    return potential_sniper_df, combined_df

@st.cache_data(ttl=300)  # Cache for 5 minutes
def calculate_pnl(potential_sniper_df, combined_df):
    """Calculate and cache PnL results"""
    results = []
    sniper_pairs = potential_sniper_df[['maker', 'token_name']].drop_duplicates()

    for _, row in sniper_pairs.iterrows():
        maker = row['maker']
        token = row['token_name']
        df = combined_df[(combined_df['maker'] == maker) & (combined_df['token_name'] == token)]
        df = df.sort_values(by='timestamp')
        trades_by_maker = defaultdict(list)

        buy_txn_count = (df['swapType'] == 'buy').sum()
        sell_txn_count = (df['swapType'] == 'sell').sum()
        first_buy_time = df.loc[df['swapType'] == 'buy', 'timestampReadable'].min()
        last_sell_time = df.loc[df['swapType'] == 'sell', 'timestampReadable'].max()
        avg_buy_price = df.loc[df['swapType'] == 'buy', 'genesis_usdc_price'].mean()
        avg_sell_price = df.loc[df['swapType'] == 'sell', 'genesis_usdc_price'].mean()
        total_tax_paid = df['Tax_1pct'].sum()
        total_transaction_fee_paid = df['transactionFee'].sum()

        for _, doc in df.iterrows():
            swap_type = doc.get("swapType")
            price = float(doc.get("genesis_usdc_price", 0) or 0)
            if price <= 0:
                continue
            if swap_type == "buy":
                amount_bought_before_tax = float(doc.get("OUT_BeforeTax", 0) or 0)
                amount_received = float(doc.get("OUT_AfterTax", 0) or 0)
                if amount_bought_before_tax <= 0:
                    continue
                trades_by_maker[maker].append({
                    'type': 'buy',
                    'amount_bought_before_tax': amount_bought_before_tax,
                    'amount_received': amount_received,
                    'price': price
                })
            elif swap_type == "sell":
                amount_sold_net = float(doc.get("IN_AfterTax", 0) or 0)
                amount_from_wallet = float(doc.get("IN_BeforeTax", 0) or 0)
                if amount_sold_net <= 0:
                    continue
                trades_by_maker[maker].append({
                    'type': 'sell',
                    'amount_sold_net': amount_sold_net,
                    'amount_from_wallet': amount_from_wallet,
                    'price': price
                })

        total_realized_pnl = 0.0
        remaining_tokens = 0.0
        buy_queue = deque()

        for trade in trades_by_maker[maker]:
            if trade['type'] == 'buy':
                buy_queue.append({
                    'amount': trade['amount_received'],
                    'amount_paid_for': trade['amount_bought_before_tax'],
                    'price': trade['price']
                })
            elif trade['type'] == 'sell':
                remaining_to_match = trade['amount_from_wallet']
                amount_sold_net = trade['amount_sold_net']
                sell_price = trade['price']

                while remaining_to_match > 0 and buy_queue:
                    buy = buy_queue.popleft()
                    matched_amount = min(remaining_to_match, buy['amount'])
                    ratio = matched_amount / buy['amount']
                    matched_paid = buy['amount_paid_for'] * ratio

                    proceeds_ratio = matched_amount / trade['amount_from_wallet']
                    actual_proceeds = amount_sold_net * sell_price * proceeds_ratio
                    cost_paid = matched_paid * buy['price']

                    total_realized_pnl += actual_proceeds - cost_paid

                    remaining_to_match -= matched_amount
                    remaining_buy = buy['amount'] - matched_amount
                    if remaining_buy > 0:
                        remaining_paid = buy['amount_paid_for'] * (remaining_buy / buy['amount'])
                        buy_queue.appendleft({
                            'amount': remaining_buy,
                            'amount_paid_for': remaining_paid,
                            'price': buy['price']
                        })

        remaining_tokens = sum(b['amount'] for b in buy_queue)

        # Get latest price for the token from combined_df
        latest_token_row = combined_df[combined_df['token_name'] == token].sort_values(by='timestamp', ascending=False).head(1)
        if not latest_token_row.empty:
            latest_price = float(latest_token_row['genesis_usdc_price'].values[0])
        else:
            latest_price = 0.0
        unrealized_pnl = remaining_tokens * latest_price

        results.append({
            'Sniper Wallet Address': maker,
            'Token': token,
            'Net PnL': round(total_realized_pnl, 6),
            'Unrealized PnL': round(unrealized_pnl, 6),
            'Remaining Tokens': round(remaining_tokens, 6),
            'Buy Txn Count': int(buy_txn_count),
            'Sell Txn Count': int(sell_txn_count),
            'First Buy Time': first_buy_time,
            'Last Sell Time': last_sell_time,
            'Average Buy Price USD': round(avg_buy_price, 6),
            'Average Sell Price USD': round(avg_sell_price, 6),
            'Total Tax Paid': round(total_tax_paid, 6),
            'Total Transaction Fee Paid': round(total_transaction_fee_paid, 6)
        })

    return pd.DataFrame(results)

# Load data with caching
with st.spinner("Loading data..."):
    combined_df = load_swap_data()
    if combined_df is None:
        st.error("No data found from MongoDB collections.")
        st.stop()
    
    token_launch_blocks = load_launch_blocks()
    potential_sniper_df, combined_df = process_sniper_data(combined_df, token_launch_blocks)
    pnl_df = calculate_pnl(potential_sniper_df, combined_df)

def render_sidebar():
    with st.sidebar:
        st.markdown("---")
        current_script = os.path.basename(__file__).lower()

        routes = [
            ("🏠 Home", "/", "cards2.py"),
            ("🌍 Global Sniper Analysis", "/🌍 Global Sniper Analysis", "global_snipers.py")
        ]

        for label, href, script_name in routes:
            is_active = script_name.lower() == current_script
            bg_color = "rgba(227,250,255,0.2)" if is_active else "rgba(42,42,42,0.4)"
            if is_active:
                st.markdown(
                    f"""
                    <div style="
                        display: block;
                        padding: 0.5rem 1rem;
                        margin-bottom: 0.5rem;
                        font-weight: 500;
                        border-radius: 6px;
                        color: white;
                        background-color: {bg_color};
                    ">{label} ✅</div>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f"""
                    <a href="{href}" target="_self" style="
                        display: block;
                        padding: 0.5rem 1rem;
                        margin-bottom: 0.5rem;
                        font-weight: 500;
                        border-radius: 6px;
                        color: white;
                        background-color: {bg_color};
                        text-decoration: none;
                    ">{label}</a>
                    """,
                    unsafe_allow_html=True
                )
        st.markdown("---")

render_sidebar()

# Streamlit UI
# Page Title
st.markdown("<h1 style='color: white;'>Potential Snipers – PnL Overview</h1>", unsafe_allow_html=True)

# Filter Popover Top-Right
header_left, header_right = st.columns([6, 1])
with header_right:
    from datetime import date
    with st.popover("FILTER SNIPERS", icon="🔽", use_container_width=True):
        s1, s2, s3 = st.columns(3)
        with s1:
            token_filter = st.multiselect(
                "Select Token(s):",
                options=sorted([t for t in pnl_df["Token"].unique() if t is not None]),
                default=[]
            )
        with s2:
            wallet_search = st.text_input("Enter Wallet Address:", value="")
        with s3:
            min_date = pd.to_datetime(pnl_df["First Buy Time"]).min()
            max_date = pd.to_datetime(pnl_df["First Buy Time"]).max()
            date_range = st.date_input(
                "Select First Buy Date Range:",
                value=(min_date.date(), max_date.date()),
                min_value=min_date.date(),
                max_value=max_date.date()
            )

        s4, s5 = st.columns([1,5])
        with s4:
            st.write("RANGE : ")
        with s5:
            min_pnl = float(pnl_df["Net PnL"].min())
            max_pnl = float(pnl_df["Net PnL"].max())
            pnl_range = st.slider(
                "",
                min_value=min_pnl,
                max_value=max_pnl,
                value=(min_pnl, max_pnl),
                step=0.01
            )
filtered_df = pnl_df.copy()
filtered_df = filtered_df.reset_index(drop=True)
filtered_df.insert(0, 'S.No', range(1, len(filtered_df) + 1))

# Filter for tokens
if token_filter:
    filtered_df = filtered_df[filtered_df["Token"].isin(token_filter)]

# Filter for wallet address (case-insensitive partial search)
if wallet_search:
    filtered_df = filtered_df[filtered_df["Sniper Wallet Address"].str.contains(wallet_search, case=False, na=False)]

# Filter for date range (First Buy Time) with safe check
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    filtered_df = filtered_df[(pd.to_datetime(filtered_df["First Buy Time"]).dt.date >= start_date.date()) & (pd.to_datetime(filtered_df["First Buy Time"]).dt.date <= end_date.date())]

# Filter for Net PnL range
filtered_df = filtered_df[(filtered_df["Net PnL"] >= pnl_range[0]) & (filtered_df["Net PnL"] <= pnl_range[1])]

# Show warning if sniper address filter yields zero rows
if wallet_search and filtered_df.empty:
    st.warning("No snipers found matching the entered address.")

st.subheader("📊 Sniper Summary Table")

# Sort by net_pnl descending
filtered_df = filtered_df.sort_values(by="Net PnL", ascending=False).reset_index(drop=True)
filtered_df['S.No'] = range(1, len(filtered_df) + 1)

# Autosize all columns
column_config = {col: {"width": "auto"} for col in filtered_df.columns}
st.markdown("<div class='scrollable'>", unsafe_allow_html=True)
st.dataframe(filtered_df, hide_index=True, column_config=column_config)
st.markdown("</div>", unsafe_allow_html=True)

# Add gap between table and KPIs
st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

# Add gap between table and token-filtered analysis
st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)


if token_filter:
    st.subheader("🎯 Token-wise Sniper Summary")

    col1, col2, col3 = st.columns(3)

    with col1:
        token_summary = (
            filtered_df.groupby('Token')
            .agg({
                'Sniper Wallet Address': pd.Series.nunique,
                'Net PnL': 'sum',
                'Unrealized PnL': 'sum'
            })
            .reset_index()
            .rename(columns={
                'Sniper Wallet Address': 'Unique Snipers',
                'Net PnL': 'Total Realized PnL',
                'Unrealized PnL': 'Total Unrealized PnL'
            })
            .sort_values('Total Realized PnL', ascending=False)
        )
        st.dataframe(token_summary, use_container_width=True, hide_index=True)

        st.markdown("##### Token Sniper Count (Filtered)")
        token_counts = (
            filtered_df.groupby('Token')['Sniper Wallet Address']
            .nunique().reset_index()
        )
        chart_counts = alt.Chart(token_counts).mark_bar().encode(
            x=alt.X('Token:N', sort='-y'),
            y=alt.Y('Sniper Wallet Address:Q', title='Unique Snipers'),
            tooltip=['Token', 'Sniper Wallet Address']
        ).properties(height=250)
        st.altair_chart(chart_counts, use_container_width=True)

    with col2:
        st.markdown("##### Top 10 Snipers by Net PnL (Filtered)")
        top10_snipers = (
            filtered_df.groupby('Sniper Wallet Address')['Net PnL']
            .sum().nlargest(10).reset_index()
        )
        chart_top10 = alt.Chart(top10_snipers).mark_bar().encode(
            x=alt.X('Sniper Wallet Address:N', sort='-y'),
            y=alt.Y('Net PnL:Q'),
            tooltip=['Sniper Wallet Address', 'Net PnL']
        ).properties(height=350)
        st.altair_chart(chart_top10, use_container_width=True)

    with col3:
        st.markdown("##### Sniper PnL Distribution (Filtered)")
        chart_pnl_dist = alt.Chart(filtered_df).mark_bar().encode(
            x=alt.X('Net PnL:Q', bin=alt.Bin(maxbins=30)),
            y=alt.Y('count():Q'),
            tooltip=['count()']
        ).properties(height=350)
        st.altair_chart(chart_pnl_dist, use_container_width=True)

st.subheader("📊 Sniper Metrics")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
        <div class='glass-kpi'>
            <div class='kpi-inner'>
                <h4>Total Unique Snipers</h4>
                <p>{pnl_df['Sniper Wallet Address'].nunique()}</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
        <div class='glass-kpi'>
            <div class='kpi-inner'>
                <h4>Total Realized PnL</h4>
                <p>${pnl_df['Net PnL'].sum():,.2f}</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
        <div class='glass-kpi'>
            <div class='kpi-inner'>
                <h4>Total Unrealized PnL</h4>
                <p>${pnl_df['Unrealized PnL'].sum():,.2f}</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

# Add gap
st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

# Graphs
st.subheader("📈 Sniper & Token Activity Overview")
graph1, graph2 = st.columns(2)

# Top 10 Snipers by Net PnL (filtered)
with graph1:
    top10_snipers = pnl_df.groupby('Sniper Wallet Address')['Net PnL'].sum().nlargest(10).reset_index()
    chart = alt.Chart(top10_snipers).mark_bar().encode(
        x=alt.X('Net PnL:Q', title='Net PnL'),
        y=alt.Y('Sniper Wallet Address:N', sort='-x', title='Sniper Wallet Address'),
        tooltip=['Sniper Wallet Address', 'Net PnL']
    ).properties(title='Top 10 Snipers by Net PnL', height=350)
    st.altair_chart(chart, use_container_width=True)

# Token Sniper Activity — only show tokens in filtered_df
with graph2:
    token_sniper_counts = pnl_df.groupby('Token')['Sniper Wallet Address'].nunique().reset_index()
    chart2 = alt.Chart(token_sniper_counts).mark_bar().encode(
        x=alt.X('Sniper Wallet Address:Q', title='Unique Snipers'),
        y=alt.Y('Token:N', sort='-x', title='Token'),
        tooltip=['Token', 'Sniper Wallet Address']
    ).properties(title='Token Sniper Activity', height=350)
    st.altair_chart(chart2, use_container_width=True)

# Sniper Profit Distribution (filtered)
st.subheader("📊 Sniper Profit Distribution")
hist = alt.Chart(pnl_df).mark_bar().encode(
    x=alt.X('Net PnL:Q', bin=alt.Bin(maxbins=30), title='Net PnL'),
    y=alt.Y('count()', title='Number of Snipers'),
    tooltip=['count()']
).properties(title='Sniper Profit Distribution', height=300)
st.altair_chart(hist, use_container_width=True)