import os
from dotenv import load_dotenv
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from datetime import timedelta, datetime, timezone, time
from random import randint
import altair as alt
from collections import defaultdict, deque

# ───── Streamlit Setup ─────
st.set_page_config(layout="wide", page_title="Sniper Analysis by Lampros")

# ───── Global Styling ─────
st.markdown("""
    <style>
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
        background: #334a63;
    }
    </style>
""", unsafe_allow_html=True)
def render_sidebar():
    with st.sidebar:
        st.markdown("---")

        # Define clean route paths (not filenames)
        routes = [
            ("🏠 Home", "/"),
            ("🪙 Token Details", "/tokendatatestcopy.py"),
            ("🌍 Global Sniper Analysis", "/global_snipers")
        ]

        # Detect current path
        current_script = os.path.basename(__file__).lower()
        # Map script filenames to clean paths
        script_to_path = {
            "cards2.py": "/",
            "tokendatatestcopy.py": "/tokendatatestcopy.py",
            "global_snipers.py": "/global_snipers"
        }
        current_path = script_to_path.get(current_script, "/")

        for label, path in routes:
            is_active = path == current_path
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
                    <a href="{path}" target="_self" style="
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

# ───── Load DB Connection ─────
load_dotenv()
dbconn = os.getenv("MongoLink")
client = MongoClient(dbconn)
db = client['genesis_tokens_swap_info']


# ───── Token Parameter ─────
query_params = st.query_params
token = query_params.get('token', '').lower().strip()
# Token list (use DB here if needed)
available_tokens = [
    "jarvis", "tian", "badai", "aispace", "wint"
]

# If no token in URL, show fallback UI
if not token:
    st.warning("⚠️ No token selected. Please return to the home page and choose a token.")
    selected = st.selectbox("Choose a token to view its data:", sorted(available_tokens))
    
    if st.button("View Token Details") and selected:
        st.switch_page(f"/tokendatatestcopy.py?token={selected.lower()}")
    
    st.stop()
colh, cold, colmpty = st.columns([3, 4, 5])
with colh:
    st.markdown(f"<h1 style='margin-top: 0rem; color: white;'>TOKEN {token.upper()}</h1>", unsafe_allow_html=True)

with cold:
    doc = db["swap_progress"].find_one({"token_symbol": token.upper()})
    if doc:
        token_addr = doc.get("token_address", "N/A")
        lp_addr = doc.get("lp", "N/A")
        genesis_block = doc.get("genesis_block", "N/A")

        # We cannot get name, dao, or timestamp from swap_progress — we'll extract from first swap doc
        #token_collection = f"{token.lower()}_swap"
        token_collection = ['jarvis_swap', 'tian_swap', 'badai_swap', 'aispace_swap', 'wint_swap']
        swap_doc = None
        for col_name in token_collection:
            result = db[col_name].find_one(
                {"genesis_token_symbol": token.upper()},
                sort=[("timestamp", 1)]
            )
            if result:
                swap_doc = result
                break

        name = swap_doc.get("persona_name", "N/A") if swap_doc else "N/A"
        dao_addr = swap_doc.get("persona_dao", "N/A") if swap_doc else "N/A"
        timestamp = swap_doc.get("timestamp", 0) if swap_doc else 0
        launch_time = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%d-%m-%Y %H:%M') if timestamp else "N/A"
        collection_name = f"{token.lower()}_swap"
        swap_count = db[collection_name].count_documents({})
        details_card = f"""
        <div style="
            background-color: rgba(255, 255, 255, 0.1);
            border-radius: 0.5rem;
            padding: 1rem;
            margin-top: 0.5rem;
            font-size: 0.9rem;
            font-family: monospace;
            color: white;
            display: grid;
            grid-template-columns: 120px auto;
            row-gap: 0.5rem;
        ">
            <div>Token Address:</div> <div style="color: #4ef577;">{token_addr}</div>
            <div>LP Address:</div> <div style="color: #4ef577;">{lp_addr}</div>
            <div>DAO Address:</div> <div style="color: #4ef577;">{dao_addr}</div>
            <div>Total Swaps:</div> <div style="color: #4ef577;">{swap_count}</div>
        </div>
        """

        st.markdown(details_card, unsafe_allow_html=True)

with colmpty:
    st.write("")

# ───── Collection Naming ─────
token_in_col = f"{token.upper()}_IN"
token_out_col = f"{token.upper()}_OUT"
virtual_in_col = "Virtual_IN"
virtual_out_col = "Virtual_OUT"
collection_name = f"{token}_swap"

# ───── Fetch Data ─────
data = list(db[collection_name].find({}, {
    "blockNumber": 1, "txHash": 1, "maker": 1, "swapType": 1, "label": 1, "timestampReadable": 1,
    token_in_col: 1, token_out_col: 1, virtual_in_col: 1, virtual_out_col: 1,
    "genesis_usdc_price": 1, "genesis_virtual_price": 1, "virtual_usdc_price": 1, "Tax_1pct":1, "transactionFee":1
}))
#st.write("Fetched rows:", len(data))
#st.write("Sample doc:", data[0] if data else "No data")

tabdf = pd.DataFrame(data).fillna(0)

# ───── Process Data ─────
def extract_amount(row):
    if row.get("swapType") == "buy":
        return pd.Series([row.get(token_out_col), row.get(virtual_in_col)])
    elif row.get("swapType") == "sell":
        return pd.Series([row.get(token_in_col), row.get(virtual_out_col)])
    return pd.Series([None, None])

# Step 1: Extract TokenAmount and Virtual, round to 4 decimals
tabdf[["TokenAmount", "Virtual"]] = tabdf.apply(extract_amount, axis=1)
tabdf["TokenAmount"] = pd.to_numeric(tabdf["TokenAmount"], errors="coerce").round(4)
tabdf["Virtual"] = pd.to_numeric(tabdf["Virtual"], errors="coerce").round(4)
tabdf.rename(columns={"TokenAmount": token.upper()}, inplace=True)

# Step 2: Select and rename required columns
tabdf = tabdf[[ 
    "blockNumber", "txHash", "maker", "swapType", "label", "timestampReadable", token.upper(), "Virtual", "genesis_usdc_price", "genesis_virtual_price", "virtual_usdc_price", "Tax_1pct", "transactionFee"
]].rename(columns={
    "blockNumber": "BLOCK",
    "txHash": "TX HASH",
    "maker": "MAKER",
    "swapType": "TX TYPE",
    "label": "SWAP TYPE",
    "timestampReadable": "TIME",
    "Virtual": "VIRTUAL",
    "genesis_usdc_price": "GENESIS \nPRICE ($)",
    "genesis_virtual_price": "GENESIS PRICE \n($VIRTUAL)",
    "virtual_usdc_price": "VIRTUAL \nPRICE ($)",
    "Tax_1pct": "TAX (ETH)",
    "transactionFee": f"TX FEE ({token.upper()})"
})

# Step 3: Transaction Value Calculation
tabdf["TRANSACTION VALUE ($)"] = (
    pd.to_numeric(tabdf[token.upper()], errors="coerce") *
    pd.to_numeric(tabdf["GENESIS \nPRICE ($)"], errors="coerce")
).round(4)

# Step 4: Parse TIME as datetime
tabdf["TIME"] = pd.to_datetime(tabdf["TIME"], errors="coerce")

# Step 5: Minimal display formatting — no HTML tags
tabdf["TX HASH"] = tabdf["TX HASH"].astype(str).str[:8] + "..." + tabdf["TX HASH"].astype(str).str[-4:]
tabdf["MAKER"] = tabdf["MAKER"].apply(lambda addr: f"{addr[:5]}...{addr[-5:]}" if isinstance(addr, str) else addr)
tabdf["TX TYPE"] = tabdf["TX TYPE"].astype(str)

# Step 6: Sortable Columns
sortable_columns = ["BLOCK", "TIME", token.upper(), "VIRTUAL", "GENESIS \nPRICE ($)", "TRANSACTION VALUE ($)", "GENESIS PRICE \n($VIRTUAL)", "VIRTUAL \nPRICE ($)", "TAX (ETH)", f"TX FEE ({token.upper()})"]

# Final filtered_df copy
filtered_df = tabdf.copy()

tab1, tab2, tab3 = st.tabs(["TRANSCTIONS", "SNIPER INSIGHTS", "OTHER"])

with tab1:
    # ───── Filters: Panel 1 ─────
    with st.container():
        col1, col2, col3, col4, col5, col10 = st.columns(6)
    
        with col1:
            st.markdown("<div style='color: white; font-weight: 500;'>Transaction Type</div>", unsafe_allow_html=True)
            swap_filter = st.segmented_control("", options=["all", "buy", "sell"], default="all")
    
        with col2:
            st.markdown("<div style='color: white; font-weight: 500;'>Swap Type</div>", unsafe_allow_html=True)
            label_options = ["All"] + sorted(tabdf["SWAP TYPE"].dropna().unique())
            label_filter = st.selectbox("", label_options)
    
        with col3:
            st.markdown("<div style='color: white; font-weight: 500;'>Date Range</div>", unsafe_allow_html=True)
            date_range = st.date_input(
                "",
                value=(tabdf["TIME"].min(), tabdf["TIME"].max())
            )

    
        with col4:
            st.markdown("<div style='color: white; font-weight: 500;'>Sort by</div>", unsafe_allow_html=True)
            sort_col = st.selectbox("", sortable_columns)
    
        with col5:
            st.markdown("<div style='color: white; font-weight: 500;'>Order</div>", unsafe_allow_html=True)
            sort_dir = st.radio("", options=["Ascending", "Descending"], horizontal=True)
    
        with col10:
            st.markdown("<div style='color: white; font-weight: 500;'>Search</div>", unsafe_allow_html=True)
            search_query = st.text_input("", placeholder="BLOCK | MAKER | TX HASH")
            if search_query:
                q = search_query.strip().lower()
                filtered_df = filtered_df[
                    filtered_df["BLOCK"].astype(str).str.contains(q) |
                    filtered_df["MAKER"].str.lower().str.contains(q) |
                    filtered_df["TX HASH"].str.lower().str.contains(q)
                ]
    
    # ───── Apply Filters ─────
    mask = pd.Series(True, index=tabdf.index)

    if swap_filter != "all":
        mask &= tabdf["TX TYPE"].str.lower() == swap_filter.lower()

    if label_filter != "All":
        mask &= tabdf["SWAP TYPE"] == label_filter

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]) + timedelta(days=1)
        mask &= (tabdf["TIME"] >= start_date) & (tabdf["TIME"] <= end_date)

    filtered_df = tabdf.loc[mask].copy()

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date = pd.to_datetime(date_range[0])
        end_date = pd.to_datetime(date_range[1]) + timedelta(days=1)
        filtered_df["TIME"] = pd.to_datetime(filtered_df["TIME"], errors='coerce')
        filtered_df = filtered_df[
            (filtered_df["TIME"] >= start_date) & (filtered_df["TIME"] <= end_date)
        ]

    
    # ───── Filters: Panel 2 (Numeric Range) ─────
    with st.container():
        col6, col7 = st.columns([1, 4])
        with col6:
            st.markdown("<div style='color: white; font-weight: 500;'>Select Metric to Filter by range :</div>", unsafe_allow_html=True)
            numeric_columns = [token.upper(), "VIRTUAL", "GENESIS \nPRICE ($)", "TRANSACTION VALUE ($)", "GENESIS PRICE \n($VIRTUAL)", "VIRTUAL \nPRICE ($)", "TAX (ETH)", f"TX FEE ({token.upper()})"]
            selected_col = st.selectbox("", numeric_columns)
    
        with col7:
            if selected_col in filtered_df.columns and not filtered_df[selected_col].dropna().empty:
                col_min, col_max = filtered_df[selected_col].min(), filtered_df[selected_col].max()
                if pd.notnull(col_min) and pd.notnull(col_max) and col_min != col_max:
                    st.markdown(f"<div style='color: white; font-weight: 500;'>Range for {selected_col}</div>", unsafe_allow_html=True)
                    value_range = st.slider(
                        "", float(col_min), float(col_max), (float(col_min), float(col_max)),
                        step=0.000001, format="%.6f"
                    )
                    filtered_df = filtered_df[
                        (filtered_df[selected_col] >= value_range[0]) &
                        (filtered_df[selected_col] <= value_range[1])
                    ]
    
    #--TABLE RENDERING
    filtered_df = filtered_df.sort_values(by=sort_col, ascending=(sort_dir == "Ascending"))
    #ordering columns
    ordered_cols = [
        "BLOCK", "TX HASH", "MAKER", "TX TYPE", "SWAP TYPE", "TIME",
        token.upper(), "VIRTUAL", "GENESIS \nPRICE ($)", "TRANSACTION VALUE ($)",
        "GENESIS PRICE \n($VIRTUAL)", "VIRTUAL \nPRICE ($)", "TAX (ETH)", f"TX FEE ({token.upper()})"
    ]
    filtered_df = filtered_df[[col for col in ordered_cols if col in filtered_df.columns]]
    
    tabdf["date"] = tabdf["TIME"].dt.date
    volume_df = tabdf.groupby("date")[token.upper()].sum().reset_index()
    # --- KPI METRICS ---
    with st.container():
        
        col1, col2, col3= st.columns([1,2,2])

        with col1:
            
            if "MAKER_CLEAN" not in tabdf.columns:
                import re
                tabdf["MAKER_CLEAN"] = tabdf["MAKER"].apply(lambda addr: re.sub(r'<.*?>', '', addr) if isinstance(addr, str) else addr)
            unique_makers = tabdf["MAKER_CLEAN"].nunique()
            
            sell_volume_usd = (
                tabdf[tabdf["TX TYPE"] == "sell"][token.upper()] *
                tabdf[tabdf["TX TYPE"] == "sell"]["GENESIS \nPRICE ($)"]
            ).sum()
            
            buy_volume_usd = (
                tabdf[tabdf["TX TYPE"] == "buy"][token.upper()] *
                tabdf[tabdf["TX TYPE"] == "buy"]["GENESIS \nPRICE ($)"]
            ).sum()
            
        tabdf["MAKER_CLEAN"] = tabdf["MAKER"].str.replace(r"<.*?>", "", regex=True)
        # BUYERS: Group by address, compute total USD bought
        buyers = (
            tabdf[tabdf["TX TYPE"] == "buy"]
            .groupby("MAKER_CLEAN")
            .apply(lambda df: (df[token.upper()] * df["GENESIS \nPRICE ($)"]).sum())
            .nlargest(10)
            .reset_index(name="buy_volume_usd")
        )

        # SELLERS: Group by address, compute total USD sold
        sellers = (
            tabdf[tabdf["TX TYPE"] == "sell"]
            .groupby("MAKER_CLEAN")
            .apply(lambda df: (df[token.upper()] * df["GENESIS \nPRICE ($)"]).sum())
            .nlargest(10)
            .reset_index(name="sell_volume_usd")
        )

        # Shorten address for readability
        buyers["MAKER_SHORT"] = buyers["MAKER_CLEAN"].apply(lambda a: a[:6] + "..." + a[-4:])
        sellers["MAKER_SHORT"] = sellers["MAKER_CLEAN"].apply(lambda a: a[:6] + "..." + a[-4:])

        with col2:
            
            chart_buyers = alt.Chart(buyers).mark_bar(color="#4fb0ff").encode(
                x=alt.X("buy_volume_usd:Q", title="Buy Volume (USD)"),
                y=alt.Y("MAKER_SHORT:N", sort="-x", title="Wallet"),
                tooltip=[
                    alt.Tooltip("MAKER_CLEAN", title="MAKER"),
                    alt.Tooltip("buy_volume_usd", title="BUY VOLUME ($)")
                ]
            ).properties(title="Top 10 Buyers", height=350)
        with col3:
            chart_sellers = alt.Chart(sellers).mark_bar(color="#2c8bda").encode(
                x=alt.X("sell_volume_usd:Q", title="Sell Volume (USD)"),
                y=alt.Y("MAKER_SHORT:N", sort="-x", title="Wallet"),
                tooltip=[
                    alt.Tooltip("MAKER_CLEAN", title="MAKER"),
                    alt.Tooltip("sell_volume_usd", title="SELL VOLUME ($)")
                ]
            ).properties(title="Top 10 Sellers", height=350)
            

    
    eq_height = 360

    # --- ALT 1: Swap Volume Over Time (Altair Line Chart) ---
    chart = alt.Chart(volume_df).mark_line().encode(
        x=alt.X('date:T', title='DATE'),
        y=alt.Y(f'{token.upper()}:Q', title="SWAP VOLUME"),
        tooltip=['date:T', f'{token.upper()}:Q']
    ).properties(title=" ", width=700, height=eq_height)


    # --- Prepare Buyers & Sellers Data (clean + group) ---
    def clean_address(addr):
        import re
        if isinstance(addr, str):
            addr = re.sub(r'<.*?>', '', addr)  # Strip HTML tags if present
            return addr
        return addr

    # --- Render Everything ---
    with st.container():
        styled_df = filtered_df.style.applymap(
            lambda x: 'color: #74fe64; font-weight: bold;' if x == 'buy' else ('color: red; font-weight: bold;' if x == 'sell' else ''),
            subset=['TX TYPE']
        )

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        st.title("")
        col1, col2, col3 = st.columns([1,2,2])
        with col1:
            st.subheader("TOKEN KPIs")
            st.markdown(f"<div class='glass-kpi'><h4>UNIQUE TRADERS</h4><p>{unique_makers}</p></div>", unsafe_allow_html=True)
            st.write("")
            st.markdown(f"<div class='glass-kpi'><h4>SELL VOLUME ($)</h4><p>${sell_volume_usd:,.2f}</p></div>", unsafe_allow_html=True)
            st.write("")
            st.markdown(f"<div class='glass-kpi'><h4>BUY VOLUME ($)</h4><p>${buy_volume_usd:,.2f}</p></div>", unsafe_allow_html=True)
        with col2:
            st.subheader("TOP 10 BUYERS")
            st.altair_chart(chart_buyers, use_container_width=True)
        with col3:
            st.subheader("TOP 10 SELLERS")
            st.altair_chart(chart_sellers, use_container_width=True)
        st.subheader("SWAP VOLUME OVER TIME")
        st.altair_chart(chart, use_container_width=True)
#-----TAB2 : SNIPER INSIGHTS WITH PNL-----
with tab2:
    # ───── Token from Query Params ─────
    token_upper = token.upper()
    # ───── Load Swap Data for Token ─────
    @st.cache_data(ttl=300)
    def load_swap_data(token):
        col_name = f"{token}_swap"
        data = list(db[col_name].find())
        if not data:
            return None
        df = pd.DataFrame(data)
        df.drop(columns=["_id"], errors="ignore", inplace=True)
        df["token_name"] = token.upper()
        df["timestampReadable"] = pd.to_datetime(df["timestampReadable"], errors='coerce')
        return df
    # ───── Launch Block (fallback logic) ─────
    @st.cache_data(ttl=600)
    def load_launch_blocks():
        client = MongoClient(dbconn)
        db = client["genesis_tokens_swap_info"]
        try:
            persona_data = list(db["swap_progress"].find({}, {"token_symbol": 1, "genesis_block": 1}))
            df = pd.DataFrame(persona_data)
            if not df.empty and "token_symbol" in df and "genesis_block" in df:
                return dict(zip(df["token_symbol"], df["genesis_block"]))
        except Exception as e:
            print("Error loading launch blocks:", e)
        return {}
    # ───── Sniper Detection Logic ─────
    @st.cache_data(ttl=300)
    def process_sniper_data(combined_df, token_launch_blocks):
        buy_df = combined_df[combined_df["swapType"] == "buy"].copy()
        buy_df = buy_df.sort_values(by=["maker", "timestampReadable"])
        chunked_buys = []
        time_threshold = pd.Timedelta(minutes=10)

        for maker, group in buy_df.groupby("maker"):
            current_chunk = []
            chunk_start_time = None
            current_sum = 0
            for row in group.itertuples():
                if not current_chunk:
                    chunk_start_time = row.timestampReadable
                    current_chunk = [row]
                    token_prefix = row.token_name  # e.g., "AFATH"
                    field = f"{token_prefix}_OUT_BeforeTax"
                    current_sum = getattr(row, field, 0)  # fallback to 0 if field missing

                elif row.timestampReadable - chunk_start_time <= time_threshold:
                    current_chunk.append(row)
                    current_sum += getattr(row, field, 0)
                else:
                    if current_sum > 100000:
                        chunked_buys.extend(current_chunk)
                    chunk_start_time = row.timestampReadable
                    current_chunk = [row]
                    token_prefix = row.token_name
                    field = f"{token_prefix}_OUT_BeforeTax"
                    current_sum = getattr(row, field, 0)

            if current_sum > 100000:
                chunked_buys.extend(current_chunk)

        df_chunked_large_buys = pd.DataFrame(chunked_buys).drop_duplicates()
        if "transactionFee" in df_chunked_large_buys.columns:
            df_high_gas = df_chunked_large_buys[df_chunked_large_buys["transactionFee"] > 0.000002]
        else:
            df_high_gas = df_chunked_large_buys.copy()
            st.warning("⚠️ 'transactionFee' missing in dataset — skipping gas filter.")


        def is_sniper_buy(row):
            launch_block = token_launch_blocks.get(row["token_name"])
            return launch_block is not None and row["blockNumber"] <= launch_block + 100

        df_sniper_buys = df_high_gas[df_high_gas.apply(is_sniper_buy, axis=1)]

        sells = combined_df[combined_df["swapType"] == "sell"][["maker", "timestampReadable", "token_name"]]
        merged = pd.merge(
            df_sniper_buys[["maker", "timestampReadable", "token_name"]],
            sells,
            on=["maker", "token_name"],
            suffixes=("_buy", "_sell"),
        )
        merged["time_diff"] = (merged["timestampReadable_sell"] - merged["timestampReadable_buy"]).dt.total_seconds()
        quick_sells = merged[merged["time_diff"].between(0, 20 * 60)]
        potential_sniper_df = df_sniper_buys[df_sniper_buys["maker"].isin(quick_sells["maker"])].copy()
        return potential_sniper_df, combined_df
    # ───── PnL Calculation ─────
    @st.cache_data(ttl=300)
    def calculate_pnl(potential_sniper_df, combined_df):
        results = []
        sniper_pairs = potential_sniper_df[["maker", "token_name"]].drop_duplicates()
        for _, row in sniper_pairs.iterrows():
            maker = row["maker"]
            token = row["token_name"]
            df = combined_df[(combined_df["maker"] == maker) & (combined_df["token_name"] == token)]
            df = df.sort_values(by="timestampReadable")
            trades = defaultdict(list)
            # Dynamic field names based on token
            out_before_tax_col = f"{token}_OUT_BeforeTax"
            out_after_tax_col = f"{token}_OUT_AfterTax"
            in_before_tax_col = f"{token}_IN_BeforeTax"
            in_after_tax_col = f"{token}_IN_AfterTax"

            buy_txn_count = (df["swapType"] == "buy").sum()
            sell_txn_count = (df["swapType"] == "sell").sum()
            first_buy_time = df.loc[df["swapType"] == "buy", "timestampReadable"].min()
            last_sell_time = df.loc[df["swapType"] == "sell", "timestampReadable"].max()
            avg_buy_price = df.loc[df["swapType"] == "buy", "genesis_usdc_price"].mean()
            avg_sell_price = df.loc[df["swapType"] == "sell", "genesis_usdc_price"].mean()
            total_tax_paid = df["Tax_1pct"].sum()
            total_tx_fees = df["transactionFee"].sum()

            for _, tx in df.iterrows():
                token_prefix = tx["token_name"]
                if tx["swapType"] == "buy":
                    out_before_tax = tx.get(f"{token_prefix}_OUT_BeforeTax", 0.0)
                    out_after_tax = tx.get(f"{token_prefix}_OUT_AfterTax", 0.0)
                    trades[maker].append({
                        "type": "buy",
                        "amount": out_after_tax,
                        "cost": out_before_tax,
                        "price": tx["genesis_usdc_price"]
                    })
                elif tx["swapType"] == "sell":
                    in_before_tax = tx.get(f"{token}_IN_BeforeTax", 0.0)
                    in_after_tax = tx.get(f"{token}_IN_AfterTax", 0.0)
                    trades[maker].append({
                        "type": "sell",
                        "amount": in_after_tax,
                        "from_wallet": in_before_tax,
                        "price": tx["genesis_usdc_price"]
                    })
            buy_queue = deque()
            realized = 0.0
            for tx in trades[maker]:
                if tx["type"] == "buy":
                    amount_bought = tx["amount"]
                    cost_in_usd = amount_bought * tx["price"]  # tokens * price per token
                    buy_queue.append({
                        "amount": amount_bought,
                        "cost": cost_in_usd,
                        "price": tx["price"]
                    })
                elif tx["type"] == "sell":
                    to_match = tx["from_wallet"]  # tokens to sell
                    while to_match > 0 and buy_queue:
                        buy = buy_queue.popleft()
                        match_amt = min(to_match, buy["amount"])
                        matched_cost = buy["cost"] * (match_amt / buy["amount"])
                        proceeds = match_amt * tx["price"]
                        realized += proceeds - matched_cost
                        to_match -= match_amt
                        leftover = buy["amount"] - match_amt
                        if leftover > 0:
                            leftover_cost = buy["cost"] * (leftover / buy["amount"])
                            buy_queue.appendleft({
                                "amount": leftover,
                                "cost": leftover_cost,
                                "price": buy["price"]
                            })
            # summing leftover tokens for unrealized
            remaining = sum(b["amount"] for b in buy_queue)
            latest_price = combined_df[combined_df["token_name"] == token].sort_values(by="timestampReadable", ascending=False).head(1)["genesis_usdc_price"].values[0]
            unrealized = remaining * latest_price

            results.append({
                "Wallet Address": maker,
                "Net PnL ($)": round(realized, 4),
                "Unrealized PnL ($)": round(unrealized, 4),
                "Remaining Tokens": float(f"{remaining:.4f}"),
                "BUY COUNT": buy_txn_count,
                "SELL COUNT": sell_txn_count,
                "First Buy Time": first_buy_time,
                "Last Sell Time": last_sell_time,
                "Average Buy Price ($)": round(avg_buy_price, 4),
                "Average Sell Price ($)": round(avg_sell_price, 4),
                "Total Tax Paid": round(total_tax_paid, 4),
                "TXN FEES (ETH)": round(total_tx_fees, 4)
            })
        print("Returning results with rows:", len(results))
        print("Result keys:", results[0].keys() if results else "No results")
        return pd.DataFrame(results)
    # ───── Load and Process ─────
    with st.spinner("Loading data..."):
        combined_df = load_swap_data(token)
        if combined_df is None:
            st.error("No data found for this token.")
            st.stop()
        token_launch_blocks = load_launch_blocks()
        potential_sniper_df, combined_df = process_sniper_data(combined_df, token_launch_blocks)
    pnl_df = calculate_pnl(potential_sniper_df, combined_df)
    if pnl_df.empty:
        st.markdown("### ❌ No Snipers Detected")
        st.stop()
    #-----------------------------------------------------------------------------------------------------------------------------
    # Streamlit UI
    st.title(f"Potential Snipers – PnL Overview for {token_upper}")
    st.subheader("📊 Sniper Summary Table")
    # Create filtered_df and add S.No once, cleanly
    filtered_df = pnl_df.copy().reset_index(drop=True)
    filtered_df["Wallet Short"] = filtered_df["Wallet Address"].apply(
        lambda addr: f"{addr[:5]}...{addr[-5:]}" if isinstance(addr, str) else addr
    )
    filtered_df["Net PnL ($)"] = filtered_df["Net PnL ($)"].round(4)
    # Now define ordered_cols safely
    ordered_cols = [
        "Wallet Short",
        "Net PnL ($)",
        "Unrealized PnL ($)",
        "Remaining Tokens",
        "BUY COUNT",
        "SELL COUNT",
        "First Buy Time",
        "Last Sell Time",
        "Average Buy Price ($)",
        "Average Sell Price ($)",
        "Total Tax Paid",
        "TXN FEES (ETH)"
    ]
    display_df = filtered_df[ordered_cols].rename(columns={"Wallet Short": "Wallet Address"})
    def highlight_net_pnl(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return 'color: #74fe64; font-weight: bold;'
            elif val < 0:
                return 'color: red; font-weight: bold;'
        return ''

    styled_df = filtered_df.style.applymap(highlight_net_pnl, subset=["Net PnL ($)"])
    st.dataframe(styled_df, use_container_width=True, hide_index=True)



    # KPIs
    filtered_df = pnl_df.copy().reset_index(drop=True)

    # Ensure numeric version is available for filtering
    filtered_df['Net PnL ($)'] = pd.to_numeric(filtered_df['Net PnL ($)'], errors='coerce')
    #sorting it in a descending order so that greatest net PNL is shown at top 
    pnl_df = pnl_df.sort_values(by="Net PnL ($)", ascending=False).reset_index(drop=True)

    successful_snipers = filtered_df[filtered_df['Net PnL ($)'] > 0]
    num_unique_snipers = filtered_df['Wallet Address'].nunique()
    success_rate = (len(successful_snipers) / num_unique_snipers * 100) if num_unique_snipers > 0 else 0



    total_realized_pnl = filtered_df['Net PnL ($)'].sum()
    total_unrealized_pnl = filtered_df['Unrealized PnL ($)'].sum()

    # Total tokens held by snipers
    total_tokens_held = filtered_df['Remaining Tokens'].sum()
    total_supply = 1_000_000_000  # adjust if needed
    tokens_held_percentage = (total_tokens_held / total_supply) * 100 if total_supply > 0 else 0
    with st.container():
        kpicol1, kpicol2 = st.columns([3,5])
        with kpicol1:      
            st.subheader('SNIPER KPIs')
            st.write("")
            kpi1, kpi2, kpi5= st.columns(3)
            with kpi1:
                st.markdown(f"""
                    <div class="glass-kpi">
                        <h4>Total Unique Snipers</h4>
                        <p>{num_unique_snipers}</p>
                    </div>
                """, unsafe_allow_html=True)

            with kpi2:
                st.markdown(f"""
                    <div class="glass-kpi">
                        <h4>Success Rate of Trades (%)</h4>
                        <p>{success_rate:.2f}%</p>
                    </div>
                """, unsafe_allow_html=True)

            with kpi5:
                st.markdown(f"""
                    <div class="glass-kpi">
                        <h4>      Total Tokens Held by Snipers (%)</h4>
                        <p>{tokens_held_percentage:.4f}%</p>
                    </div>
                """, unsafe_allow_html=True)

            st.write("")
            kpi3, kpi4 = st.columns(2)
            with kpi3:
                st.markdown(f"""
                    <div class="glass-kpi">
                        <h4>Total Realized PnL</h4>
                        <p>${total_realized_pnl:,.2f}</p>
                    </div>
                """, unsafe_allow_html=True)
            with kpi4:
                st.markdown(f"""
                    <div class="glass-kpi">
                        <h4>Total Unrealized PnL</h4>
                        <p>${total_unrealized_pnl:,.2f}</p>
                    </div>
                """, unsafe_allow_html=True)
        with kpicol2:
            # Top 5 Traders by Net PnL
            num_snipers = len(filtered_df)

            if num_snipers == 1:
                st.info("\nOnly one sniper detected")
            elif 2 <= num_snipers <= 5:
                st.subheader('Top Snipers by Total Net PnL')
                top = filtered_df.copy()
            elif num_snipers > 5:
                st.subheader('Top 5 Snipers by Total Net PnL')
                top = filtered_df.nlargest(5, 'Net PnL ($)')

            if num_snipers >= 2:
                st.markdown("""
                    <style>
                    .glass-chart {
                        padding: 1rem;
                        margin: 1rem 0;
                        background: rgba(255, 255, 255, 0.12);
                        border-radius: 12px;
                        backdrop-filter: blur(10px);
                        -webkit-backdrop-filter: blur(10px);
                        border: 1px solid rgba(255, 255, 255, 0.25);
                        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
                    }
                    </style>
                """, unsafe_allow_html=True)

                bar_chart = alt.Chart(top).mark_bar().encode(
                    y=alt.Y('Wallet Address:N', title='Wallet Address', sort=top['Wallet Address'].tolist()),
                    x=alt.X('Net PnL ($):Q', title='Net PnL ($)'),color=alt.value("#00aeff"),
                    tooltip=['Wallet Address', 'Net PnL ($)']
                ).properties(
                    width=600,
                    height=225
                )
                st.altair_chart(bar_chart, use_container_width=True)
    # --- Top 50 Traders by Net PnL ---
    # ───── PnL for All Participants ─────
    def calculate_pnl_all(df):
        results = []
        wallet_pairs = df[["maker", "token_name"]].drop_duplicates()
        for _, row in wallet_pairs.iterrows():
            maker = row["maker"]
            token = row["token_name"]
            user_df = df[(df["maker"] == maker) & (df["token_name"] == token)].sort_values(by="timestampReadable")
            trades = defaultdict(list)

            # Dynamic fields
            out_before_tax = f"{token}_OUT_BeforeTax"
            out_after_tax = f"{token}_OUT_AfterTax"
            in_before_tax = f"{token}_IN_BeforeTax"
            in_after_tax = f"{token}_IN_AfterTax"

            buy_txn_count = (user_df["swapType"] == "buy").sum()
            sell_txn_count = (user_df["swapType"] == "sell").sum()
            first_buy_time = user_df.loc[user_df["swapType"] == "buy", "timestampReadable"].min()
            last_sell_time = user_df.loc[user_df["swapType"] == "sell", "timestampReadable"].max()
            avg_buy_price = user_df.loc[user_df["swapType"] == "buy", "genesis_usdc_price"].mean()
            avg_sell_price = user_df.loc[user_df["swapType"] == "sell", "genesis_usdc_price"].mean()
            total_tax_paid = user_df["Tax_1pct"].sum()
            total_tx_fees = user_df["transactionFee"].sum()

            for _, tx in user_df.iterrows():
                prefix = tx["token_name"]
                if tx["swapType"] == "buy":
                    trades[maker].append({
                        "type": "buy",
                        "amount": tx.get(f"{prefix}_OUT_AfterTax", 0),
                        "cost": tx.get(f"{prefix}_OUT_BeforeTax", 0),
                        "price": tx["genesis_usdc_price"]
                    })
                elif tx["swapType"] == "sell":
                    trades[maker].append({
                        "type": "sell",
                        "amount": tx.get(f"{prefix}_IN_AfterTax", 0),
                        "from_wallet": tx.get(f"{prefix}_IN_BeforeTax", 0),
                        "price": tx["genesis_usdc_price"]
                    })

            buy_queue = deque()
            realized = 0.0
            for tx in trades[maker]:
                if tx["type"] == "buy":
                    buy_queue.append(tx)
                elif tx["type"] == "sell":
                    to_match = tx["from_wallet"]
                    proceeds = tx["amount"] * tx["price"]
                    while to_match > 0 and buy_queue:
                        buy = buy_queue.popleft()
                        match_amt = min(to_match, buy["amount"])
                        match_cost = buy["cost"] * (match_amt / buy["amount"])
                        realized += proceeds * (match_amt / tx["from_wallet"]) - match_cost * buy["price"]
                        to_match -= match_amt
                        leftover = buy["amount"] - match_amt
                        if leftover > 0:
                            buy_queue.appendleft({
                                "amount": leftover,
                                "cost": buy["cost"] * (leftover / buy["amount"]),
                                "price": buy["price"]
                            })

            remaining = sum(b["amount"] for b in buy_queue)
            latest_price = df[df["token_name"] == token].sort_values(by="timestampReadable", ascending=False).head(1)["genesis_usdc_price"].values[0]
            unrealized = remaining * latest_price

            results.append({
                "Wallet Address": maker,
                "Net PnL ($)": round(realized, 4),
                "Unrealized PnL ($)": round(unrealized, 4),
                "Remaining Tokens": float(f"{remaining:.4f}"),
                "Txn Count (BUY)": buy_txn_count,
                "Txn Count (SELL)": sell_txn_count,
                "First Buy Time": first_buy_time,
                "Last Sell Time": last_sell_time,
                "Average Buy Price ($)": round(avg_buy_price, 4),
                "Average Sell Price ($)": round(avg_sell_price, 4),
                "Total Tax Paid": round(total_tax_paid, 4),
                "Total Tx Fees Paid (ETH)": round(total_tx_fees, 4)
            })

        return pd.DataFrame(results)
    # --- Top 50 Traders by Net PnL (All Participants) ---
    st.subheader("📊 Top 50 Traders by Net PnL (All Participants)")

    # Calculate full PnL
    pnl_all_df = calculate_pnl_all(combined_df)
    pnl_all_df = pnl_all_df.sort_values(by="Net PnL ($)", ascending=False).reset_index(drop=True)
    pnl_all_df["Rank"] = pnl_all_df.index + 1

    # Add "Is Sniper" column
    sniper_wallets = set(potential_sniper_df["maker"].unique())
    pnl_all_df["Is Sniper"] = pnl_all_df["Wallet Address"].apply(
        lambda addr: "Yes" if addr in sniper_wallets else "No"
    )


    # Wallet shortening
    pnl_all_df["Wallet Display"] = pnl_all_df["Wallet Address"].apply(
        lambda addr: f"{addr[:5]}...{addr[-5:]}" if isinstance(addr, str) else addr
    )


    # Style Net PnL
    pnl_all_df["Net PnL ($)"] = pnl_all_df["Net PnL ($)"].round(4)

    # Total Buys and Sells in USD
    def get_total_usd(tx_df, maker, token, tx_type):
        subset = tx_df[
            (tx_df["maker"] == maker) & 
            (tx_df["token_name"] == token) & 
            (tx_df["swapType"] == tx_type)
        ]
        try:
            field = f"{token}_OUT_AfterTax" if tx_type == "buy" else f"{token}_IN_AfterTax"
            return (subset["genesis_usdc_price"] * subset[field]).sum()
        except KeyError:
            return 0.0

    pnl_all_df["Total Buys (USD)"] = pnl_all_df.apply(
        lambda row: get_total_usd(combined_df, row["Wallet Address"], token_upper, "buy"), axis=1
    )
    pnl_all_df["Total Sells (USD)"] = pnl_all_df.apply(
        lambda row: get_total_usd(combined_df, row["Wallet Address"], token_upper, "sell"), axis=1
    )


    # Number of Trades
    pnl_all_df["Number of Trades"] = pnl_all_df["Txn Count (BUY)"] + pnl_all_df["Txn Count (SELL)"]

    # Final column order
    display_cols = [
        "Rank", "Wallet Address", "Is Sniper", "Net PnL ($)", "Number of Trades",
        "Total Buys (USD)", "Total Sells (USD)"
    ]

    pnl_all_df = pnl_all_df.sort_values(by="Net PnL ($)", ascending=False).head(50)

    # Rename Wallet Display → Wallet Address (final table shows shortened version cleanly)
    display_df = pnl_all_df[display_cols].rename(columns={
        "Wallet Display": "Wallet Address"
    })

    def highlight_sniper(val):
        if isinstance(val, str) and "Yes" in val:
            return 'color: red; font-weight: bold;'
        elif isinstance(val, str) and "No" in val:
            return 'color: #74fe64; font-weight: bold;'
        return ''

    styled_df = display_df.style.applymap(highlight_sniper, subset=["Is Sniper"])
    st.dataframe(styled_df, use_container_width=True, hide_index=True)




with tab3:
        st.header("MORE INSIGHTS INCOMING, STAY TUNED!")
