#--IMPORTING AND GENERAL SETUP
import os
from dotenv import load_dotenv
import streamlit as st
from pymongo import MongoClient
from datetime import datetime, timezone, date


#--STREAMLIT CONFIGURATION
st.set_page_config(layout="wide")

#st.write("Loaded URI:", os.environ.get("MongoLink"))

#--ENVIRONMENT LOADING AND DB CONNECTION
load_dotenv()

dbconn = os.getenv("MongoLink")
client = MongoClient(dbconn)
db = client['genesis_tokens_swap_info']

# UI elements
# --GLOBAL CSS
st.markdown("""
<style>
/* Remove top padding in main block */
.block-container {
    padding-top: 0rem !important;
}
section.main > div:first-child {
    padding-top: 0rem;
    margin-top: -3rem;
}
header[data-testid="stHeader"] {
    background: transparent;
    visibility: visible;
}
[data-testid="stSidebarNav"] {
    display: none;
}
section[data-testid="stSidebar"] {
    background: rgba(255, 255, 255, 0.35);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    box-shadow: 2px 0 10px rgba(0, 0, 0, 0.1);
}
section[data-testid="stSidebar"] .markdown-text-container {
    color: white !important;
}
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
    background: #013155;
    background-size: cover;
    background-attachment: fixed;
    background-position: center;
}
.card {
    backdrop-filter: blur(12px);
    background: rgba(255, 255, 255, 0.25);
    padding: 16px;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    min-height: 200px;
}
.card:hover {
    transform: translateY(-7px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    color:white;
}
.card h1 {
    margin-top: 0;
    font-size: 2rem;
    color: white;
    font-weight: bold;
    margin-bottom: 8px;
}
.card p {
    color: white;
    font-weight: semibold;
    margin: 2px 0;
}
.card a {
    display: inline-block;
    padding: 6px 12px;
    background: rgba(255, 255, 255, 0.2);
    color: white;
    font-weight: semibold;
    text-decoration: none;
    border-radius: 6px;
    margin-top: 8px;
    font-size: 1rem;
    font-weight: 500;
    backdrop-filter: blur(5px);
    transition: background 0.2s ease;
}
.card a:hover {
    background: rgba(255, 255, 255, 0.25);
    color: #173d5f;
}
</style>
""", unsafe_allow_html=True)
#trigger rebuild
# ───── Sidebar ─────
import streamlit as st
import os

def render_sidebar():
    with st.sidebar:
        st.markdown("---")

        # Define clean route paths (not filenames)
        routes = [
            ("🏠 Home", "/"),
            ("🌍 Global Sniper Analysis", "/global_snipers")
        ]

        # Detect current path
        current_path = st.query_params.get("page", [""])[0]
        if current_path == "":
            current_path = "/"  # fallback to home if no query param

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

st.markdown("""
<style>
button[kind="popover"] {
    font-size: 18px !important;
    padding: 12px 24px !important;
    border-radius: 8px !important;
    background-color: #1f77b4 !important;
    color: white !important;
}
</style>
""", unsafe_allow_html=True)


#--THE PAGE HEADER
st.markdown("<h1 style='color: white;'>SUCCESSFULLY LAUNCHED GENESIS TOKENS</h1>", unsafe_allow_html=True)
h1, h2 = st.columns([6,1])
with h2:
    from datetime import date
    with st.popover("FILTER TOKENS",icon="🔽", use_container_width=True):
            s1, s2, s3 = st.columns(3)
            with s1:
                search_query = st.text_input("Search by Token Name or Address", key="search").lower()
            with s2:
                start_date = st.date_input("Launch Start Date", value=date(2024, 4, 1), key="start")
            with s3:
                end_date = st.date_input("Launch End Date", value=datetime.now().date(), key="end")
            
            sort_row1, sort_row2 = st.columns([2, 1])
            with sort_row1:
                sort_option = st.selectbox("Sort by", ["Launch Date", "Token Name"], key="sort_field")
            with sort_row2:
                sort_order = st.radio("Order", ["Descending", "Ascending"], horizontal=True, key="sort_order")


#--MODULARIZATION
#--DATA FETCHING
# TOKEN COLLECTIONS
token_collection = ['jarvis_swap', 'tian_swap', 'badai_swap', 'aispace_swap', 'wint_swap']

# Map collection name to token symbol
token_map = {
    'jarvis_swap': 'JARVIS',
    'tian_swap': 'TIAN',
    'badai_swap': 'BADAI',
    'aispace_swap': 'AISPACE',
    'wint_swap': 'WINT'
}
#--RENDERING TOKEN CARDS

def shorten(addr):
    if isinstance(addr, str) and addr.startswith("0x") and len(addr) > 10:
        return addr[:6] + "..." + addr[-4:]
    return addr
# FETCH INFO FROM SWAP COLLECTIONS
def fetch_token_docs(db):
    docs = []
    for collection in token_collection:
        token = token_map.get(collection, collection.replace("_swap", "").upper())
        col = db[collection]

        # Get earliest swap document (assumed to be launch time)
        first_doc = col.find_one(sort=[("blockNumber", 1)])
        if not first_doc:
            continue

        # Launch time from earliest tx
        launch_ts = first_doc.get("timestampReadable")
        try:
            launch_dt = datetime.strptime(launch_ts, "%Y-%m-%d %H:%M:%S")
            launch_time = launch_dt.strftime('%d-%m-%Y %H:%M')
        except:
            launch_time = "N/A"

        # Token address from first_doc, fallback to 'maker'
        token_address = first_doc.get("token_address") or first_doc.get("maker") or "N/A"

        doc = {
            "token_symbol": token,
            "token_address": token_address,
            "updated_at": launch_dt if launch_time != "N/A" else None,
            "launch_time": launch_time
        }
        docs.append(doc)
    return docs

# RENDER FUNCTION (UNTOUCHED)
def render_token_cards_from_docs(docs, num_cols=5):
    for i in range(0, len(docs), num_cols):
        chunk = docs[i:i + num_cols]
        with st.container():
            cols = st.columns(num_cols, vertical_alignment="center")
            for j, doc in enumerate(chunk):
                with cols[j]:
                    name = doc["token_symbol"]
                    token_address = shorten(doc.get("token_address", "N/A"))
                    full_token_address = doc.get("token_address", "")
                    launch_time = doc.get("launch_time", "N/A")

                    card_html = f"""
                    <div class="card">
                        <h1>{name}</h1>
                        <p><b>Name:</b> {name}</p>
                        <p><b>Launch Time:</b> {launch_time}</p>
                        <p title="{full_token_address}"><b>Token:</b> {token_address}</p>
                        <a href="/tokendatatestcopy?token={name}" target="_blank">See TXNs</a>
                    </div>
                    """
                    st.markdown(card_html, unsafe_allow_html=True)
                    st.write("")

# FETCH AND RENDER
all_docs = fetch_token_docs(db)
render_token_cards_from_docs(all_docs)