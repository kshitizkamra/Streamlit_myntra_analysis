import streamlit as st
from time import sleep
from streamlit.runtime.scriptrunner import get_script_run_ctx
from streamlit.source_util import get_pages


def get_current_page_name():
    ctx = get_script_run_ctx()
    if ctx is None:
        raise RuntimeError("Couldn't get script context")

    pages = get_pages("")

    return pages[ctx.page_script_hash]["page_name"]


def make_sidebar():
    with st.sidebar:
        st.title("💎 90NorthBrands")
        st.divider()
        

        if st.session_state.get("logged_in", False):
            st.page_link("pages/Sales_Overview.py", label="Sales Overview", icon="💹")
            st.page_link("pages/P&L.py", label="P&L", icon="💸")
            st.page_link("pages/Style_Review.py", label="StyleReview", icon="👕")
            st.page_link("pages/Actions.py", label="Actions", icon="⏯️")
            st.page_link("pages/Data_Export.py", label="Data Export", icon="📨")
            st.page_link("pages/Data_Import.py", label="Data Import", icon="📩")
            st.page_link("pages/Data_Sync.py", label="Data Sync", icon="♾️")
            

            

            if st.button("Log out"):
                logout()
            st.divider()
        elif get_current_page_name() != "home":
            # If anyone tries to access a secret page without being logged in,
            # redirect them to the login page
            st.switch_page("home.py")


def logout():
    st.session_state.logged_in = False
    st.info("Logged out successfully!")
    sleep(0.5)
    st.switch_page("home.py")