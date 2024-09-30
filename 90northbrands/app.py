import hmac
import streamlit as st
import pandas as pd
import numpy as np
import sys
import glob
import os
from sqlalchemy import create_engine
import altair as alt
import plotly.express as px
import datetime
import time
import math
import matplotlib.pyplot as plt
import plotly.graph_objects as go

st.set_page_config(layout="wide",initial_sidebar_state='expanded')

# st.markdown(
#     """
# <style>
#     [data-testid="collapsedControl"] {
#         display: none
#     }
# </style>
# """,
#     unsafe_allow_html=True,
# )
st.cache_data.clear()
engine = create_engine(st.secrets["engine_main"])
conn = st.connection("my_database")
st.session_state["login_check"]=0

def check_password():
    """Returns `True` if the user had a correct password."""

    def login_form():
        """Form with widgets to collect user information"""
        with st.form(key="Credentials_1"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in st.secrets[
            "passwords"
        ] and hmac.compare_digest(
            st.session_state["password"],
            st.secrets.passwords[st.session_state["username"]],
        ):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the username or password.
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    # Return True if the username + password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show inputs for username + password.
    login_form()   
    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")
    return False
col1,col2,col3=st.columns([4,1,5])
with col2 :
    # st.image("logo.png",width=150)
    st.write("try")
with col3:
    col5,col6=st.columns([3,2])
    with col6:
            st.write("")
            st.write("")
            with st.popover("Sign In"):
                
                if check_password():
                    st.session_state["login_check"]=1

if st.session_state['login_check']==1 :
    tab_so,tab_pnl,tab_sr,tab_sa,tab_exp,tab_imp,tab_sync,tab_trial=st.tabs(["Sales Overview","P&L","Style_Review","Suggested Actions","Export_data","Import_data","Sync_data","trial"])
else :
    home,about_us=st.tabs(["Home","About Us"])

try :
    with about_us:
     st.write('<a href="/Sales_Overview" target="_self">Next page</a>', unsafe_allow_html=True)

except :
    conn = st.connection("my_database")
    
                        
    with tab_trial :
         with st.container(border=True) :
            st.subheader("latlong")
            col1, col2 = st.columns([2,1],gap="small")
            with col1:
                uploaded_lat_long = st.file_uploader(
                "Upload recommendation File ", accept_multiple_files=True
                )

            with col2 :
                
                
                st.write("")
                st.write("")
                subcol1,subcol2,subcol3=st.columns([2,3,2],gap="small")
                with subcol2 :
                    if st.button('Upload',key="lat_long_btn"):
                        recommendation_bar = st.progress(0, text="Uploading")
                        st.cache_data.clear()
                        total_recommendation_files=len(uploaded_lat_long)
                        y=0
                        
                        for filename in uploaded_lat_long:
                            y=y+1
                            recommendation_bar.progress(y/total_recommendation_files, text="Uploading")
                            df = pd.read_csv(filename, index_col=None, header=0)
                            df.columns = [x.lower() for x in df.columns]
                            
                        recommendation_bar.empty()
                        st.write("Uploaded Successfully")    
                
                try:
                    df.to_sql(
                    name="latlong", # table name
                    con=engine,  # engine
                    if_exists="append", #  If the table already exists, append
                    index=False # no index
                    )        
                except :
                    df.to_sql(
                    name="latlong", # table name
                    con=engine,  # engine
                    if_exists="replace", #  If the table already exists, append
                    index=False # no index
                    )


                            

                        
                        





