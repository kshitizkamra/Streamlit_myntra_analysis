import streamlit as st
import pandas as pd
import numpy as np
import sys
import glob
import os
from sqlalchemy import create_engine,MetaData,Table, Column, Numeric, Integer, VARCHAR, update
import altair as alt
import plotly.express as px


conn = st.connection("my_database")






container=st.container(border=True)

with st.container(border=True) :
   col1, col2 = st.columns([2,1],gap="medium")
   with col1:
    uploaded_settlement = st.file_uploader(
    "Upload Settlement Files ", accept_multiple_files=True
    )

   with col2 :
    st.write("")
    st.write("")
    st.write("")

    if st.button('Upload',key="settlement_btn"):
       db_settlement=pd.DataFrame()
       try:
          db_settlement_original=conn.query("select * from final_settlement;")
       except:
          db_settlement_original=pd.DataFrame() 
       for filename in uploaded_settlement:
        df = pd.read_csv(filename, index_col=None, header=0)
        db_settlement = pd.concat([db_settlement, df], ignore_index=True, sort=False)
       st.write(db_settlement.shape)
       db_settlement.columns = [x.lower() for x in db_settlement.columns]
       
       db_settlement=pd.concat([db_settlement,db_settlement_original],ignore_index=True)
       st.write(db_settlement.shape)
       db_settlement=db_settlement.drop_duplicates()
       st.write(db_settlement.shape)
       engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/myntra_roi')
       db_settlement.to_sql(
        name="final_settlement", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )
       
 
with st.container(border=True) : 
  col1, col2 = st.columns([2,1],gap="medium")
  with col1:
    
    uploaded_sales = st.file_uploader(
    "Upload Sales Files ", accept_multiple_files=True
    )
    
  with col2 :
    st.write("")
    st.write("")
    st.write("")
    if st.button('Upload',key="sales_btn"):
       db_sales=pd.DataFrame()
       db_sales_original=pd.DataFrame()
       try :
           db_sales_original=conn.query("select * from final_sales;")
       except :
           db_sales_original=pd.DataFrame()


       st.write(db_sales_original.shape)
       for filename in uploaded_sales:
        df = pd.read_csv(filename, index_col=None, header=0)
        db_sales = pd.concat([db_sales, df], ignore_index=True, sort=False)
       db_sales.columns = [x.lower() for x in db_sales.columns]
       db_sales['order_created_date']=db_sales['order_created_date'].apply(pd.to_datetime)
       st.write(db_sales.shape)
       db_sales=pd.concat([db_sales,db_sales_original],ignore_index=True)
       st.write(db_sales.shape)
       db_sales=db_sales.drop_duplicates()
       st.write(db_sales.shape)
       engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/myntra_roi')
       db_sales.to_sql(
        name="final_sales", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )    
    
with st.container(border=True) : 
  col1, col2 = st.columns([2,1],gap="medium")
  with col1:
     uploaded_master = st.file_uploader(
    "Upload Style Master Files ", accept_multiple_files=True
    )
  with col2 :
    st.write("")
    st.write("")
    st.write("")
    if st.button('Upload',key="master_btn"):
       db_master=pd.DataFrame()
       try :
          db_master_original=conn.query("select * from styles_master;")
       except :
          db_master_original=pd.DataFrame()
       for filename in uploaded_master:
        df = pd.read_csv(filename, index_col=None, header=0)
        db_master = pd.concat([db_master, df], ignore_index=True, sort=False)
       db_master.columns = [x.lower() for x in db_master.columns]
       db_master=pd.concat([db_master,db_master_original],ignore_index=True)
       db_master=db_master.drop_duplicates()
       engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/db_90northbrands')
       db_master.to_sql(
        name="styles_master", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )

with st.container(border=True) : 
  col1, col2 = st.columns([2,1],gap="medium")
  with col1:
    uploaded_action = st.file_uploader(
    "Upload Style action Files ", accept_multiple_files=True
    )
  with col2 :
    st.write("")
    st.write("")
    st.write("")
    
    if st.button('Upload',key="action_btn"):
       db_action=pd.DataFrame()
       try :
          db_action_original=conn.query("select * from styles_action;")
       except :
          db_action_original=pd.DataFrame()
       for filename in uploaded_action:
        df = pd.read_csv(filename, index_col=None, header=0)
        db_action = pd.concat([db_action, df], ignore_index=True, sort=False)
       db_action.columns = [x.lower() for x in db_action.columns]
       db_action=pd.concat([db_action,db_action_original],ignore_index=True)
       db_action=db_action.drop_duplicates()
       engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/db_90northbrands')
       db_action.to_sql(
        name="styles_action", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )

with st.container(border=True) : 
  col1, col2 = st.columns([2,1],gap="medium")
  with col1:
    uploaded_actual_action = st.file_uploader(
    "Upload actual action Files ", accept_multiple_files=True
    )
  with col2 :
    st.write("")
    st.write("")
    st.write("")
    if st.button('Upload',key="actual_action_btn"):
       db_actual_action=pd.DataFrame()
       try :
          db_actual_action_original=conn.query("select * from actual_actions;")
       except :
          db_actual_action_original=pd.DataFrame()
       for filename in uploaded_actual_action:
        df = pd.read_csv(filename, index_col=None, header=0)
        db_actual_action = pd.concat([db_actual_action, df], ignore_index=True, sort=False)
       db_actual_action.columns = [x.lower() for x in db_actual_action.columns]
       db_actual_action=pd.concat([db_actual_action,db_actual_action_original],ignore_index=True)
       db_actual_action=db_actual_action.drop_duplicates()
       engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/db_90northbrands')
       db_actual_action.to_sql(
        name="actual_actions", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )
