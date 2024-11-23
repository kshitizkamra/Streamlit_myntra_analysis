

from navigation import make_sidebar
import hmac
import streamlit as st
import pandas as pd
import numpy as np
import sys
import glob
import os
from sqlalchemy import create_engine,MetaData,Table, Column, Numeric, Integer, VARCHAR, update,insert,text
import altair as alt
import plotly.express as px
import datetime
import time
import math
import matplotlib.pyplot as plt
import plotly.graph_objects as go


make_sidebar()


engine = create_engine(st.secrets["engine_main"])
conn = st.connection("my_database")

conn = st.connection("my_database")
db_data=conn.query("select * from final_data;")
db_sales_data=conn.query("select * from final_sales")
db_sales_data_for_side_filter=conn.query("select * from final_sales")
db_latlong=conn.query("select * from latlong")
st.markdown("""
    <style>
            .block-container {
                padding-top: 5rem;
                padding-bottom: 0rem;
                padding-left: 1rem;
                padding-right: 1rem;
                    line-height: 30%;
                text-align: center;
                font-size : 15px;
                gap: 0rem;

            }
            .divder{
                padding-top: 0rem;
                padding-bottom: 0rem;
                padding-left: 0rem;
                padding-right: 0rem;
        }

            .box-font {
font-size:14px !important;

}

        .value-font {
font-size:15px !important;

}
                </style>
    """, unsafe_allow_html=True)


container=st.container(border=True)

db_settlement=pd.DataFrame()
db_sales=pd.DataFrame()
db_master=pd.DataFrame()
db_actions=pd.DataFrame()
db_recommendation=pd.DataFrame()


with st.container(border=True) :
   st.subheader("Settlements")
   col1, col2 = st.columns([2,1],gap="small")
   with col1:
    uploaded_settlement = st.file_uploader(
    "Upload Settlement Files ", accept_multiple_files=True
    )

   with col2 :
    
    
    st.write("")
    st.write("")
    
    portal_selection_settlement=st.selectbox("Select",st.secrets["portals"],index=0,label_visibility='collapsed',key="settlement")
    subcol1,subcol2,subcol3=st.columns([2,3,2],gap="small")
    with subcol2 :
        if st.button('Upload',key="settlement_btn"):
            settlement_bar = st.progress(0, text="Uploading")
            st.cache_data.clear()
            total_settlement_files=len(uploaded_settlement)
            x=0
            
            for filename in uploaded_settlement:
                x=x+1
                settlement_bar.progress(x/total_settlement_files, text="Uploading")
                df = pd.read_csv(filename, index_col=None, header=0)
                df.columns = [x.lower() for x in df.columns]
                if portal_selection_settlement=="Myntra" :
                   try:
                    df1=df[['order_release_id','customer_paid_amt','commission','igst_tcs','cgst_tcs','sgst_tcs','tds','total_logistics_deduction','pick_and_pack_fee','fixed_fee','payment_gateway_fee','logistics_commission','settled_amount','payment_date','order_type']].copy()
                    df1['total_tax_on_logistics']=df1['logistics_commission']-df1['total_logistics_deduction']-df1['pick_and_pack_fee']-df1['fixed_fee']-df1['payment_gateway_fee']
                    df1['tcs_amount']=df1['igst_tcs']+df1['cgst_tcs']+df1['sgst_tcs']
                    df1['sequence']=2
                    df1.rename(columns = {'commission':'platform_fees','tds':'tds_amount','total_logistics_deduction':'shipping_fee','logistics_commission':'total_logistics','settled_amount':'total_actual_settlement'}, inplace = True)
                    df1=df1.drop(['igst_tcs','cgst_tcs','sgst_tcs'],axis=1) 
                    df1['channel']="Myntra"
                   except:
                        try:
                            df1= df[['order_release_id','customer_paid_amt','platform_fees','tcs_amount','tds_amount',  'shipping_fee','pick_and_pack_fee','fixed_fee','payment_gateway_fee','total_tax_on_logistics','total_actual_settlement','settlement_date_prepaid_payment','settlement_date_postpaid_comm_deduction','shipment_zone_classification']].copy()
                            df1['total_logistics']=df1['shipping_fee']+df1['total_tax_on_logistics']+df1['pick_and_pack_fee']+df1['fixed_fee']+df1['payment_gateway_fee']
                            df1['settlement_date_prepaid_payment']=pd.to_datetime(df['settlement_date_prepaid_payment'], format='ISO8601')
                            df1['settlement_date_postpaid_comm_deduction']=pd.to_datetime(df['settlement_date_postpaid_comm_deduction'], format='ISO8601')  
                            df1['payment_date']=df1[['settlement_date_postpaid_comm_deduction','settlement_date_prepaid_payment']].max(1)
                            df1['sequence']=1
                            df1['order_type']='Forward'
                            df1.loc[df1['total_actual_settlement']<0,'order_type']='Reverse'
                            df1=df1.drop(['settlement_date_prepaid_payment','settlement_date_postpaid_comm_deduction'],axis=1)
                            df1['channel']="Myntra"
                        except:
                            st.write(str(filename.name)+" not uploaded, wrong format")
                      
                   db_settlement = pd.concat([db_settlement, df1], ignore_index=True, sort=False)
            settlement_bar.empty()
            st.write("Uploaded Successfully")    
    db_settlement=db_settlement.drop_duplicates()
    
    try:
        db_settlement.to_sql(
        name="settlement_upload", # table name
        con=engine,  # engine
        if_exists="append", #  If the table already exists, append
        index=False # no index
        )        
    except :
        db_settlement.to_sql(
        name="settlement_upload", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )


with st.container(border=True) :
   st.subheader("Sales")
   col1, col2 = st.columns([2,1],gap="small")
   with col1:
    uploaded_sales = st.file_uploader(
    "Upload Sales Files ", accept_multiple_files=True
    )

   with col2 :
    
    
    st.write("")
    st.write("")
    
    portal_selection_sales=st.selectbox("Select",st.secrets["portals"],index=0,label_visibility='collapsed',key="sales")
    subcol1,subcol2,subcol3=st.columns([2,3,2],gap="small")
    with subcol2 :
        if st.button('Upload',key="sales_btn"):
            sales_bar = st.progress(0, text="Uploading")
            st.cache_data.clear()
            total_sales_files=len(uploaded_sales)
            y=0
            
            for filename in uploaded_sales:
                y=y+1
                abc=0
                sales_bar.progress(y/total_sales_files, text="Uploading")
                df = pd.read_csv(filename, index_col=None, header=0)
                df.columns = [x.lower() for x in df.columns]
                if portal_selection_sales=="Myntra" :
                   try:
                    df1=df[['order release id','myntra sku code','state','created on','seller id','order status','return creation date','final amount']].copy()
                    df1['returns']=0
                    df1.loc[df1['return creation date']>'01-01-2000','returns']=1
                    df1.drop(['return creation date'],axis=1,inplace=True)
                    df1.rename(columns = {'order release id':'order_release_id','myntra sku code':'sku_code','created on':'order_created_date','seller id':'seller_id','order status':'order_status','final amount':'final_amount'}, inplace = True)
                    df1['channel']="Myntra"
                    df1['order_created_date']=pd.to_datetime(df1['order_created_date'],dayfirst=True, format='mixed')
                    
                    abc=abc+1
                   except:
                        st.write(str(filename.name))
                        st.write(" not uploaded, wrong format")
                        st.write("")
                      
                   db_sales = pd.concat([df1, db_sales], ignore_index=True, sort=False)
            sales_bar.empty()
            if abc>0 :
                st.write("Uploaded Successfully")    
    db_sales=db_sales.drop_duplicates(subset="order_release_id",keep='first')
    
    try:
        db_sales.to_sql(
        name="sales_upload", # table name
        con=engine,  # engine
        if_exists="append", #  If the table already exists, append
        index=False # no index
        )        
    except :
        db_sales.to_sql(
        name="sales_upload", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )


with st.container(border=True) :
   st.subheader("Style Master")
   col1, col2 = st.columns([2,1],gap="small")
   with col1:
    uploaded_master = st.file_uploader(
    "Upload Master File ", accept_multiple_files=True
    )

   with col2 :
    
    
    st.write("")
    st.write("")
    st.write("")
    
    
    subcol1,subcol2,subcol3=st.columns([2,3,2],gap="small")
    with subcol2 :
        if st.button('Upload',key="master_btn"):
            master_bar = st.progress(0, text="Uploading")
            st.cache_data.clear()
            total_master_files=len(uploaded_master)
            y=0
            
            for filename in uploaded_master:
                y=y+1
                master_bar.progress(y/total_master_files, text="Uploading")
                df = pd.read_csv(filename, index_col=None, header=0)
                df.columns = [x.lower() for x in df.columns]
                try:
                        df1=df[['channel name','channel product id','seller sku code','vendor sku code','channel style id','vendor style code','brand','gender','article type','image link','size','cost','mrp','color','fabric','collection name']].copy()
                except:
                            st.write(str(filename.name)+" not uploaded, wrong format")
                      
                db_master = pd.concat([db_master, df1], ignore_index=True, sort=False)
            db_master.rename(columns = {'channel name':'channel','channel product id':'channel_product_id','seller sku code':'sku_code','vendor sku code':'vendor_sku_code','channel style id':'channel_style_id','vendor style code':'vendor_style_code','article type':'article_type','image link':'image_link','collection name':'collection'}, inplace = True)
            master_bar.empty()
            st.write("Uploaded Successfully")    
    db_master=db_master.drop_duplicates()
    
    try:
        db_master.to_sql(
        name="master_upload", # table name
        con=engine,  # engine
        if_exists="append", #  If the table already exists, append
        index=False # no index
        )        
    except :
        db_master.to_sql(
        name="master_upload", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )


with st.container(border=True) :
   st.subheader("Actions Category")
   col1, col2 = st.columns([2,1],gap="small")
   with col1:
    uploaded_actions = st.file_uploader(
    "Upload actions File ", accept_multiple_files=True
    )

   with col2 :
    
    
    st.write("")
    st.write("")
    
    portal_selection_actions=st.selectbox("Select",st.secrets["portals"],index=0,label_visibility='collapsed',key="actions")
    subcol1,subcol2,subcol3=st.columns([2,3,2],gap="small")
    with subcol2 :
        if st.button('Upload',key="actions_btn"):
            actions_bar = st.progress(0, text="Uploading")
            st.cache_data.clear()
            total_actions_files=len(uploaded_actions)
            y=0
            
            for filename in uploaded_actions:
                y=y+1
                actions_bar.progress(y/total_actions_files, text="Uploading")
                df = pd.read_csv(filename, index_col=None, header=0)
                df.columns = [x.lower() for x in df.columns]
                if portal_selection_actions=="Myntra" :
                   try:
                        df1=df[['brand','gender','article_type','metrics','a','b','c']].copy()
                   except:
                            st.write(str(filename.name)+" not uploaded, wrong format")
                      
                   db_actions = pd.concat([db_actions, df1], ignore_index=True, sort=False)
                   db_actions['channel']="Myntra"
                   
            actions_bar.empty()
            st.write("Uploaded Successfully")    
    db_actions=db_actions.drop_duplicates()
    
    try:
        db_actions.to_sql(
        name="actions_upload", # table name
        con=engine,  # engine
        if_exists="append", #  If the table already exists, append
        index=False # no index
        )        
    except :
        db_actions.to_sql(
        name="actions_upload", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )


with st.container(border=True) :
   st.subheader("Recommendations")
   col1, col2 = st.columns([2,1],gap="small")
   with col1:
    uploaded_recommendation = st.file_uploader(
    "Upload recommendation File ", accept_multiple_files=True
    )

   with col2 :
    
    
    st.write("")
    st.write("")
    subcol1,subcol2,subcol3=st.columns([2,3,2],gap="small")
    with subcol2 :
        if st.button('Upload',key="recommendation_btn"):
            recommendation_bar = st.progress(0, text="Uploading")
            st.cache_data.clear()
            total_recommendation_files=len(uploaded_recommendation)
            y=0
            
            for filename in uploaded_recommendation:
                y=y+1
                recommendation_bar.progress(y/total_recommendation_files, text="Uploading")
                df = pd.read_csv(filename, index_col=None, header=0)
                df.columns = [x.lower() for x in df.columns]
                try:
                        df1=df[['ros','roi','return %','selling_price','pla','replenishment','remarks']].copy()
                except:
                            st.write(str(filename.name)+" not uploaded, wrong format")
                      
                db_recommendation = pd.concat([db_recommendation, df1], ignore_index=True, sort=False)
                   
            recommendation_bar.empty()
            st.write("Uploaded Successfully")    
    db_recommendation=db_recommendation.drop_duplicates()
    
    try:
        db_recommendation.to_sql(
        name="recommendation_upload", # table name
        con=engine,  # engine
        if_exists="append", #  If the table already exists, append
        index=False # no index
        )        
    except :
        db_recommendation.to_sql(
        name="recommendation_upload", # table name
        con=engine,  # engine
        if_exists="replace", #  If the table already exists, append
        index=False # no index
        )