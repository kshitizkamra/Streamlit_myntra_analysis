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
with st.sidebar:
        st.title('Filters')
        today = datetime.datetime.now()
        next_year = today.year + 1
        start_date = db_sales_data_for_side_filter['order_created_date'].min()
        end_date = db_sales_data_for_side_filter['order_created_date'].max()

        date_range = st.date_input(
        "Date Range",
        (start_date,end_date),
        start_date,
        end_date,
        format="MM.DD.YYYY",
        )

        db_channel=conn.query("select distinct channel from master;")
        channel_list = db_channel['channel'].values.tolist()
        channels = st.multiselect(
        "Channel",
        channel_list,
        channel_list,
        )

        db_seller=conn.query("select distinct channel_x,seller_id from final_sales;")
        db_seller=db_seller[(db_seller['channel_x'].isin(channels))]
        db_seller=db_seller.drop(['channel_x'],axis=1)
        db_seller=db_seller.drop_duplicates()
        seller_list = db_seller['seller_id'].values.tolist()
        seller = st.multiselect(
        "Seller_id",
        seller_list,
        seller_list,
        )

        db_gender=conn.query("select distinct gender,seller_id from final_sales;")
        db_gender=db_gender[(db_gender['seller_id'].isin(seller))]
        db_gender=db_gender.drop(['seller_id'],axis=1)
        db_gender=db_gender.drop_duplicates()
        gender_list = db_gender['gender'].values.tolist()
        genders = st.multiselect(
        "Gender",
        gender_list,
        gender_list,
        )

        db_brands=conn.query("select distinct brand,gender,seller_id from final_sales;")
        db_brands=db_brands[(db_brands['seller_id'].isin(seller))]
        db_brands=db_brands.drop(['seller_id'],axis=1)
        db_brands=db_brands.drop_duplicates()
        db_brands=db_brands[(db_brands['gender'].isin(genders))]
        db_brands=db_brands.drop(['gender'],axis=1)
        db_brands=db_brands.drop_duplicates()
        brands_list = db_brands['brand'].values.tolist()
        brands = st.multiselect(
        "Brands",
        brands_list,
        brands_list,
        )

        db_article_type=conn.query("select distinct article_type,brand from master")
        db_article_type=db_article_type[(db_article_type['brand'].isin(brands))]
        db_article_type=db_article_type.drop(['brand'],axis=1)
        db_article_type=db_article_type.drop_duplicates()
        article_type_list = db_article_type['article_type'].values.tolist()
        article_type = st.multiselect(
        "Article Types",
        article_type_list,
        article_type_list,
        )

try:
        db_data=db_data[(db_data['order_created_date'].dt.date >= date_range[0]) & (db_data['order_created_date'].dt.date <=date_range[1] )]
        db_data=db_data[(db_data['channel'].isin(channels))]
        db_data=db_data[(db_data['seller_id'].isin(seller))]
        db_data=db_data[(db_data['gender'].isin(genders))]
        db_data=db_data[(db_data['brand'].isin(brands))]
        db_data=db_data[(db_data['article_type'].isin(article_type))]

        db_sales_data=db_sales_data[(db_sales_data['order_created_date'].dt.date >= date_range[0]) & (db_sales_data['order_created_date'].dt.date <=date_range[1] )]
        db_sales_data=db_sales_data[(db_sales_data['channel_x'].isin(channels))]
        db_sales_data=db_sales_data[(db_sales_data['seller_id'].isin(seller))]
        db_sales_data=db_sales_data[(db_sales_data['gender'].isin(genders))]
        db_sales_data=db_sales_data[(db_sales_data['brand'].isin(brands))]
        db_sales_data=db_sales_data[(db_sales_data['article_type'].isin(article_type))]
        


except:
        db_data=db_data
        db_sales_data=db_sales_data        










db_data_final=db_data
db_sales_data_final=db_sales_data

tab1,tab2=st.tabs(['Sales_Data','Settlement_Data'])
with tab1:
    db_sales_data_final['month']=pd.to_datetime(db_sales_data_final['order_created_date']).dt.strftime('%b-%y')
    db_sales_data_final.rename(columns={'channel_x' : 'channel'},inplace=True)
    db_sales_data_final.drop(['sku_code_x','channel_y'],inplace=True,axis=1)
    db_sales_data_final.sort_values(by=['order_created_date'],ascending=False,inplace=True)
    db_sales_data_final['return_amount']=db_sales_data_final['final_amount']*db_sales_data_final['returns']
    db_sales_data_final.reset_index(inplace=True)

    db_sales_data_display=db_sales_data_final
    
    with st.container(border=True):
                
        
        
        group_by_list = ['channel','seller_id','state','brand','gender','article type','month','vendor_style_code','size','fabric','collection','mrp','cost','color']
        
        group_by = st.multiselect(
    "Group By",
    group_by_list,
    placeholder="Select for Grouping the data",
    label_visibility='collapsed'
    )

        if len(group_by)>0:
                
            db_sales_data_display=db_sales_data_final.groupby(group_by,sort=False).agg({'order_release_id':'count','final_amount':'sum','returns':'sum','return_amount':'sum'})
            
            # db_data_display= db_data_display.iloc[:,[0,1,2,14,15,11,12,13,16,17,18,19,20,21,22,23,24,25,3,4,5,6,7,8,9,10]]  
            st.divider()
            db_sales_data_display.rename(columns={'order_release_id' : 'Total_Orders'},inplace=True)
            db_sales_data_display
        else :
            db_sales_data_final

with tab2:
    
        
    db_data_final['order_count']=db_data_final['return_count']=1
    db_data_final.loc[db_data_final['order_type']=='Reverse','order_count']=0
    db_data_final.loc[db_data_final['returns']==1,'cost']=0
    db_data_final.loc[db_data_final['returns']==1,'customer_paid_amt']=0
    db_data_final.loc[db_data_final['returns']==1,'platform_fees']=0
    db_data_final.loc[db_data_final['returns']==1,'tcs_amount']=0
    db_data_final.loc[db_data_final['returns']==1,'tds_amount']=0
    db_data_final['return_count']=0
    db_data_final.loc[(db_data_final['returns']==1)&(db_data_final['order_type']=='Forward'),'return_count']=1
    
    db_data_final['settlement']=db_data_final['customer_paid_amt']-db_data_final['platform_fees']-db_data_final['tcs_amount']-db_data_final['tds_amount']-db_data_final['total_logistics']
    # db_data_final.drop(['total_logistics'],inplace=True, axis=1)
    db_data_final['month']=pd.to_datetime(db_data_final['order_created_date']).dt.strftime('%b-%y')
    db_data_final.sort_values(by=['order_created_date'],ascending=False,inplace=True)
    db_data_display=db_data_final
    with st.container(border=True):
                
        
        
        group_by_list = ['channel','seller_id','state','brand','gender','article type','month','vendor_style_code','shipment_zone_classification','size','fabric','collection name','mrp','cost','color']
        
        group_by = st.multiselect(
    "Group By",
    group_by_list,
    placeholder="Select for Grouping the data",
    label_visibility='collapsed'
    )

        if len(group_by)>0:
                
            db_data_display=db_data_final.groupby(group_by,sort=False).agg({'order_count':'sum','return_count':'sum','customer_paid_amt':'sum','platform_fees':'sum','tcs_amount':'sum','tds_amount':'sum','shipping_fee':'sum','pick_and_pack_fee':'sum','fixed_fee':'sum','payment_gateway_fee':'sum','total_tax_on_logistics':'sum','total_logistics':'sum','settlement':'sum','cost':'sum'})
            db_data_display['commision']=db_data_display['platform_fees']
            db_data_display['taxes']=db_data_display['tcs_amount']+db_data_display['tds_amount']
            db_data_display['p/l']=db_data_display['settlement']-db_data_display['cost']
            db_data_display['roi']=db_data_display['p/l']/db_data_display['cost']
            db_data_display['return %']=db_data_display['return_count']/db_data_display['order_count'] 
            db_data_display['asp']=db_data_display['customer_paid_amt']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display['commision/unit']=db_data_display['commision']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display['taxes/unit']=db_data_display['taxes']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display['logistics/unit']=db_data_display['total_logistics']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display['settlement/unit']=db_data_display['settlement']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display['cost/unit']=db_data_display['cost']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display['p&l/unit']=db_data_display['p/l']/(db_data_display['order_count']-db_data_display['return_count'])
            db_data_display= db_data_display.iloc[:,[0,1,2,14,15,11,12,13,16,17,18,19,20,21,22,23,24,25,3,4,5,6,7,8,9,10]]  
            st.divider()
            
            db_data_display
        else :
            db_data_final
