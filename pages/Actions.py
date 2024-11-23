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







db_data=conn.query("select * from final_data;")
db_sales_data_for_side_filter=conn.query("select * from final_sales")

        
db_data=db_data[(db_data['order_created_date'].dt.date >= date_range[0]) & (db_data['order_created_date'].dt.date <=date_range[1] )]
st.title ("System Suggested Actions")
db_data['order_count']=0
db_data.loc[db_data['order_type']=='Forward','order_count']=1
db_data.loc[db_data['returns']==1,'cost']=0
db_data.loc[db_data['returns']==1,'customer_paid_amt']=0
db_data.loc[db_data['returns']==1,'platform_fees']=0
db_data.loc[db_data['returns']==1,'tcs_amount']=0
db_data.loc[db_data['returns']==1,'tds_amount']=0
db_data['return_count']=0
db_data.loc[(db_data['returns']==1)&(db_data['order_type']=='Forward'),'return_count']=1
# db_style_data_try=db_data.groupby(['vendor_style_code','channel','brand','gender','article_type'],as_index=False).agg({'order_count':'sum','return_count':'sum','platform_fees':'sum','tcs_amount':'sum','tds_amount':'sum','shipping_fee':'sum','pick_and_pack_fee':'sum','fixed_fee':'sum','payment_gateway_fee':'sum','total_tax_on_logistics':'sum','cost':'sum','order_created_date':'min'})
# db_style_data_try
db_data['settlement']=db_data['customer_paid_amt']-db_data['platform_fees']-db_data['tcs_amount']-db_data['tds_amount']-db_data['shipping_fee']-db_data['pick_and_pack_fee']-db_data['fixed_fee']-db_data['payment_gateway_fee']-db_data['total_tax_on_logistics']
db_data.sort_values(by=['order_created_date'],inplace=True)
db_data['p/l']=db_data['settlement']-db_data['cost']
# db_data
db_style_data=db_data.groupby(['vendor_style_code','channel','brand','gender','article_type'],as_index=False).agg({'order_count':'sum','return_count':'sum','p/l':'sum','cost':'sum','order_created_date':'min'})
db_style_data['ros']=db_style_data['order_count']/(pd.to_datetime(datetime.date.today(),format='ISO8601')-db_style_data['order_created_date']).dt.days
db_style_data['returns']=db_style_data['return_count']/db_style_data['order_count']
db_style_data['roi']=db_style_data['p/l']/db_style_data['cost']
db_style_data['ros_action']=db_style_data['roi_action']=db_style_data['return_action']='D'
db_style_data.drop(['order_count','return_count','p/l','cost','order_created_date'],inplace=True,axis=1)

db_styles_action=conn.query("select * from actions_upload;")
db_actual_action=conn.query("select * from recommendation_upload")

db_styles_action.sort_values(by=['metrics'],inplace=True)


for index,rows in db_style_data.iterrows():
    db_styles_action_tab=db_styles_action[(db_styles_action['brand']==rows.brand)&(db_styles_action['gender']==rows.gender)&(db_styles_action['article_type']==rows.article_type)&(db_styles_action['channel']==rows.channel)]
    db_styles_action_tab.reset_index(inplace=True)
    if rows.ros>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='ros','a'].sum():
        db_style_data.loc[index,'ros_action']='A'
    elif rows.ros>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='ros','b'].sum():
        db_style_data.loc[index,'ros_action']='B'
    else:
        db_style_data.loc[index,'ros_action']='C'

    if rows.roi>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='roi','a'].sum():
        db_style_data.loc[index,'roi_action']='A'
    elif rows.roi>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='roi','b'].sum():
        db_style_data.loc[index,'roi_action']='B'
    else:
        db_style_data.loc[index,'roi_action']='A'


    if rows.returns<=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='return %','a'].sum():
        db_style_data.loc[index,'return_action']='A'
    elif rows.returns<=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='return %','b'].sum():
        db_style_data.loc[index,'return_action']='B'
    else:
        db_style_data.loc[index,'return_action']='C'


    db_actual_action_tab=db_actual_action[(db_actual_action['ros']==db_style_data.loc[index,'ros_action'])&(db_actual_action['roi']==db_style_data.loc[index,'roi_action'])&(db_actual_action['return %']==db_style_data.loc[index,'return_action'])]
    db_actual_action_tab.reset_index(inplace=True)
    db_style_data.loc[index,'selling_price']=db_actual_action_tab['selling_price'][0]
    db_style_data.loc[index,'pla']=db_actual_action_tab['pla'][0]
    db_style_data.loc[index,'replenishment']=db_actual_action_tab['replenishment'][0]
    db_style_data.loc[index,'remarks']=db_actual_action_tab['remarks'][0]



db_style_data

st.title ("Accepted Actions")

db_accpeted_actions=conn.query("select * from action_items_manual;")
db_accpeted_actions
