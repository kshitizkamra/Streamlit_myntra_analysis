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


st.set_page_config(
    page_title="Myntra Dashboard",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

conn = st.connection("my_database")
db_master=conn.query("select * from styles_master;")


db_sales=conn.query("select * from final_sales;")
db_settlement=conn.query("select * from final_settlement;")
db_sales_final=conn.query("select * from sales_data_sync;")
db_styles_action=conn.query("select * from styles_action;")
db_actual_actions=conn.query("select * from actual_actions")


st.markdown("""
        <style>
               .block-container {
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
                    text-align: center;
                    font-size : 7px;

                }
              .divder{
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
            }
        </style>
        """, unsafe_allow_html=True)


with st.sidebar:
    st.title('Filters')
    today = datetime.datetime.now()
    next_year = today.year + 1
    start_date = db_sales['order_created_date'].min()
    end_date = db_sales['order_created_date'].max()

    date_range = st.date_input(
    "Date Range",
    (start_date,end_date),
    start_date,
    end_date,
    format="MM.DD.YYYY",
    )

    db_gender=conn.query("select distinct gender from styles_master;")
    gender_list = db_gender['gender'].values.tolist()
    genders = st.multiselect(
      "Gender",
      gender_list,
      gender_list,
    )

    db_brands=conn.query("select distinct brand,gender from styles_master;")
    db_brands=db_brands[(db_brands['gender'].isin(genders))]
    db_brands=db_brands.drop(['gender'],axis=1)
    db_brands=db_brands.drop_duplicates()
    brands_list = db_brands['brand'].values.tolist()
    brands = st.multiselect(
      "Brands",
      brands_list,
      brands_list,
    )

    db_article_type=conn.query("select distinct \"article type\",brand from styles_master")
    db_article_type=db_article_type[(db_article_type['brand'].isin(brands))]
    db_article_type=db_article_type.drop(['brand'],axis=1)
    db_article_type=db_article_type.drop_duplicates()
    article_type_list = db_article_type['article type'].values.tolist()
    article_type = st.multiselect(
      "Article Types",
      article_type_list,
      article_type_list,
    )










db_sales_final=db_sales_final[(db_sales_final['order_created_date'].dt.date >= date_range[0]) & (db_sales_final['order_created_date'].dt.date <=date_range[1] )]

db_sales_final=db_sales_final[(db_sales_final['gender'].isin(genders))]

db_sales_final=db_sales_final[(db_sales_final['brand'].isin(brands))]

db_sales_final=db_sales_final[(db_sales_final['article type'].isin(article_type))]

db_sales_final['week']=db_sales_final['order_created_date'].dt.isocalendar().week
db_sales_final['start']=db_sales_final['order_created_date'].dt.to_period('W').apply(lambda r: r.start_time)
db_sales_final['end']=db_sales_final['start']+pd.DateOffset(days=6)
db_sales_final['date_range']=db_sales_final['start'].dt.date.astype(str)+"---"+db_sales_final['end'].dt.date.astype(str)







def checkbox_container(data):
    st.markdown("<h1 style='text-align: center; color: grey;'>Week on Week</h1>", unsafe_allow_html=True)
    st.markdown("<h4 style='text-align: center; color: grey;'>Select Attributes</h4>", unsafe_allow_html=True)
    st.divider()
    cols = st.columns(len(attributes))
    count=0
    for i in data:
        with cols[count]:
            st.checkbox(i, key='dynamic_checkbox_' + i)
            count=count+1



if 'attributes' not in st.session_state.keys():
    attributes = ['brand','gender','article type','vendor style code']
    st.session_state['attributes'] = attributes
else:
    attributes = st.session_state['attributes']

def get_selected_attributes():
    return [i.replace('dynamic_checkbox_','') for i in st.session_state.keys() if i.startswith('dynamic_checkbox_') and st.session_state[i]]


checkbox_container(attributes)
st.divider()

db_sales_final_1=db_sales_final.copy()

try :
    db_search_style_code=db_sales_final['vendor style code'].drop_duplicates()
    search_style_code_list = db_search_style_code.values.tolist()
    search_style_code = st.multiselect(
      "Search/Select Style Code",
      search_style_code_list,
      placeholder="Search/Select Style Code",
      label_visibility='collapsed'
    )



    if len(search_style_code)>0 :
       db_sales_final=db_sales_final_1[(db_sales_final_1['vendor style code'].isin(search_style_code))]
       st.session_state['page_number'] = 1
    else :
        db_sales_final=db_sales_final_1.copy()
        
    st.divider()
    selected_attribute=get_selected_attributes()
    selected_attribute.append('week')
    selected_attribute.append('date_range')
    db_sales_weekly=db_sales_final.groupby(selected_attribute).agg({'total_orders':'sum','successful_orders':'sum','returns':'sum','customer_paid_amount':'sum','taxes':'sum','commission':'sum','logistics':'sum','settled_amount':'sum','cost':'sum'})
    db_sales_weekly['profit_value']=db_sales_weekly['settled_amount']-db_sales_weekly['cost']
    db_sales_weekly['sales vol %']=db_sales_weekly['successful_orders']/db_sales_weekly['successful_orders'].sum()
    db_sales_weekly['sales vol %'] = db_sales_weekly['sales vol %'].map('{:.1%}'.format)
    db_sales_weekly['val %']=db_sales_weekly['settled_amount']/db_sales_weekly['settled_amount'].sum()
    db_sales_weekly['val %'] = db_sales_weekly['val %'].map('{:.1%}'.format)
    db_sales_weekly['unit_customer_paid_amount']=db_sales_weekly['customer_paid_amount']/db_sales_weekly['successful_orders']
    db_sales_weekly['unit_taxes']=db_sales_weekly['taxes']/db_sales_weekly['successful_orders']
    db_sales_weekly['unit_commission']=db_sales_weekly['commission']/db_sales_weekly['successful_orders']
    db_sales_weekly['unit_logistics']=db_sales_weekly['logistics']/db_sales_weekly['successful_orders']
    db_sales_weekly['unit_settled_amount']=db_sales_weekly['settled_amount']/db_sales_weekly['successful_orders']
    db_sales_weekly['unit_cost']=db_sales_weekly['cost']/db_sales_weekly['successful_orders']
    db_sales_weekly['unit_p/l']=db_sales_weekly['unit_settled_amount']-db_sales_weekly['unit_cost']
    db_sales_weekly['roi']=db_sales_weekly['profit_value']/db_sales_weekly['cost']
    db_sales_weekly['return %']=db_sales_weekly['returns']/db_sales_weekly['total_orders']

    pd.set_option('display.max_colwidth', None)
    db_sales_weekly=db_sales_weekly.round(2)
    db_sales_weekly['roi'] = db_sales_weekly['roi'].map('{:.0%}'.format)
    db_sales_weekly['return %'] = db_sales_weekly['return %'].map('{:.0%}'.format)
    db_sales_weekly=db_sales_weekly.sort_values('week',ascending=False)
    db_sales_weekly=db_sales_weekly.reset_index()
    days=7
    total_profit=db_sales_weekly['profit_value'].sum()
    db_sales_weekly['ROS']=db_sales_weekly['successful_orders']/days
    db_sales_weekly['profit %']=db_sales_weekly['profit_value']/total_profit
    db_sales_weekly=db_sales_weekly.round(2)
    db_sales_weekly['profit %'] = db_sales_weekly['profit %'].map('{:.0%}'.format)
    total_pages=len(db_sales_weekly)
    db_sales_weekly.reset_index(inplace=True)
    db_sales_weekly.index += 1
    db_sales_weekly=db_sales_weekly.drop(['index'],axis=1)

    db_sales_weekly
 
except:
    st.write("Select attributes")


