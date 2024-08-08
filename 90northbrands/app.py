
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


db_sales_bar1_final=db_sales_final.drop(['order_created_date'],axis=1)

if 'attributes' not in st.session_state.keys():
    attributes = ['seller_id','customer_state','brand','gender','article type','fabric','collection name','vendor style code']
    st.session_state['attributes'] = attributes
else:
    attributes = st.session_state['attributes']

def checkbox_container(data):
    st.markdown("<h1 style='text-align: center; color: grey;'>P/L</h1>", unsafe_allow_html=True)
    st.markdown("<h4 style='text-align: center; color: grey;'>Select Attributes</h4>", unsafe_allow_html=True)
    st.divider()
    cols = st.columns(len(attributes))
    count=0
    for i in data:
        with cols[count]:
            st.checkbox(i, key='dynamic_checkbox_' + i)
            count=count+1

def get_selected_attributes():
    return [i.replace('dynamic_checkbox_','') for i in st.session_state.keys() if i.startswith('dynamic_checkbox_') and st.session_state[i]]


checkbox_container(attributes)
st.divider()

try:
    db_sales_summary=db_sales_bar1_final.groupby(get_selected_attributes()).agg({'total_orders':'sum','successful_orders':'sum','returns':'sum','customer_paid_amount':'sum','taxes':'sum','commission':'sum','logistics':'sum','settled_amount':'sum','cost':'sum'})
    db_sales_summary['profit_value']=db_sales_summary['settled_amount']-db_sales_summary['cost']
    db_sales_summary['sales vol %']=db_sales_summary['successful_orders']/db_sales_summary['successful_orders'].sum()
    db_sales_summary['sales vol %'] = db_sales_summary['sales vol %'].map('{:.1%}'.format)
    db_sales_summary['val %']=db_sales_summary['settled_amount']/db_sales_summary['settled_amount'].sum()
    db_sales_summary['val %'] = db_sales_summary['val %'].map('{:.1%}'.format)

    db_sales_summary['unit_customer_paid_amount']=db_sales_summary['customer_paid_amount']/db_sales_summary['successful_orders']
    db_sales_summary['unit_taxes']=db_sales_summary['taxes']/db_sales_summary['successful_orders']
    db_sales_summary['unit_commission']=db_sales_summary['commission']/db_sales_summary['successful_orders']
    db_sales_summary['unit_logistics']=db_sales_summary['logistics']/db_sales_summary['successful_orders']
    db_sales_summary['unit_settled_amount']=db_sales_summary['settled_amount']/db_sales_summary['successful_orders']
    db_sales_summary['unit_cost']=db_sales_summary['cost']/db_sales_summary['successful_orders']
    db_sales_summary['unit_p/l']=db_sales_summary['unit_settled_amount']-db_sales_summary['unit_cost']

    db_sales_summary['roi']=db_sales_summary['profit_value']/db_sales_summary['cost']
    
    db_sales_summary['return %']=db_sales_summary['returns']/db_sales_summary['total_orders']
   
    total_orders=db_sales_summary['total_orders'].sum()
    successful_orders=db_sales_summary['successful_orders'].sum()
    returns=db_sales_summary['returns'].sum()
    customer_paid_amount=db_sales_summary['customer_paid_amount'].sum()
    taxes=db_sales_summary['taxes'].sum()
    commission=db_sales_summary['commission'].sum()
    logistics=db_sales_summary['logistics'].sum()
    settled_amount=db_sales_summary['settled_amount'].sum()
    cost=db_sales_summary['cost'].sum()
    profit_value=settled_amount-cost
    unit_customer_paid_amount=customer_paid_amount/successful_orders
    unit_taxes=taxes/successful_orders
    unit_commission=commission/successful_orders
    unit_logistics=logistics/successful_orders
    unit_settled_amount=settled_amount/successful_orders
    unit_cost=cost/successful_orders
    unit_pl=unit_settled_amount-unit_cost
    roi=profit_value/cost
    return_perc=returns/total_orders
    
    dict={'total_orders':[total_orders],
          'successful_orders':[successful_orders],
          'returns':[returns],
          'customer_paid_amount':[customer_paid_amount],
          'taxes':[taxes],
          'commission':[commission],
          'logistics':[logistics],
          'settled_amount':[settled_amount],
          'cost':[cost],
          'profit_value':[profit_value],
          'unit_customer_paid_amount':[unit_customer_paid_amount],
          'unit_taxes':[unit_taxes],
          'unit_commission':[unit_commission],
          'unit_logistics':[unit_logistics],
          'unit_settled_amount':[unit_settled_amount],
          'unit_cost':[unit_cost],
          'unit_p/l':[unit_pl],
          'roi':[roi],
          'return %':[return_perc],
          }
    
    db_sales_summary['roi'] = db_sales_summary['roi'].map('{:.0%}'.format)
    db_sales_summary['return %'] = db_sales_summary['return %'].map('{:.0%}'.format)

    pie1, pie2 = st.columns(2,gap="medium")

    with pie1 :
            data = {'cost_type': ['cost','Commission','Logistics','Taxes','Settled Amount'],
                    'total': [cost,commission,logistics,taxes,settled_amount-cost]}
            db_pie=pd.DataFrame(data)
            explode1=(0,0,0,0,0.1)
            fig=px.pie(db_pie,values='total',names='cost_type',title="P/L Summary")
            st.plotly_chart(fig,use_container_width=True)
    with pie2 :
            db_sales_total=pd.DataFrame(dict)
            db_sales_total['Metric']='Total'
            db_sales_total=db_sales_total.set_index('Metric')
            
            pd.set_option('display.max_colwidth', None)
            db_sales_total=db_sales_total.round(2)

            db_sales_total['roi'] = db_sales_total['roi'].map('{:.0%}'.format)
            db_sales_total['return %'] = db_sales_total['return %'].map('{:.0%}'.format)
            
            db_sales_total=db_sales_total.T
            
            db_sales_total1=db_sales_total.drop(['unit_customer_paid_amount','unit_taxes','unit_commission','unit_logistics','unit_settled_amount','unit_cost','unit_p/l','roi'])
            db_sales_total2=db_sales_total.drop(['total_orders','successful_orders','returns','customer_paid_amount','taxes','commission','logistics','settled_amount','cost','profit_value','return %'])
            
            table1,table2=st.columns(2)
            with table1 :
                 st.dataframe(db_sales_total1)
            with table2:
                 st.dataframe(db_sales_total2)

    st.dataframe(db_sales_summary)
except :
    st.write("PLease select attribute/s")
