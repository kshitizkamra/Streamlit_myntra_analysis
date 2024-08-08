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

conn = st.connection("my_database")
db_master=conn.query("select * from styles_master;")


db_sales=conn.query("select * from final_sales;")
db_settlement=conn.query("select * from final_settlement;")
db_sales_final=conn.query("select * from sales_data_sync;")


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



st.markdown("<h1 style='text-align: center; color: grey;'>Style Summary</h1>", unsafe_allow_html=True)

st.divider()

db_sales_summary=db_sales_bar1_final.groupby(['vendor style code','brand','gender','article type','fabric','collection name','image link']).agg({'total_orders':'sum','successful_orders':'sum','returns':'sum','customer_paid_amount':'sum','taxes':'sum','commission':'sum','logistics':'sum','settled_amount':'sum','cost':'sum'})
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




db_sales_summary.reset_index(inplace=True)


st.markdown(
    """
    <style>
    text-align:center
    </style>
    """,unsafe_allow_html=True
)


db_sales_summary=db_sales_summary.round(2)
db_sales_summary['roi'] = db_sales_summary['roi'].map('{:.0%}'.format)
db_sales_summary['return %'] = db_sales_summary['return %'].map('{:.0%}'.format)
db_sales_summary=db_sales_summary.sort_values('successful_orders',ascending=False)



db_sales_summary_1=db_sales_summary.copy()
db_sales_summary=db_sales_summary['vendor style code'].drop_duplicates()
search_style_code_list = db_sales_summary.values.tolist()
search_style_code = st.multiselect(
      "Search/Select Style Code",
      search_style_code_list,
      placeholder="Search/Select Style Code",
      label_visibility='collapsed'
    )
if len(search_style_code)>0 :
       db_sales_summary=db_sales_summary_1[(db_sales_summary_1['vendor style code'].isin(search_style_code))]
else :
        db_sales_summary=db_sales_summary_1.copy()
st.divider()



col1,col2=st.columns(2,gap='small')
pd.set_option('display.max_colwidth', None)

db_sales_summary=db_sales_summary.reset_index()
db_sales_summary.index += 1
db_sales_summary=db_sales_summary.drop(['index'],axis=1)

for index,rows in db_sales_summary.iterrows():
     db_style_summary=db_sales_summary.loc[index]
     db_style_summary=db_style_summary.drop(['gender','image link','unit_customer_paid_amount','unit_taxes','unit_commission','unit_logistics','unit_settled_amount','unit_cost','unit_p/l','roi','sales vol %','val %','return %'])
     db_style_unit_summary=db_sales_summary.loc[index]
     db_style_unit_summary=db_style_unit_summary.drop(['vendor style code','brand','article type','fabric','collection name','gender','image link','total_orders','successful_orders','returns','customer_paid_amount','taxes','commission','logistics','settled_amount','cost','profit_value'])
    


     if np.remainder(index,2)==1 :
          with col1 :
             with st.container(border=True):
               subcol1,subcol2,subcol3=st.columns([1.15,2,1.15])
               with subcol2 :
                   
                    st.image(db_sales_summary.loc[index,'image link'], width=250)
               sub_col1,sub_col2=st.columns(2)
               with sub_col1:
                    st.dataframe(db_style_summary)
               with sub_col2:
                    st.dataframe(db_style_unit_summary)

          

     
     else :
          with col2 :
              with st.container(border=True):
               subcol1,subcol2,subcol3=st.columns([1.15,2,1.15])
               with subcol2 :
                   
                    st.image(db_sales_summary.loc[index,'image link'], width=250)
               sub_col1,sub_col2=st.columns(2)
               with sub_col1:
                    st.dataframe(db_style_summary)
               with sub_col2:
                    st.dataframe(db_style_unit_summary)

          
