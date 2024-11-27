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
        try:
            db_data=db_data[(db_data['order_created_date'].dt.date >= date_range[0]) & (db_data['order_created_date'].dt.date <=date_range[1] )]
        except: 
            db_data=db_data[(db_data['order_created_date'] >= date_range[0]) & (db_data['order_created_date'] <=date_range[1] )]

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
       st.write("something is wrong")


st.title ("Sales Overview")
db_data_final=db_data
db_sales_data_final=db_sales_data
total_channel=db_sales_data['channel_x'].unique().tolist()
total_channel.insert(0, "All")
tab=st.tabs(total_channel)
tab_len=len(total_channel)

for i in range(tab_len):
    with tab[i]:
        if i==0 :
            db_data_so=db_data_final
            db_sales_data_so=db_sales_data_final
        else :
            db_data_so=db_data_final[db_data_final['channel']==total_channel[i]]
            db_sales_data_so=db_sales_data_final[db_sales_data_final['channel_x']==total_channel[i]]
        


        
        container=st.container(border=True)
        container1=st.container(border=True)

        db_data_forward=db_data_so[db_data_so['order_type']=='Forward']
        db_data_reverse=db_data_so[db_data_so['order_type']=='Reverse']
        total_orders=len(db_sales_data_so)
        cancelled_orders=len(db_sales_data_so[db_sales_data_so['order_status']=='F'])
        shipped_orders=total_orders-cancelled_orders
        RTO=len(db_sales_data_so[db_sales_data_so['order_status']=='RTO'])
        in_transit=len(db_sales_data_so[db_sales_data_so['order_status'].isin({'SH','L','PK','WP'})])
        delivered=shipped_orders-RTO-in_transit
        total_returns=len(db_sales_data_so[db_sales_data_so['returns']==1])
        successful_order=delivered-total_returns
        db_sales_value=db_sales_data_so[db_sales_data_so['returns']==0]
        db_sales_value=db_sales_value[db_sales_value['order_status']=='C']
        total_sales_value=sum(db_sales_value['final_amount'])
        profit=sum(db_data_forward['customer_paid_amt'])


        with st.container(border=True) :
            
            col1,col2,col3,col4,col5,col6,col7,col8=st.columns(8,gap='small')
            with col1:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>Orders</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(total_orders))+'</p>', unsafe_allow_html=True)

            with col2:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>Cancelled</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(cancelled_orders))+'</p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+str(round(cancelled_orders/total_orders*100,2))+'%</p>', unsafe_allow_html=True)
                    
            with col3:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>Shipped</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(shipped_orders))+'</p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+str(round(shipped_orders/total_orders*100,2))+'%</p>', unsafe_allow_html=True)
            
            with col4:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>RTO</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(RTO))+'</p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+str(round(RTO/shipped_orders*100,2))+'%</p>', unsafe_allow_html=True)

            with col5:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>In Transit</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(in_transit))+'</p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+str(round(in_transit/shipped_orders*100,2))+'%</p>', unsafe_allow_html=True)

            
            with col6:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>Returns</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(total_returns))+'</p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+str(round(total_returns/shipped_orders*100,2))+'%</p>', unsafe_allow_html=True)

            with col7:
                with st.container(border=True):
                    st.markdown('<p class="box-font"><b>Net Sales</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(successful_order))+'</p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+str(round(successful_order/shipped_orders*100,2))+'%</p>', unsafe_allow_html=True)



            with col8:
                with st.container(border=True):
                    st.markdown('<p class="value-font"><b>Sales Value</b></p>', unsafe_allow_html=True)
                    st.markdown('<p class="value-font">'+('{:,}'.format(total_sales_value))+'</p>', unsafe_allow_html=True)

        with st.container(border=True) :
                st.subheader("Sales Trend (Shipped Orders)")
                # db_sales_data_daily=db_sales_data[db_sales_data['order_status']!='F']
                # db_sales_data_daily['order_created_date']=db_sales_data_daily['order_created_date'].dt.date
                # db_sales_data_daily['order_created_date']=db_sales_data_daily['order_created_date'].astype(str)
                # db_sales_data_daily['asp']=db_sales_data_daily['final_amount']
                # db_sales_data_daily=db_sales_data_daily.groupby(['order_created_date']).agg({'final_amount':'sum','asp':'mean','order_release_id':'count'})
                # db_sales_data_daily.reset_index(inplace=True)
                # db_sales_data_daily.set_index('order_created_date',inplace=True)
                # st.line_chart(data=db_sales_data_daily, x=None, y=['final_amount'], x_label='Date', y_label='Sales_Value', color=None, width=None, height=None, use_container_width=True)
                # st.bar_chart(data=db_sales_data_daily, x=None, y=['asp'], x_label='Date', y_label='ASP', color=None, width=None, height=None, use_container_width=True)
                
                db_sales_data_daily=db_sales_data_so[db_sales_data_so['order_status']!='F']
                db_sales_data_daily['order_created_date']=db_sales_data_daily['order_created_date'].dt.date
                db_sales_data_daily['asp']=db_sales_data_daily['final_amount']
                db_sales_data_daily=db_sales_data_daily.groupby(['order_created_date']).agg({'final_amount':'sum','asp':'mean','order_release_id':'count'})
                db_sales_data_daily.reset_index(inplace=True)
                db_sales_data_daily.set_index('order_created_date',inplace=True)
                


                fig, ax = plt.subplots(1, figsize=(15,5))
                ax_2 = ax.twinx()
                ax.set_xlabel('Date')
                # ax.set_xticklabels('Date',rotation = 90)
                ax.set_ylabel('Sales Value',color='darkturquoise')
                ax_2.set_ylabel('ASP',color='red')
                ax_2.plot(db_sales_data_daily.index, db_sales_data_daily['asp'],color='red')
                ax_2.tick_params(axis='y', labelcolor='red')
                ax.bar(db_sales_data_daily.index, db_sales_data_daily['final_amount'],color='darkturquoise')
                ax.tick_params(axis='y', labelcolor='darkturquoise')
                # fig.tight_layout()
                st.pyplot(fig)


        with st.container(border=True):
            tab1,tab2,tab3,tab4,tab5=st.tabs(['Category Contribution','Brand Contribution','Gender Contribution','State Distribution','Size Contribution'])




            with tab1 :
            
                st.subheader("Category Contribution")
                db_sales_category=db_sales_data_so[db_sales_data_so['order_status']!='F']
                db_sales_category=db_sales_category.groupby(['article_type']).agg({'final_amount':'sum'})
                db_sales_category.reset_index(inplace=True)

                fig=px.pie(db_sales_category,values='final_amount',names='article_type',title=None)
                # fig = go.Figure(data=[go.Pie(labels=db_sales_category['article_type'], values=db_sales_category['final_amount'])])
                st.plotly_chart(fig,use_container_width=True,key=1)        

            with tab2 :
            
                st.subheader("Brand Contribution")
                db_sales_brand=db_sales_data_so[db_sales_data_so['order_status']!='F']
                db_sales_brand=db_sales_brand.groupby(['brand']).agg({'final_amount':'sum'})
                db_sales_brand.reset_index(inplace=True)

                fig=px.pie(db_sales_brand,values='final_amount',names='brand',title=None)
                st.plotly_chart(fig,use_container_width=True)    


            with tab3 :
            
                st.subheader("Gender Contribution")
                db_sales_brand=db_sales_data_so[db_sales_data_so['order_status']!='F']
                db_sales_brand=db_sales_brand.groupby(['gender']).agg({'final_amount':'sum'})
                db_sales_brand.reset_index(inplace=True)

                fig=px.pie(db_sales_brand,values='final_amount',names='gender',title=None)
                st.plotly_chart(fig,use_container_width=True)          


                
            with tab4 :
                db_sales_latlong=db_sales_data_so[db_sales_data_so['order_status']!='F']
                db_sales_latlong=db_sales_latlong.groupby(['state']).agg({'final_amount':'sum'})
                db_sales_latlong.reset_index(inplace=True)
                size=db_sales_latlong['final_amount'].max()
                db_sales_latlong['amount']=db_sales_latlong['final_amount']/size*200000
                db_sales_latlong=db_sales_latlong.merge(db_latlong,left_on='state',right_on='state')
                st.map(db_sales_latlong, latitude='lat', longitude='lon',size='amount')
                
            with tab5 :
            
                st.subheader("Size Contribution")
                db_sales_brand=db_sales_data_so[db_sales_data_so['order_status']!='F']
                db_sales_brand=db_sales_brand.groupby(['size']).agg({'final_amount':'sum'})
                db_sales_brand.reset_index(inplace=True)

                fig=px.pie(db_sales_brand,values='final_amount',names='size',title=None)
                st.plotly_chart(fig,use_container_width=True)          

