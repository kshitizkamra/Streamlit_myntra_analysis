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
    st.image("assets/logo.png",width=150)
with col3:
    col5,col6=st.columns([3,2])
    with col6:
            st.write("")
            st.write("")
            with st.popover("Sign In"):
                
                if check_password():
                    st.session_state["login_check"]=1

if st.session_state['login_check']==1 :
    tab_imp,tab_sync,tab_so,tab_pnl,tab_sr,tab_sa,tab_exp,=st.tabs(["Import_data","Sync_data","Sales Overview","P&L","Style_Review","Suggested Actions","Export_data"])
else :
    home,about_us=st.tabs(["Home","About Us"])

try :
    with about_us:
     st.write('<a href="/Sales_Overview" target="_self">Next page</a>', unsafe_allow_html=True)

except :
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
    
    with tab_so:
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
                        st.plotly_chart(fig,use_container_width=True)        

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

    with tab_pnl:
        st.title ("P&L Overview")
        
        db_data_final=db_data
        db_sales_data_final=db_sales_data
        

        total_channel=db_sales_data['channel_x'].unique().tolist()

        total_channel.insert(0, "All")
        tab=st.tabs(total_channel)
        tab_len=len(total_channel)

        for i in range(tab_len):
            with tab[i]:
                if i==0 :
                    db_data_pnl=db_data_final
                    db_sales_data_pnl=db_sales_data_final
                else :
                    db_data_pnl=db_data_final[db_data_final['channel']==total_channel[i]]
                    db_sales_data_pnl=db_sales_data_final[db_sales_data_final['channel_x']==total_channel[i]]
                


                container=st.container(border=True)
                container1=st.container(border=True)

                
                total_orders=len(db_sales_data_pnl)
                ordered_gmv=sum(db_sales_data_pnl['final_amount'])
                cancelled_orders=len(db_sales_data_pnl[db_sales_data_pnl['order_status']=='F'])
                cancelled_value=db_sales_data_pnl.loc[db_sales_data_pnl['order_status']=='F','final_amount'].sum()

                shipped_orders=total_orders-cancelled_orders
                shipped_value=ordered_gmv-cancelled_value
                RTO=len(db_sales_data_pnl[db_sales_data_pnl['order_status']=='RTO'])
                rto_value=db_sales_data_pnl.loc[db_sales_data_pnl['order_status']=='RTO','final_amount'].sum()
                in_transit=len(db_sales_data_pnl[db_sales_data_pnl['order_status'].isin({'SH','L','PK','WP'})])
                in_transit_value=db_sales_data_pnl.loc[db_sales_data_pnl['order_status'].isin({'SH','L','PK','WP'}),'final_amount'].sum()
                delivered=shipped_orders-RTO-in_transit
                delivered_value=shipped_value-in_transit_value-rto_value
                total_returns=len(db_sales_data_pnl[db_sales_data_pnl['returns']==1])
                returns_value=db_sales_data_pnl.loc[db_sales_data_pnl['returns']==1,'final_amount'].sum()
                successful_order=delivered-total_returns
                db_sales_value=db_sales_data_pnl[db_sales_data_pnl['returns']==0]
                db_sales_value=db_sales_value[db_sales_value['order_status']=='C']
                total_sales_value=sum(db_sales_value['final_amount'])
                
                settled_orders_value=db_data_pnl.loc[db_data_pnl['order_type']=='Forward','customer_paid_amt'].sum()
                settled_order_qty=db_data_pnl.loc[db_data_pnl['order_type']=='Forward','customer_paid_amt'].count()
                non_settled_orders_value=round(delivered_value-settled_orders_value,2)
                settled_orders_value_net_sales=round(db_data_pnl.loc[db_data_pnl['returns']==0,'customer_paid_amt'].sum(),2)
                net_sales_order_qty=round(db_data_pnl.loc[db_data_pnl['returns']==0,'customer_paid_amt'].count(),2)
                settled_orders_value_returns=round(settled_orders_value-settled_orders_value_net_sales,2)
                return_order_qty=round(settled_order_qty-net_sales_order_qty,2)
                
                non_settled_orders_count=len(db_sales_data[db_sales_data['order_status'].isin({'C'})])-db_data_pnl.loc[db_data_pnl['order_type']=='Forward','customer_paid_amt'].count()
                

            
                commission_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'platform_fees'].sum()
                commission_returns=0
                taxes_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'tcs_amount'].sum()+db_data_pnl.loc[db_data_pnl['returns']==0,'tds_amount'].sum()
                taxes_returns=0
                logistics_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'shipping_fee'].sum()+db_data_pnl.loc[db_data_pnl['returns']==0,'total_tax_on_logistics'].sum()
                logistics_returns=db_data_pnl.loc[db_data_pnl['returns']==1,'shipping_fee'].sum()+db_data_pnl.loc[db_data_pnl['returns']==1,'total_tax_on_logistics'].sum()
                pick_pack_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'pick_and_pack_fee'].sum()
                pick_pack_return=db_data_pnl.loc[db_data_pnl['returns']==1,'pick_and_pack_fee'].sum()
                payment_gateway_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'payment_gateway_fee'].sum()
                payment_gateway_return=db_data_pnl.loc[db_data_pnl['returns']==1,'payment_gateway_fee'].sum()
                fixed_fee_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'fixed_fee'].sum()
                fixed_fee_return=db_data_pnl.loc[db_data_pnl['returns']==1,'fixed_fee'].sum()
                settlement_net_sales=settled_orders_value_net_sales-commission_net_sales-taxes_net_sales-logistics_net_sales-fixed_fee_net_sales-pick_pack_net_sales-payment_gateway_net_sales
                settlement_return=0-logistics_returns-fixed_fee_return-pick_pack_return-payment_gateway_return
                cogs_net_sales=db_data_pnl.loc[db_data_pnl['returns']==0,'cost'].sum()
                cogs_return=0
                pnl_net_sales=settlement_net_sales-cogs_net_sales
                pnl_return=settlement_return
                pnl_total=pnl_net_sales+pnl_return
                returns_data = [['Order_qty', '{:,}'.format(round(return_order_qty,2))],['Comission', '{:,}'.format(round(commission_returns,2))], ['Taxes', '{:,}'.format(round(taxes_returns,2))], ['Logistics', '{:,}'.format(round(logistics_returns,2))], ['Fixed-Fee', '{:,}'.format(round(fixed_fee_return,2))], ['Pick-Pack Fee', '{:,}'.format(round(pick_pack_return,2))], ['Pay Gate Fee', '{:,}'.format(round(payment_gateway_return,2))], ['Settlement', '{:,}'.format(round(settlement_return,2))], ['COGS', '{:,}'.format(round(cogs_return,2))], ['P/L', '{:,}'.format(round(pnl_return,2))]]
                returns_dataframe=pd.DataFrame(returns_data, columns=['Metric', 'Value'])
                net_sales_data = [['Order_qty', '{:,}'.format(round(net_sales_order_qty,2))],['Comission', '{:,}'.format(round(commission_net_sales,2))], ['Taxes', '{:,}'.format(round(taxes_net_sales,2))], ['Logistics', '{:,}'.format(round(logistics_net_sales,2))], ['Fixed-Fee', '{:,}'.format(round(fixed_fee_net_sales,2))], ['Pick-Pack Fee', '{:,}'.format(round(pick_pack_net_sales,2))], ['Pay Gate Fee', '{:,}'.format(round(payment_gateway_net_sales,2))], ['Settlement', '{:,}'.format(round(settlement_net_sales,2))], ['COGS', '{:,}'.format(round(cogs_net_sales,2))], ['P/L', '{:,}'.format(round(pnl_net_sales,2))]]
                net_sales_dataframe=pd.DataFrame(net_sales_data, columns=['Metric', 'Value'])



                estimated_settled_orders_value_net_sales=settled_orders_value_net_sales/settled_orders_value*non_settled_orders_value
                estimated_settled_orders_value_returns=settled_orders_value_returns/settled_orders_value*non_settled_orders_value
                estimated_settled_orders_count=round(net_sales_order_qty/settled_order_qty*non_settled_orders_count,0)
                estimated_settled_orders_count_returns=non_settled_orders_count-estimated_settled_orders_count

                estimated_commission_net_sales=commission_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_commission_returns=0
                estimated_taxes_net_sales=taxes_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_taxes_returns=0
                estimated_logistics_net_sales=logistics_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_logistics_returns=logistics_returns/settled_orders_value_returns*estimated_settled_orders_value_returns
                estimated_pick_pack_net_sales=pick_pack_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_pick_pack_return=pick_pack_return/settled_orders_value_returns*estimated_settled_orders_value_returns
                estimated_payment_gateway_net_sales=payment_gateway_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_payment_gateway_return=payment_gateway_return/settled_orders_value_returns*estimated_settled_orders_value_returns
                estimated_fixed_fee_net_sales=fixed_fee_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_fixed_fee_return=fixed_fee_return/settled_orders_value_returns*estimated_settled_orders_value_returns
                estimated_settlement_net_sales=settlement_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_settlement_return=settlement_return/settled_orders_value_returns*estimated_settled_orders_value_returns
                estimated_cogs_net_sales=cogs_net_sales/settled_orders_value_net_sales*estimated_settled_orders_value_net_sales
                estimated_cogs_return=0
                estimated_pnl_net_sales=estimated_settlement_net_sales-estimated_cogs_net_sales
                estimated_pnl_return=estimated_settlement_return
                estimated_pnl_total=estimated_pnl_net_sales+estimated_pnl_return
                estimated_returns_data = [['Order_qty', '{:,}'.format(estimated_settled_orders_count_returns)],['Comission', '{:,}'.format(round(estimated_commission_returns,2))], ['Taxes', '{:,}'.format(round(estimated_taxes_returns,2))], ['Logistics', '{:,}'.format(round(estimated_logistics_returns,2))], ['Fixed-Fee', '{:,}'.format(round(estimated_fixed_fee_return,2))], ['Pick-Pack Fee', '{:,}'.format(round(estimated_pick_pack_return,2))], ['Pay Gate Fee', '{:,}'.format(round(estimated_payment_gateway_return,2))], ['Settlement', '{:,}'.format(round(estimated_settlement_return,2))], ['COGS', '{:,}'.format(round(estimated_cogs_return,2))], ['P/L', '{:,}'.format(round(estimated_pnl_return,2))]]
                estimated_returns_dataframe=pd.DataFrame(estimated_returns_data, columns=['Metric', 'Value'])
                estimated_net_sales_data = [['Order_qty', '{:,}'.format(estimated_settled_orders_count)],['Comission', '{:,}'.format(round(estimated_commission_net_sales,2))], ['Taxes', '{:,}'.format(round(estimated_taxes_net_sales,2))], ['Logistics', '{:,}'.format(round(estimated_logistics_net_sales,2))], ['Fixed-Fee', '{:,}'.format(round(estimated_fixed_fee_net_sales,2))], ['Pick-Pack Fee', '{:,}'.format(round(estimated_pick_pack_net_sales,2))], ['Pay Gate Fee', '{:,}'.format(round(estimated_payment_gateway_net_sales,2))], ['Settlement', '{:,}'.format(round(estimated_settlement_net_sales,2))], ['COGS', '{:,}'.format(round(estimated_cogs_net_sales,2))], ['P/L', '{:,}'.format(round(estimated_pnl_net_sales,2))]]
                estimated_net_sales_dataframe=pd.DataFrame(estimated_net_sales_data, columns=['Metric', 'Value'])





                with st.container(border=True) :
                    
                    col1,col2,col3,col4,col5,col6=st.columns(6,gap='small')
                    with col1:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Ordered Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(ordered_gmv))+'</p>', unsafe_allow_html=True)

                    with col2:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Cancelled Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(cancelled_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(cancelled_value/ordered_gmv*100,2))+'%</p>', unsafe_allow_html=True)
                            
                    with col3:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Shipped Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(shipped_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(shipped_value/ordered_gmv*100,2))+'%</p>', unsafe_allow_html=True)
                    
                    with col4:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>RTO Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(rto_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(rto_value/shipped_value*100,2))+'%</p>', unsafe_allow_html=True)

                    with col5:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>In Transit Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(in_transit_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(in_transit_value/shipped_value*100,2))+'%</p>', unsafe_allow_html=True)

                    
                    with col6:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Delivered Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(delivered_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(delivered_value/shipped_value*100,2))+'%</p>', unsafe_allow_html=True)

                st.markdown("")
                st.markdown("")

                with st.container(border=True):
                    
                    with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Delivered Value</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(delivered_value))+'</p>', unsafe_allow_html=True)
                            
                    plcol1,plcol2=st.columns(2,gap='small')
                    with plcol1:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Settled GMV</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(settled_orders_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(settled_orders_value/delivered_value*100,2))+'%</p>', unsafe_allow_html=True)
                        subplcol1,subplcol2=st.columns(2,gap='small')
                        with subplcol1:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Return GMV</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(settled_orders_value_returns))+'</p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+str(round(settled_orders_value_returns/settled_orders_value*100,2))+'%</p>', unsafe_allow_html=True)  
                            
                            with st.container(border=True):
                                    st.markdown(returns_dataframe.style.hide(axis="index").to_html(), unsafe_allow_html=True)
                            
                        with subplcol2:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Net Sales GMV</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(settled_orders_value_net_sales,2)))+'</p>', unsafe_allow_html=True)  
                                st.markdown('<p class="value-font">'+str(round(settled_orders_value_net_sales/settled_orders_value*100,2))+'%</p>', unsafe_allow_html=True)
                            with st.container(border=True):
                                    st.markdown(net_sales_dataframe.style.hide(axis="index").to_html(), unsafe_allow_html=True)

                        with st.container(border=True):
                                subplcol1,subplcol2,subplcol3=st.columns(3,gap='small')
                                with subplcol1:
                                    st.markdown('<p class="value-font"><b>Settlement</b></p>', unsafe_allow_html=True)
                                    st.markdown('<p class="value-font">'+('{:,}'.format(round(settlement_net_sales+settlement_return,2)))+'</p>', unsafe_allow_html=True)
                                

                                with subplcol2:
                                    st.markdown('<p class="value-font"><b>P/L</b></p>', unsafe_allow_html=True)
                                    st.markdown('<p class="value-font">'+('{:,}'.format(round(pnl_total,2)))+'</p>', unsafe_allow_html=True)
                                
                                with subplcol3:
                                    st.markdown('<p class="value-font"><b>ROI</b></p>', unsafe_allow_html=True)
                                    st.markdown('<p class="value-font">'+str(round(pnl_total/cogs_net_sales*100,2))+'%</p>', unsafe_allow_html=True)  
                            
                    with plcol2:
                        with st.container(border=True):
                            st.markdown('<p class="value-font"><b>Non-Settled GMV</b></p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+('{:,}'.format(non_settled_orders_value))+'</p>', unsafe_allow_html=True)
                            st.markdown('<p class="value-font">'+str(round(non_settled_orders_value/delivered_value*100,2))+'%</p>', unsafe_allow_html=True)

                        subplcol1,subplcol2=st.columns(2,gap='small')
                        with subplcol1:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Estimated Return GMV</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(settled_orders_value_returns/settled_orders_value*non_settled_orders_value,2)))+'</p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">Actuals may vary</p>', unsafe_allow_html=True)
                            with st.container(border=True):
                                    st.markdown(estimated_returns_dataframe.style.hide(axis="index").to_html(), unsafe_allow_html=True)
                            
                        with subplcol2:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Estimated Net Sales GMV</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(settled_orders_value_net_sales/settled_orders_value*non_settled_orders_value,2)))+'</p>', unsafe_allow_html=True)  
                                st.markdown('<p class="value-font">Actuals may vary</p>', unsafe_allow_html=True)
                            with st.container(border=True):
                                    st.markdown(estimated_net_sales_dataframe.style.hide(axis="index").to_html(), unsafe_allow_html=True)

                        with st.container(border=True):
                                subplcol1,subplcol2=st.columns(2,gap='small')
                                with subplcol1:
                                    st.markdown('<p class="value-font"><b>Estimated Settlement</b></p>', unsafe_allow_html=True)
                                    st.markdown('<p class="value-font">'+('{:,}'.format(round(estimated_settlement_net_sales+estimated_settlement_return,2)))+'</p>', unsafe_allow_html=True)
                                
                                with subplcol2:
                                    st.markdown('<p class="value-font"><b>Estimated P/L</b></p>', unsafe_allow_html=True)
                                    st.markdown('<p class="value-font">'+('{:,}'.format(round(estimated_pnl_total,2)))+'</p>', unsafe_allow_html=True)
                                
                # with st.container(border=True):
                #     st.subheader("ASP-Settlement Co-relation")
                #     db_data_scatter=db_data
                #     db_data_scatter['order_created_date']=db_data_scatter['order_created_date'].dt.date
                #     db_data_scatter['order_created_date']=db_data_scatter['order_created_date'].astype(str)
                #     db_data_scatter=db_data_scatter.groupby(['order_created_date']).agg({'total_actual_settlement':'sum','customer_paid_amt':'mean'}) 
                #     db_data_scatter.reset_index(inplace=True)

                #     c = np.corrcoef(db_data_scatter['customer_paid_amt'],db_data_scatter['total_actual_settlement'])
                #     fig = px.imshow(c, text_auto=True,x=['ASP','Settlement_Amount'],y=['ASP','Settlement_Amount'])
                #     st.plotly_chart(fig, theme="streamlit")

                with st.container(border=True):
                    db_data_monthly=db_data_pnl[['order_created_date','order_type','customer_paid_amt','total_actual_settlement','platform_fees','tcs_amount','tds_amount','shipping_fee','pick_and_pack_fee','fixed_fee','payment_gateway_fee','total_tax_on_logistics','returns','cost']]
                    db_data_monthly.loc[db_data_monthly['returns']==1,'platform_fees']=0
                    db_data_monthly.loc[db_data_monthly['returns']==1,'tcs_amount']=0
                    db_data_monthly.loc[db_data_monthly['returns']==1,'tds_amount']=0
                    db_data_monthly['final_amount']=db_data_monthly['customer_paid_amt']
                    db_data_monthly.loc[db_data_monthly['order_type']=='Reverse','final_amount']=0
                    db_data_monthly.loc[db_data_monthly['returns']==1,'customer_paid_amt']=0
                    db_data_monthly.loc[db_data_monthly['returns']==1,'cost']=0
                    
                    db_data_monthly['settlement']=db_data_monthly['customer_paid_amt']-db_data_monthly['platform_fees']-db_data_monthly['tcs_amount']-db_data_monthly['tds_amount']-db_data_monthly['shipping_fee']-db_data_monthly['pick_and_pack_fee']-db_data_monthly['fixed_fee']-db_data_monthly['payment_gateway_fee']-db_data_monthly['total_tax_on_logistics']
                    db_data_monthly['month']=pd.to_datetime(db_data_monthly['order_created_date']).dt.strftime('%b-%y')
                    db_data_monthly.sort_values(by=['order_created_date'],inplace=True)
                    
                    db_data_monthly.drop(['returns','total_actual_settlement','order_created_date','total_actual_settlement','platform_fees','tcs_amount','tds_amount','shipping_fee','pick_and_pack_fee','fixed_fee','payment_gateway_fee','total_tax_on_logistics'],inplace=True,axis=1)
                    
                    db_data_monthly=db_data_monthly.groupby(['month'], sort=False, as_index=False).agg({'final_amount':'sum','customer_paid_amt':'sum','settlement':'sum','cost':'sum'})
                    
                    db_data_monthly['P/L']=db_data_monthly['settlement']-db_data_monthly['cost']
                    
                    db_data_monthly.rename(columns={'final_amount':'Delivered_Value','customer_paid_amt':'Net_Sales_Value'},inplace=True)
                    fig = px.line()
                    #    update_traces  
                    fig.add_scatter(x=db_data_monthly['month'], y=db_data_monthly['Delivered_Value'],name="Settled Sales Value")
                    fig.add_scatter(x=db_data_monthly['month'], y=db_data_monthly['Net_Sales_Value'],name="Net Sales_Value")
                    fig.add_scatter(x=db_data_monthly['month'], y=db_data_monthly['settlement'],name="Settlement")
                    fig.add_scatter(x=db_data_monthly['month'], y=db_data_monthly['cost'],name="COGS")
                    fig.add_scatter(x=db_data_monthly['month'], y=db_data_monthly['P/L'],name="P/L")
                    st.plotly_chart(fig, theme="streamlit")

                    #    fig = px.line(df, x="x", y="y", title="Sorted Input") 
                    #    st.plotly_chart(fig, theme="streamlit")

    with tab_sr:
        
        db_data_sr=db_data
        db_sales_data_sr=db_sales_data
        

        db_data_forward=db_data_sr[db_data_sr['order_type']=='Forward']
        # db_data_reverse=db_data[db_data['order_type']=='Reverse']
        # db_sales_data=db_sales_data[db_sales_data['order_status']=='C']

        # db_vendor_style_code=db_data[['vendor_style_code','image_link','brand','gender','article_type','color','fabric','cost']]
        # db_vendor_style_code=db_vendor_style_code.drop_duplicates()

        db_search_style_code=db_sales_data_sr['vendor_style_code'].drop_duplicates()
        search_style_code_list = db_search_style_code.values.tolist()
        search_style_code = st.multiselect(
            "Search/Select Style Code",
            search_style_code_list,
            placeholder="Search/Select Style Code",
            label_visibility='collapsed'
            )
        if len(search_style_code)>0 :
            db_style_code=db_data_sr[(db_data_sr['vendor_style_code'].isin(search_style_code))]
        
            st.session_state['page_number'] = 1
        else :
            db_style_code=db_data_sr
        
        
        db_style_code['order_count']=0
        db_style_code.loc[db_style_code['order_type']=='Forward','order_count']=1
        db_style_code.loc[db_style_code['returns']==1,'cost']=0
        db_style_code.loc[db_style_code['returns']==1,'customer_paid_amt']=0
        db_style_code.loc[db_style_code['returns']==1,'platform_fees']=0
        db_style_code.loc[db_style_code['returns']==1,'tcs_amount']=0
        db_style_code.loc[db_style_code['returns']==1,'tds_amount']=0
        db_style_code['return_count']=0
        db_style_code.loc[(db_style_code['returns']==1)&(db_style_code['order_type']=='Forward'),'return_count']=1
        db_style_code['month']=pd.to_datetime(db_style_code['order_created_date']).dt.strftime('%b-%y')
        db_style_code_funnel=db_style_code
        
        db_style_code['settlement']=db_style_code['customer_paid_amt']-db_style_code['platform_fees']-db_style_code['tcs_amount']-db_style_code['tds_amount']-db_style_code['shipping_fee']-db_style_code['pick_and_pack_fee']-db_style_code['fixed_fee']-db_style_code['payment_gateway_fee']-db_style_code['total_tax_on_logistics']
        db_style_code.sort_values(by=['order_created_date'],inplace=True)
        db_style_code=db_style_code.fillna("NA")
        
        db_style_code=db_style_code.groupby(['state','order_type','channel','vendor_style_code','order_created_date','brand','gender','article_type','image_link','color','fabric','collection','month','size'], as_index=False,sort=False).agg({'returns':'sum','customer_paid_amt':'sum','order_count':'sum','settlement':'sum','return_count':'sum','cost':'sum'})
        # db_style_code1
        db_style_code['p/l']=db_style_code['settlement']-db_style_code['cost']
        
        db_style_code=db_style_code.merge(db_latlong,left_on='state',right_on='state')
        
        # db_style_code['order_count']=db_style_code['vendor_style_code'].map(db_data_forward['vendor_style_code'].value_counts())
        db_style_code_for_sequence=db_style_code.groupby(['vendor_style_code'],as_index=False,sort=False).agg({'order_count':'sum'})

        db_style_code_for_sequence=db_style_code_for_sequence.sort_values('order_count',ascending=False)
        db_style_code_for_sequence.reset_index(inplace=True)
        db_style_code_for_sequence.drop(['index'],axis=1,inplace=True)
        db_style_code_for_sequence.index +=1
        db_style_code_for_sequence.fillna(0,inplace=True)


        # db_sales_data=db_sales_data.drop(['order_release_id','sku_code_x','channel_y','sku_code_y','vendor_sku_code'],axis=1)
        # db_sales_data['order_count']=1
        total_pages=len(db_style_code_for_sequence)

        # db_style_code_for_sequence

        if 'page_number' not in st.session_state:
            st.session_state['page_number'] = 1
            
        else:
            page_number = st.session_state['page_number']
            

        with st.container(border=True) :
            
            col1,col2=st.columns([4,1],gap="small")
            with col2 :
                subcol1,subcol2,subcol3,subcol4=st.columns([2,3,3,2],gap='small')

                with subcol1 :
                    if st.button("⬅️", use_container_width=True):
                        st.session_state['page_number'] -= 1 
                    if st.session_state['page_number']==0 :
                        st.session_state['page_number']=total_pages
                with subcol4 :
                    if st.button("➡️", use_container_width=True):
                        st.session_state['page_number'] += 1 
                    if st.session_state['page_number']==total_pages+1 :
                        st.session_state['page_number']=1
                with subcol2:
                    page_number = st.number_input("",value=st.session_state['page_number'],min_value=1, max_value=total_pages,label_visibility='collapsed')
                    st.session_state['page_number']=page_number
                with subcol3:
                
                    st.write("/"+str(total_pages))    
        
        
            with col1 :
            
                db_style_code_display_final=db_style_code[db_style_code['vendor_style_code']==db_style_code_for_sequence.loc[st.session_state['page_number'],'vendor_style_code']]
                db_style_code_display_final.reset_index(inplace=True)
            
            # db_style_code_display_final['vendor_style_code'][0]
            
            #   st.text()
            total_channel=db_style_code_display_final['channel'].unique().tolist()

            total_channel.insert(0, "All")
            tab=st.tabs(total_channel)
            tab_len=len(total_channel)

            
            for i in range(tab_len):
                    with tab[i]:
                        if i==0 :
                            db_style_code_display=db_style_code_display_final
                    # db_sales_data=db_sales_data_final
                        else :
                            db_style_code_display=db_style_code_display_final[db_style_code_display_final['channel']==total_channel[i]]
                    # db_sales_data=db_sales_data_final[db_sales_data_final['channel_x']==total_channel[i]]  
                    
                    
                        col3,col4=st.columns([0.85,2],gap="small")
                    with col3:
                        with st.container(border=True):
                            try:
                                st.image(db_style_code_display['image_link'][0], width=300)
                                st.header(db_style_code_display['vendor_style_code'][0])
                            except: 
                                st.text("Please check image url")
                            
                    with col4:
                        subcol1,subcol2,subcol3,subcol4,subcol5=st.columns(5,gap="small")
                        orders=db_style_code_display['order_count'].sum()
                        returns=db_style_code_display['return_count'].sum()
                        db_style_net_sales=orders-returns
                        value=db_style_code_display['customer_paid_amt'].sum()
                        #   db_style_data['order_count']=1
                    
                        with subcol1:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Orders</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(orders),0))+'</p>', unsafe_allow_html=True)
                            
                        with subcol2:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Returns</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(returns),0))+'</p>', unsafe_allow_html=True)

                        with subcol3:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Net Orders</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(orders-returns),0))+'</p>', unsafe_allow_html=True)

                        with subcol4:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>Value</b></p>', unsafe_allow_html=True)
                                st.markdown('<p class="value-font">'+('{:,}'.format(round(value),0))+'</p>', unsafe_allow_html=True)

                        with subcol5:
                            with st.container(border=True):
                                st.markdown('<p class="value-font"><b>ASP</b></p>', unsafe_allow_html=True)
                                try:
                                    st.markdown('<p class="value-font">'+('{:,}'.format(round(value/(orders-returns)),0))+'</p>', unsafe_allow_html=True)
                                except:
                                    st.markdown('<p class="value-font"> 0 </p>', unsafe_allow_html=True)

                        
                        if i==0:
                            tab1,tab2,tab3,tab4,tab5=st.tabs(['Orders-return','p/l','State Distribution','Size Contribution','Channel Contribution'])
                        else :
                            tab1,tab2,tab3,tab4=st.tabs(['Orders-return','p/l','State Distribution','Size Contribution'])

                        with tab1:
                            db_style_code_display_tab=db_style_code_display.groupby(['month'], as_index=False,sort=False).agg({'order_count':'sum','return_count':'sum'})
                            db_style_code_display_tab['net_order_count']=db_style_code_display_tab['order_count']-db_style_code_display_tab['return_count']
                            
                            fig = px.line(height=320)
                            
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['order_count'],name="Total orders")
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['return_count'],name="Returns")
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['net_order_count'],name="Net Sales")
                            with st.container(height=355):
                                st.plotly_chart(fig, theme="streamlit")

                        with tab2:
                            db_style_code_display_tab=db_style_code_display.groupby(['month'], as_index=False,sort=False).agg({'customer_paid_amt':'sum','settlement':'sum','cost':'sum','p/l':'sum'})
                            
                            fig = px.line(height=320)
                            
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['customer_paid_amt'],name="Net Sales")
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['settlement'],name="Settlement Amount")
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['cost'],name="COGS")
                            fig.add_scatter(x=db_style_code_display_tab['month'], y=db_style_code_display_tab['p/l'],name="P/L")
                            with st.container(height=355):
                                st.plotly_chart(fig, theme="streamlit")

                        with tab3:
                            db_style_code_display_tab=db_style_code_display.groupby(['latitude','longitude','state'], as_index=False,sort=False).agg({'order_count':'sum','return_count':'sum'})
                            db_style_code_display_tab['net_order_count']=db_style_code_display_tab['order_count']-db_style_code_display_tab['return_count']
                            size=db_style_code_display_tab['net_order_count'].max()
                            db_style_code_display_tab['amount']=db_style_code_display_tab['net_order_count']/size*200000
                            with st.container(height=355):
                                st.map(db_style_code_display_tab, latitude='latitude', longitude='longitude',size='amount')
                            # fig = px.scatter_geo(db_style_code_display_tab, lat='latitude', lon='longitude',hover_name='state', size='amount',title=None,color='net_order_count')
                            
                            # fig.show()

                        with tab4:
                            db_style_code_display_tab=db_style_code_display.groupby(['size'], as_index=False,sort=False).agg({'order_count':'sum','return_count':'sum'})
                            db_style_code_display_tab['net_order_count']=db_style_code_display_tab['order_count']-db_style_code_display_tab['return_count']
                            
                            
                            fig=px.pie(db_style_code_display_tab,values='net_order_count',names='size',title=None,height=320)
                            with st.container(height=355):
                                st.plotly_chart(fig,use_container_width=True)  

                        if i==0:
                            with tab5:
                                db_style_code_display_tab=db_style_code_display.groupby(['channel'], as_index=False,sort=False).agg({'order_count':'sum','return_count':'sum'})
                                db_style_code_display_tab['net_order_count']=db_style_code_display_tab['order_count']-db_style_code_display_tab['return_count']
                            
                            
                                fig=px.pie(db_style_code_display_tab,values='net_order_count',names='channel',title=None,height=320)
                                with st.container(height=355):
                                    st.plotly_chart(fig,use_container_width=True)
                        else:
                            ""


        with st.container(border=True):
            st.header ("Suggested Actions")
            # db_style_code_display
            db_style_code_actions=db_style_code_display.groupby(['vendor_style_code','channel','brand','gender','article_type'],as_index=False).agg({'order_created_date':'min','order_count':'sum','return_count':'sum','cost':'sum','p/l':'sum'})
            db_style_code_actions['return %']=db_style_code_actions['return_count']/db_style_code_actions['order_count']
            db_style_code_actions['roi']=db_style_code_actions['p/l']/db_style_code_actions['cost']
            db_style_code_actions['ros']=(db_style_code_actions['order_count'])/(pd.to_datetime(datetime.date.today(),format='ISO8601')-db_style_code_actions['order_created_date']).dt.days
            
            db_styles_action=conn.query("select * from actions_upload;")
            db_actual_action=conn.query("select * from recommendation_upload")
            
            selling_price_list=conn.query("select distinct selling_price from recommendation_upload")
            selling_price_list=selling_price_list['selling_price'].values.tolist()
            pla_list=conn.query("select distinct pla from recommendation_upload")
            pla_list=pla_list['pla'].values.tolist()
            replenishment_list=conn.query("select distinct replenishment from recommendation_upload")
            replenishment_list=replenishment_list['replenishment'].values.tolist()
            total_channel=db_style_code_display_final['channel'].unique().tolist()
            tab_suggestion=st.tabs(total_channel)
            tab_len=len(total_channel)

            
            
            
            

            def action_update(stmt) :
                with engine.connect() as conn:
                                    transaction = conn.begin()
                                    result1=conn.execute(stmt)
                                    transaction.commit()
                                    return result1.rowcount
            
            for i in range(tab_len):
                with tab_suggestion[i]:
                        

                        db_style_code_actions_tab=db_style_code_actions[db_styles_action['channel']==total_channel[i]]
                        
                        db_styles_action_tab=db_styles_action[(db_styles_action['brand']==db_style_code_display.loc[0,'brand'])&(db_styles_action['gender']==db_style_code_display['gender'][0])&(db_styles_action['article_type']==db_style_code_display['article_type'][0])]
                        
                        if db_style_code_actions_tab['ros'].sum()>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='ros','a'].sum() :
                            ros_action="A"
                        elif db_style_code_actions_tab['ros'].sum()>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='ros','b'].sum() :
                            ros_action="B"
                        else :
                            ros_action="C"
                        
                        if db_style_code_actions_tab['roi'].sum()>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='roi','a'].sum() :
                            roi_action="A"
                        elif db_style_code_actions_tab['roi'].sum()>=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='roi','b'].sum() :
                            roi_action="B"
                        else :
                            roi_action="C"

                        if db_style_code_actions_tab['return %'].sum()<=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='return %','a'].sum() :
                            return_action="A"
                        elif db_style_code_actions_tab['return %'].sum()<=db_styles_action_tab.loc[db_styles_action_tab['metrics']=='return %','b'].sum() :
                            return_action="B"
                        else :
                            return_action="C"
                        # db_sales_data.loc[db_sales_data['order_status']=='F','final_amount'].sum()
                        # ros_action
                        # roi_action
                        # return_action
                        # db_styles_action_tab
                        
                        db_actual_action_final=db_actual_action[(db_actual_action['ros']==ros_action)&(db_actual_action['roi']==roi_action)&(db_actual_action['return %']==return_action)]  
                        
                        db_actual_action_final.reset_index(inplace=True)
                        
                        
                        db_style_code_actions_tab
                        with st.container(border=True):
                            subcolm1,subcolm2,subcol3=st.columns([1,3,1])    
                            with subcolm1:
                                st.markdown('<p class="small-font">Selling Price</p>', unsafe_allow_html=True)
                            with subcolm2:
                                action_sp=st.selectbox("Select",selling_price_list,index=selling_price_list.index(db_actual_action_final.loc[0,'selling_price']),label_visibility='collapsed')
                                
                        with st.container(border=True):
                            subcolm1,subcolm2,subcol3=st.columns([1,3,1])
                            with subcolm1:
                                st.markdown('<p class="small-font">PLA</p>', unsafe_allow_html=True)
                            with subcolm2:
                                
                                action_pla=st.selectbox("Select",pla_list,index=pla_list.index(db_actual_action_final.loc[0,'pla']),label_visibility='collapsed')   
                        
                        with st.container(border=True):
                            subcolm1,subcolm2,subcol3=st.columns([1,3,1])
                            with subcolm1:
                                st.markdown('<p class="small-font">Replenishment</p>', unsafe_allow_html=True)
                            with subcolm2:
                                
                                action_replenishment=st.selectbox("Select",replenishment_list,index=replenishment_list.index(db_actual_action_final.loc[0,'replenishment']),label_visibility='collapsed')   

                        with st.container(border=True):
                            subcolm1,subcolm2,subcol3=st.columns([1,3,1])
                            with subcolm1:
                                st.markdown('<p class="small-font">Remarks</p>', unsafe_allow_html=True)
                            with subcolm2:
                                if db_actual_action_final.loc[0,'remarks']==None:
                                    db_actual_action_final.loc[0,'remarks']="no remarks"
                                action_remarks=st.text_input("textbox",value=db_actual_action_final.loc[0,'remarks'],max_chars=None,label_visibility='collapsed')
                                # action_replenishment=st.selectbox("Select",replenishment_list,index=replenishment_list.index(db_actual_action_final.loc[0,'replenishment']),label_visibility='collapsed')   

                        

                        brand=db_style_code_actions_tab['brand'][0]
                        gender=db_style_code_actions_tab['gender'][0]
                        article_type=db_style_code_actions_tab['article_type'][0]
                        channel=db_style_code_actions_tab['channel'][0]
                        ros=db_style_code_actions_tab['ros'][0]
                        roi=db_style_code_actions_tab['roi'][0]
                        returns=db_style_code_actions_tab['return %'][0]
                        if st.button("Accept Actions ✅"):
                            
                                
                            
                                style_code=db_style_code_actions_tab['vendor_style_code'][0]
                                
                                stmt_sp=text("Update action_items_manual set selling_price='"+action_sp+"',pla='"+action_pla+"',replenishment='"+action_replenishment+"',date_updated=now(),ros="+str(ros)+",roi="+str(roi)+",returns="+str(returns)+",ros_action='"+ros_action+"',roi_action='"+roi_action+"',return_action='"+return_action+"',remarks='"+action_remarks+"' where vendor_style_code='"+style_code+"' and channel='"+channel+"'")
                                
                                action_check=action_update(stmt_sp)
                                    
                                if action_check==0:
                                    
                                    stmt_sp=text("Insert into action_items_manual (vendor_style_code,selling_price,pla,replenishment,date_updated,brand,gender,article_type,ros,returns,roi,ros_action,roi_action,return_action,channel,remarks) values  ('"+style_code+"','"+action_sp+"','"+action_pla+"','"+action_replenishment+"',now(),'"+brand+"','"+gender+"','"+article_type+"',"+str(ros)+","+str(returns)+","+str(roi)+",'"+ros_action+"','"+roi_action+"','"+return_action+"','"+channel+"','"+action_remarks+"')") 
                                    
                                    action_check=action_update(stmt_sp)     

                        


        with st.container(border=True):
            st.header ("CODB")
                
            db_style_code_funnel_final=db_style_code_funnel[db_style_code_funnel['vendor_style_code']==db_style_code_for_sequence.loc[st.session_state['page_number'],'vendor_style_code']]
            db_style_code_funnel_final.reset_index(inplace=True)
            # db_style_code_funnel_final
            db_style_code_funnel_final['taxes']=db_style_code_funnel_final['tcs_amount']+db_style_code_funnel_final['tds_amount']
            db_style_code_funnel_final['settlement']=db_style_code_funnel_final['customer_paid_amt']-db_style_code_funnel_final['platform_fees']-db_style_code_funnel_final['taxes']-db_style_code_funnel_final['total_logistics']
            db_style_code_funnel_final=db_style_code_funnel_final.groupby(['channel'],as_index=False).agg({'customer_paid_amt':'sum','platform_fees':'sum','taxes':'sum','total_logistics':'sum','settlement':'sum','cost':'sum'})
            db_style_code_funnel_final['p/l']=db_style_code_funnel_final['settlement']-db_style_code_funnel_final['cost']



            funnel_channel_list=db_style_code_funnel_final['channel'].unique().tolist()
            funnel_channel_len=len(funnel_channel_list)
            db_style_code_funnel_final_funnel=pd.DataFrame()
            for i in range(funnel_channel_len):
                db_style_code_funnel_final_1=db_style_code_funnel_final[db_style_code_funnel_final['channel']==funnel_channel_list[i] ]
                db_style_code_funnel_final_1.drop(['channel'],inplace=True,axis=1)
                
                db_style_code_funnel_final_1_T=db_style_code_funnel_final_1.T
                db_style_code_funnel_final_1_T.reset_index(inplace=True)
                db_style_code_funnel_final_1_T.rename(columns = {'index':'metric',0:'value'},inplace=True)
                db_style_code_funnel_final_1_T['channel']=funnel_channel_list[i]
                db_style_code_funnel_final_funnel=pd.concat([db_style_code_funnel_final_funnel, db_style_code_funnel_final_1_T], ignore_index=True, sort=False)

            db_style_code_funnel_final_funnel['value']=round(db_style_code_funnel_final_funnel['value'],0)
            fig = px.funnel(db_style_code_funnel_final_funnel, x='value', y='metric', color='channel')
            st.plotly_chart(fig, theme="streamlit")

    with tab_sa:
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

    with tab_exp :
      


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

    with tab_imp :
       
        
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
                                df1=df[['order_release_id','customer_paid_amt','commission','igst_tcs','cgst_tcs','sgst_tcs','tds','total_logistics_deduction','pick_and_pack_fee','fixed_fee','payment_gateway_fee','logistics_commission','settled_amount','payment_date','order_type']]
                                df1['total_tax_on_logistics']=df1['logistics_commission']-df1['total_logistics_deduction']-df1['pick_and_pack_fee']-df1['fixed_fee']-df1['payment_gateway_fee']
                                df1['tcs_amount']=df1['igst_tcs']+df1['cgst_tcs']+df1['sgst_tcs']
                                df1['sequence']=2
                                df1.rename(columns = {'commission':'platform_fees','tds':'tds_amount','total_logistics_deduction':'shipping_fee','logistics_commission':'total_logistics','settled_amount':'total_actual_settlement'}, inplace = True)
                                df1=df1.drop(['igst_tcs','cgst_tcs','sgst_tcs'],axis=1) 
                                df1['channel']="Myntra"
                             except:
                                    try:
                                        df1= df[['order_release_id','customer_paid_amt','platform_fees','tcs_amount','tds_amount','shipping_fee','pick_and_pack_fee','fixed_fee','payment_gateway_fee','total_tax_on_logistics','total_actual_settlement','settlement_date_prepaid_payment','settlement_date_postpaid_comm_deduction','shipment_zone_classification']]
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
                                df1=df[['order release id','myntra sku code','state','created on','seller id','order status','return creation date','final amount']]
                                df1['returns']=0
                                df1.loc[df1['return creation date']>'01-01-2000','returns']=1
                                df1.drop(['return creation date'],axis=1,inplace=True)
                                df1.rename(columns = {'order release id':'order_release_id','myntra sku code':'sku_code','created on':'order_created_date','seller id':'seller_id','order status':'order_status','final amount':'final_amount'}, inplace = True)
                                df1['channel']="Myntra"
                                df1['order_created_date']=pd.to_datetime(df1['order_created_date'],dayfirst=True, format='mixed')
                                
                                abc=abc+1
                             except:
                                    st.write(str(filename.name)+" not uploaded, wrong format")
                                
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
                                    df1=df[['channel name','channel product id','seller sku code','vendor sku code','channel style id','vendor style code','brand','gender','article type','image link','size','cost','mrp','color','fabric','collection name']]
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
                                    df1=df[['brand','gender','article_type','metrics','a','b','c']]
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
                                    df1=df[['ros','roi','return %','selling_price','pla','replenishment','remarks']]
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

                        
    with tab_sync :
        db_sales_upload_new=pd.DataFrame()

                
        container=st.container(border=True)



        with st.container(border=True) :
            st.subheader("Sync All Data")

            if st.button('Sync Now',key="sync_btn"):
             try:
                settlement_bar = st.progress(0, text="Syncing Settlements")
                db_settlement_upload=conn.query("select * from settlement_upload;")
                db_settlement_upload.drop_duplicates(inplace=True)
                settlement_bar.progress(1/4, text="Syncing Settlements- Reading Upload data")
                try:
                    db_settlement=conn.query("select * from settlement;")
                except :
                    db_settlement=pd.DataFrame()
                settlement_bar.progress(1/2, text="Syncing Settlements - Reading Settlement table")
                db_settlement_upload.fillna(0,inplace=True)
                db_settlement_monthly=db_settlement_upload[(db_settlement_upload['sequence']==1)]
                db_settlement_weekly=db_settlement_upload[(db_settlement_upload['sequence']==2)]
                db_settlement_weekly.drop_duplicates(inplace=True)
                db_settlement_monthly.drop_duplicates(inplace=True)
                db_settlement_weekly=db_settlement_weekly.groupby(['order_release_id','shipment_zone_classification','payment_date','order_type','channel','sequence']).agg({'customer_paid_amt':'sum','platform_fees':'sum','tcs_amount':'sum','tds_amount':'sum','shipping_fee':'sum','pick_and_pack_fee':'sum','fixed_fee':'sum','payment_gateway_fee':'sum','total_tax_on_logistics':'sum','total_actual_settlement':'sum','total_logistics':'sum'})
                db_settlement_weekly.reset_index(inplace=True)
                db_settlement_final=pd.concat([db_settlement_monthly,db_settlement_weekly],ignore_index=True,sort=False)
                db_settlement_final.reset_index(inplace=True)
                db_settlement_final.drop_duplicates(inplace=True)
                db_settlement_final.drop(['index','sequence'],axis=1,inplace=True)
                db_settlement_all=pd.concat([db_settlement_final,db_settlement],ignore_index=True,sort=False)
                db_settlement_all.drop_duplicates(subset=['order_release_id','order_type'],inplace=True)
                settlement_bar.progress(3/4, text="Syncing Settlements - Updating Settelment data")
                db_settlement_upload_new=pd.DataFrame()
                db_settlement_upload_new.to_sql(name="settlement_upload", con=engine, if_exists="replace", index=False)    
                db_settlement_all.to_sql(name="settlement", con=engine,if_exists="replace", index=False)    
                settlement_bar.progress(4/4, text="Syncing Settlements - Updated Successfully ")
             except:
                    settlement_bar.progress(4/4, text="No new settlement data to sync")

             try:
                sales_bar = st.progress(0, text="Syncing Sales")
                db_sales_upload=conn.query("select * from sales_upload;")
                try:
                    db_sales=conn.query("select * from sales")
                except:
                    db_sales=pd.DataFrame()
                
                sales_bar.progress(1/4, text="Reading new sales")
                
                if len(db_sales_upload)>0:
                    db_sales_upload.drop_duplicates(subset="order_release_id",inplace=True,keep='first')
                    db_sales_all=pd.concat([db_sales_upload,db_sales],ignore_index=True,sort=False)
                    db_sales_all.drop_duplicates(subset=['order_release_id'],inplace=True,keep='first')
                    sales_bar.progress(2/4, text="Updating new Sales")
                    
                    db_sales_upload_new.to_sql(
                    name="sales_upload", # table name
                    con=engine,  # engine
                    if_exists="replace", #  If the table already exists, replace
                    index=False # no index
                    )    
                    db_sales_all.to_sql(
                    name="sales", # table name
                    con=engine,  # engine
                    if_exists="replace", #  If the table already exists, replace
                    index=False # no index
                    )    
                    sales_bar.progress(4/4, text="New Sales synced Successfully")
                else:
                    sales_bar.progress(4/4, text="No new sales data to sync")  
             except:
                sales_bar.progress(4/4, text="No new sales data to sync")


             try:
                master_bar = st.progress(0, text="Syncing master")
                db_master_upload=conn.query("select * from master_upload;")
                try:
                    db_master=conn.query("select * from master")
                except:
                    db_master=pd.DataFrame()
                master_bar.progress(1/4, text="Reading new master")

                if len(db_master_upload)>0:
                    
                    db_master_upload.drop_duplicates(subset=['channel','channel_product_id'],inplace=True,keep='first')
                    db_master_all=pd.concat([db_master_upload,db_master],ignore_index=True,sort=False)
                    db_master_all.drop_duplicates(subset=['channel','channel_product_id'],inplace=True,keep='first')
                    master_bar.progress(2/4, text="Updating new master")
                    db_master_upload_new=pd.DataFrame()
                    db_master_upload_new.to_sql(
                    name="master_upload", # table name
                    con=engine,  # engine
                    if_exists="replace", #  If the table already exists, replace
                    index=False # no index
                    )    
                    db_master_all.to_sql(
                    name="master", # table name
                    con=engine,  # engine
                    if_exists="replace", #  If the table already exists, replace
                    index=False # no index
                    )    
                    master_bar.progress(4/4, text="New master synced Successfully")
                else :
                    master_bar.progress(4/4, text="No new master data to sync")
             except:
                master_bar.progress(4/4, text="No new master data to sync")



             try:
                final_bar=st.progress(0,text="Syncing all data")
                db_sales=conn.query("select * from sales;")
                db_settlement=conn.query("select * from settlement;")
                db_master=conn.query("select * from master;")
                db_sales=db_sales.drop_duplicates()
                db_settlement=db_settlement.drop_duplicates()
                db_master=db_master.drop_duplicates()

                final_bar.progress(1/4,text="Merging the Data")
                db_data=db_sales.merge(db_settlement,left_on=['order_release_id'],right_on=['order_release_id'])
                db_data=db_data.merge(db_master,left_on=['sku_code'],right_on=['channel_product_id'])
                db_data['seller_id']=db_data['seller_id'].astype(str)
                db_data.drop(['sku_code_x','channel_x','channel_y','channel_product_id','sku_code_y','channel_style_id'],axis=1,inplace=True)

                db_sales_final=db_sales.merge(db_master,left_on=['sku_code'],right_on=['channel_product_id'])
                db_sales_final['seller_id']=db_sales_final['seller_id'].astype(str)
                final_bar.progress(2/4,text="Final Magic ")
                db_data.to_sql(
                name="final_data", # table name
                con=engine,  # engine
                if_exists="replace", #  If the table already exists, replace
                index=False # no index
                )  
                
                db_sales_final.to_sql(
                name="final_sales", # table name
                con=engine,  # engine
                if_exists="replace", #  If the table already exists, replace
                index=False # no index
                )  
                final_bar.progress(3/4,text="Final Magic")

                db_data=conn.query("select * from final_data;")
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
                        db_style_data.loc[index,'roi_action']='C'


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
                
                    db_style_data['date_updated']=datetime.datetime.now()
                    db_style_data.to_sql(
                name="action_items_suggestion", # table name
                con=engine,  # engine
                if_exists="replace", #  If the table already exists, replace
                index=False # no index
                )  
                
                final_bar.progress(4/4,text="All syncing done - Happy Analysing")

             except :
                st.write("something went wrong, please contact administrator")    

                            

                            

                        
                        





