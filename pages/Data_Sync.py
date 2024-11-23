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
try:
    db_data=conn.query("select * from final_data;")
    db_sales_data=conn.query("select * from final_sales")
    db_sales_data_for_side_filter=conn.query("select * from final_sales")
    db_latlong=conn.query("select * from latlong")
except:
    db_data=pd.DataFrame()
    db_sales_data=pd.DataFrame()
    db_sales_data_for_slide=pd.DataFrame()
    db_latlong=pd.DataFrame()


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
                db_sales
                db_settlement
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

                # db_style_data['order_created_date']).dt.days

                db_style_data=db_data.groupby(['vendor_style_code','channel','brand','gender','article_type'],as_index=False).agg({'order_count':'sum','return_count':'sum','p/l':'sum','cost':'sum','order_created_date':'min'})
                try:
                    db_style_data['ros']=db_style_data['order_count']/(pd.to_datetime(datetime.date.today(),format='ISO8601')-db_style_data['order_created_date']).dt.days
                except:
                    db_style_data['ros']=db_style_data['order_count']/(pd.to_datetime(datetime.date.today(),format='ISO8601')-db_style_data['order_created_date'])
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

                    db_style_data['date_updated']=datetime.datetime.now()
                    db_style_data.to_sql(
                name="action_items_suggestion", # table name
                con=engine,  # engine
                if_exists="replace", #  If the table already exists, replace
                index=False # no index
                )  

                final_bar.progress(4/4,text="All syncing done - Happy Analysing")

     except :
            st.write("something went wrong, please contact your admin")  
                            

