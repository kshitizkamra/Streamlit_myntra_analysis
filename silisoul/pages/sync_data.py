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

if st.button("Click to Sync the Latest Uploads"):
     alt.themes.enable("dark")

     conn = st.connection("my_database")
     db_master=conn.query("select * from styles_master;")
     db_sales=conn.query("select * from final_sales;")
     db_settlement=conn.query("select * from final_settlement;")
     
     db_settlement=db_settlement.drop(['payment_type','nod_comment','store_order_id','seller_order_id','comments','mrp','neft_ref','return_id','packet_id','marketingcontribution'],axis=1)
     db_settlement.fillna(0,inplace=True)
     db_settlement=db_settlement.groupby(['order_line_id','order_type','order_release_id','seller_id']).agg({'igst_amount':'sum','cgst_amount':'sum','sgst_amount':'sum','customer_paid_amt':'sum','commission':'sum','igst_tcs':'sum','cgst_tcs':'sum','sgst_tcs':'sum','tds':'sum','total_logistics_deduction':'sum','pick_and_pack_fee':'sum','fixed_fee':'sum','payment_gateway_fee':'sum','logistics_commission':'sum','settled_amount':'sum','taxable_amount':'sum','tax_rate':'max','seller_discount':'sum','platform_discount':'sum','total_discount':'sum','fwdadditionalcharges':'sum','rvsadditionalcharges':'sum','techenablement':'sum','airlogistics':'sum','royaltycharges':'sum','royaltypercent':'sum','marketingcharges':'sum','marketingpercent':'sum','payment_date':'max'})
     db_settlement=db_settlement.reset_index()
     
     db_sales_123=db_sales[['sale_order_code','customer_state','sku_code','order_created_date']].copy()

     db_master_1=db_master[['channel name','channel product id','seller sku code','vendor sku code','channel style id','vendor style code','brand','gender','article type','image link','size','cost','mrp','color','fabric','collection name']].copy()
     db_master_1['channel style id']=db_master_1['channel style id'].astype(str)
     
     db_sales_1=pd.DataFrame()
     db_sales_1 = db_settlement.merge(
        db_sales_123,
        left_on=['order_release_id'],
        right_on=['sale_order_code']
        )
    
     db_sales_final = db_sales_1.merge(
        db_master_1,
        left_on=['sku_code'],
        right_on=['channel product id']
        )
     db_sales_final['GST']=db_sales_final['customer_paid_amt']*db_sales_final['tax_rate']/100
     db_sales_final=db_sales_final.groupby(['brand','gender','article type','fabric','collection name','vendor style code','image link','customer_state','seller_id','order_created_date','order_type','channel style id','size','seller sku code','mrp']).agg({'igst_amount':'sum','cgst_amount':'sum','sgst_amount':'sum','customer_paid_amt':'sum','commission':'sum','igst_tcs':'sum','cgst_tcs':'sum','sgst_tcs':'sum','tds':'sum','total_logistics_deduction':'sum','pick_and_pack_fee':'sum','fixed_fee':'sum','payment_gateway_fee':'sum','logistics_commission':'sum','settled_amount':'sum','seller_discount':'sum','platform_discount':'sum','total_discount':'sum','fwdadditionalcharges':'sum','rvsadditionalcharges':'sum','techenablement':'sum','airlogistics':'sum','royaltycharges':'sum','royaltypercent':'sum','marketingcharges':'sum','marketingpercent':'sum','cost':'sum','order_type':'count','GST':'sum'})
     db_sales_final.rename(columns = {'order_type':'order_count'}, inplace = True)
     db_sales_final=db_sales_final.reset_index()

     db_sales_bar1=db_sales_final.drop(['channel style id','size','seller sku code','mrp','igst_amount','cgst_amount','sgst_amount'],axis=1)
     db_sales_bar1=db_sales_bar1.groupby(['order_created_date','seller_id','customer_state','gender','brand','article type','fabric','collection name','vendor style code','order_type','image link']).sum()
     db_sales_bar1=db_sales_bar1.reset_index()
     db_sales_bar1['tax']=db_sales_bar1['igst_tcs']+db_sales_bar1['cgst_tcs']+db_sales_bar1['sgst_tcs']+db_sales_bar1['tds']+db_sales_bar1['GST']
     db_sales_bar1=db_sales_bar1.drop(['igst_tcs','cgst_tcs','sgst_tcs','tds','GST'],axis=1)
     db_sales_bar1_forward=db_sales_bar1[(db_sales_bar1['order_type']=='Forward')].drop(['order_type'],axis=1)
     db_sales_bar1_reverse=db_sales_bar1[(db_sales_bar1['order_type']=='Reverse')].drop(['order_type'],axis=1)
     db_sales_bar1_forward['checkpoint']=db_sales_bar1_forward['order_created_date'].astype(str)+db_sales_bar1_forward['seller_id'].astype(str)+db_sales_bar1_forward['customer_state']+db_sales_bar1_forward['gender']+db_sales_bar1_forward['brand']+db_sales_bar1_forward['article type']+db_sales_bar1_forward['fabric']+db_sales_bar1_forward['collection name']+db_sales_bar1_forward['vendor style code']+db_sales_bar1_forward['image link']
     db_sales_bar1_forward=db_sales_bar1_forward.set_index('checkpoint')
     db_sales_bar1_reverse['checkpoint']=db_sales_bar1_reverse['order_created_date'].astype(str)+db_sales_bar1_reverse['seller_id'].astype(str)+db_sales_bar1_reverse['customer_state']+db_sales_bar1_reverse['gender']+db_sales_bar1_reverse['brand']+db_sales_bar1_reverse['article type']+db_sales_bar1_reverse['fabric']+db_sales_bar1_reverse['collection name']+db_sales_bar1_reverse['vendor style code']+db_sales_bar1_reverse['image link']
     db_sales_bar1_reverse=db_sales_bar1_reverse.set_index('checkpoint')
     db_sales_bar1_final=db_sales_bar1[['order_created_date','seller_id','customer_state','gender','brand','article type','fabric','collection name','vendor style code','image link']].copy()
     db_sales_bar1_final=db_sales_bar1_final.drop_duplicates()
     db_sales_bar1_final['checkpoint']=db_sales_bar1_final['order_created_date'].astype(str)+db_sales_bar1_final['seller_id'].astype(str)+db_sales_bar1_final['customer_state']+db_sales_bar1_final['gender']+db_sales_bar1_final['brand']+db_sales_bar1_final['article type']+db_sales_bar1_final['fabric']+db_sales_bar1_final['collection name']+db_sales_bar1_final['vendor style code']+db_sales_bar1_final['image link']
     db_sales_bar1_final[['total_orders','successful_orders','returns','return %','customer_paid_amount','taxes','commission','logistics','settled_amount','cost','profit_value','roi','unit_paid_amount','unit_cost','unit_p/l']]=0
     db_sales_bar1_final=db_sales_bar1_final.set_index('checkpoint')  
     db_sales_bar1_final['checkpoint1']=db_sales_bar1_final['order_created_date'].astype(str)+db_sales_bar1_final['seller_id'].astype(str)+db_sales_bar1_final['customer_state']+db_sales_bar1_final['gender']+db_sales_bar1_final['brand']+db_sales_bar1_final['article type']+db_sales_bar1_final['fabric']+db_sales_bar1_final['collection name']+db_sales_bar1_final['vendor style code']+db_sales_bar1_final['image link']

     for index,rows in db_sales_bar1_final.iterrows():
        
        checkpoint=rows.checkpoint1
        
        try :
            fwd_order_count=db_sales_bar1_forward.loc[checkpoint,'order_count']
            fwd_cust_paid_amount=db_sales_bar1_forward.loc[checkpoint,'customer_paid_amt']
            fwd_taxes=db_sales_bar1_forward.loc[checkpoint,'tax']
            fwd_logistics_tax=db_sales_bar1_forward.loc[checkpoint,'logistics_commission']-db_sales_bar1_forward.loc[checkpoint,'logistics_commission']/1.18
            fwd_commission=db_sales_bar1_forward.loc[checkpoint,'commission']
            fwd_logistics=db_sales_bar1_forward.loc[checkpoint,'logistics_commission']/1.18
            fwd_settled_amount=db_sales_bar1_forward.loc[checkpoint,'settled_amount']
            fwd_cost=db_sales_bar1_forward.loc[checkpoint,'cost']
        except :
            fwd_order_count=0
            fwd_cust_paid_amount=0
            fwd_taxes=0
            fwd_commission=0
            fwd_logistics=0
            fwd_settled_amount=0
            fwd_cost=0
            fwd_logistics_tax=0
        try:
            rvs_order_count=db_sales_bar1_reverse.loc[checkpoint,'order_count']
            rvs_cust_paid_amount=db_sales_bar1_reverse.loc[checkpoint,'customer_paid_amt']
            rvs_taxes=db_sales_bar1_reverse.loc[checkpoint,'tax']
            rvs_logistics_tax=db_sales_bar1_reverse.loc[checkpoint,'logistics_commission']-db_sales_bar1_reverse.loc[checkpoint,'logistics_commission']/1.18
            rvs_commission=db_sales_bar1_reverse.loc[checkpoint,'commission']
            rvs_logistics=db_sales_bar1_reverse.loc[checkpoint,'logistics_commission']/1.18
            rvs_settled_amount=db_sales_bar1_reverse.loc[checkpoint,'settled_amount']
            rvs_cost=db_sales_bar1_reverse.loc[checkpoint,'cost']
        except :
            rvs_order_count=0
            rvs_cust_paid_amount=0
            rvs_taxes=0
            rvs_logistics_tax=0
            rvs_logistics=0
            rvs_commission=0
            rvs_settled_amount=0
            rvs_cost=0
        db_sales_bar1_final.loc[checkpoint,'total_orders']=fwd_order_count
        db_sales_bar1_final.loc[checkpoint,'successful_orders']=fwd_order_count-rvs_order_count
        db_sales_bar1_final.loc[checkpoint,'returns']=rvs_order_count
        db_sales_bar1_final.loc[checkpoint,'customer_paid_amount']=fwd_cust_paid_amount-rvs_cust_paid_amount
        db_sales_bar1_final.loc[checkpoint,'taxes']=fwd_taxes-rvs_taxes+rvs_logistics_tax+fwd_logistics_tax
        db_sales_bar1_final.loc[checkpoint,'commission']=fwd_commission-rvs_commission
        db_sales_bar1_final.loc[checkpoint,'logistics']=fwd_logistics+rvs_logistics
        db_sales_bar1_final.loc[checkpoint,'settled_amount']=fwd_settled_amount+rvs_settled_amount
        db_sales_bar1_final.loc[checkpoint,'cost']=fwd_cost-rvs_cost
       
     db_sales_bar1_final['profit_value']=db_sales_bar1_final['settled_amount']-db_sales_bar1_final['cost']
     db_sales_bar1_final['roi']=db_sales_bar1_final['profit_value']/db_sales_bar1_final['cost']
     db_sales_bar1_final['unit_paid_amount']=db_sales_bar1_final['settled_amount']/db_sales_bar1_final['successful_orders']
     db_sales_bar1_final['unit_cost']=db_sales_bar1_final['cost']/db_sales_bar1_final['successful_orders']
     db_sales_bar1_final['unit_p/l']=db_sales_bar1_final['unit_paid_amount']-db_sales_bar1_final['unit_cost']
     db_sales_bar1_final['return %']=db_sales_bar1_final['returns']/db_sales_bar1_final['total_orders']
     db_sales_bar1_final['return %'] = db_sales_bar1_final['return %'].map('{:.0%}'.format)
     db_sales_bar1_final['roi'] = db_sales_bar1_final['roi'].map('{:.0%}'.format)
     db_sales_bar1_final=db_sales_bar1_final.reset_index()
     db_sales_bar1_final=db_sales_bar1_final.drop(['checkpoint1','checkpoint'],axis=1)





     engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/db_silisoul')
     db_sales_bar1_final.to_sql(
         name="sales_data_sync", # table name
         con=engine,  # engine
         if_exists="replace", #  If the table already exists, append
         index=False # no index
     )

