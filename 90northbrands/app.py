import hmac
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
    # st.image("logo.png",width=150)
    st.write("try")
with col3:
    col5,col6=st.columns([3,2])
    with col6:
            st.write("")
            st.write("")
            with st.popover("Sign In"):
                
                if check_password():
                    st.session_state["login_check"]=1

if st.session_state['login_check']==1 :
    tab_so,tab_pnl,tab_sr,tab_sa,tab_exp,tab_imp,tab_sync=st.tabs(["Sales Overview","P&L","Style_Review","Suggested Actions","Export_data","Import_data","Sync_data"])
else :
    home,about_us=st.tabs(["Home","About Us"])

try :
    with about_us:
     st.write('<a href="/Sales_Overview" target="_self">Next page</a>', unsafe_allow_html=True)

except :
    conn = st.connection("my_database")
    
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
                                df1=df[['order_release_id','customer_paid_amt','commission','igst_tcs','cgst_tcs','sgst_tcs','tds','total_logistics_deduction','pick_and_pack_fee','fixed_fee','payment_gateway_fee','logistics_commission','settled_amount','payment_date','order_type']].copy()
                                df1['total_tax_on_logistics']=df1['logistics_commission']-df1['total_logistics_deduction']-df1['pick_and_pack_fee']-df1['fixed_fee']-df1['payment_gateway_fee']
                                df1['tcs_amount']=df1['igst_tcs']+df1['cgst_tcs']+df1['sgst_tcs']
                                df1['sequence']=2
                                df1.rename(columns = {'commission':'platform_fees','tds':'tds_amount','total_logistics_deduction':'shipping_fee','logistics_commission':'total_logistics','settled_amount':'total_actual_settlement'}, inplace = True)
                                df1=df1.drop(['igst_tcs','cgst_tcs','sgst_tcs'],axis=1) 
                                df1['channel']="Myntra"
                             except:
                                    try:
                                        df1= df[['order_release_id','customer_paid_amt','platform_fees','tcs_amount','tds_amount','shipping_fee','pick_and_pack_fee','fixed_fee','payment_gateway_fee','total_tax_on_logistics','total_actual_settlement','settlement_date_prepaid_payment','settlement_date_postpaid_comm_deduction','shipment_zone_classification']].copy()
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
                st.write("something went wrong, please contact administrator")    

                            

                            

                        
                        





