import streamlit as st
import pandas as pd
import numpy as np
import sys
import glob
import os
from sqlalchemy import create_engine,MetaData,Table, Column, Numeric, Integer, VARCHAR, update,insert
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

db_actual_actions_selling_price=conn.query("select distinct selling_price from actual_actions")
db_actual_actions_replenishment=conn.query("select distinct replenishment from actual_actions")
db_actual_actions_pla=conn.query("select distinct pla from actual_actions")

actual_actions_selling_price=db_actual_actions_selling_price['selling_price'].values.tolist()
actual_actions_pla=db_actual_actions_pla['pla'].values.tolist()
actual_actions_replenishment=db_actual_actions_replenishment['replenishment'].values.tolist()

engine = create_engine('postgresql://90northbrands:90northbrands@34.41.36.17:5432/db_90northbrands')
meta = MetaData()
meta.reflect(bind=engine)




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

st.markdown("""
        <style>
               .block-container {
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
                    text-align: center;
                    font-size : 7px;
                    gap: 0rem;

                }
              .divder{
                    padding-top: 1rem;
                    padding-bottom: 0rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
            }
        </style>
        """, unsafe_allow_html=True)




st.markdown("<h1 style='text-align: center; color: grey;'>Style Review</h1>", unsafe_allow_html=True)

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






pd.set_option('display.max_colwidth', None)
db_sales_summary=db_sales_summary.round(2)
db_sales_summary['roi'] = db_sales_summary['roi'].map('{:.0%}'.format)
db_sales_summary['return %'] = db_sales_summary['return %'].map('{:.0%}'.format)

db_sales_summary=db_sales_summary.sort_values('successful_orders',ascending=False)
db_sales_summary=db_sales_summary.reset_index()
days=(end_date-start_date).days
total_profit=db_sales_summary['profit_value'].sum()
db_sales_summary['ROS']=db_sales_summary['successful_orders']/days
db_sales_summary['profit %']=db_sales_summary['profit_value']/total_profit
db_sales_summary=db_sales_summary.round(2)
db_sales_summary['profit %'] = db_sales_summary['profit %'].map('{:.0%}'.format)





db_search_style_code=db_sales_summary['vendor style code'].drop_duplicates()
search_style_code_list = db_search_style_code.values.tolist()
search_style_code = st.multiselect(
      "Search/Select Style Code",
      search_style_code_list,
      placeholder="Search/Select Style Code",
      label_visibility='collapsed'
    )



if len(search_style_code)>0 :
   db_sales_summary_final=db_sales_summary[(db_sales_summary['vendor style code'].isin(search_style_code))]
   st.session_state['page_number'] = 1
else :
    db_sales_summary_final=db_sales_summary.copy()
st.divider()

db_sales_summary_final.reset_index(inplace=True)
db_sales_summary_final.index += 1
db_sales_summary_final=db_sales_summary_final.drop(['index'],axis=1)

total_pages=len(db_sales_summary_final)


if 'page_number' not in st.session_state:
   st.session_state['page_number'] = 1
else:
    page_number = st.session_state['page_number']



col1,col2=st.columns([6,1])
with col2 :
     subcol1,subcol2,subcol3,subcol4=st.columns([2,3,3,2],vertical_alignment="bottom")

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
         page_number = st.number_input("",value=st.session_state['page_number'],min_value=1, max_value=total_pages)
         st.session_state['page_number']=page_number
     with subcol3:
         st.text("/"+str(total_pages))    
 


col1,col2=st.columns([1,2])

with col1 :
     
     st.image(db_sales_summary_final.loc[st.session_state['page_number'],'image link'], width=360)
     
     
with col2:
     db_sales_summary_final=db_sales_summary_final[['vendor style code','brand','article type','successful_orders','ROS','unit_customer_paid_amount','unit_cost','unit_p/l','roi','return %','val %','profit %','collection name','gender']].copy()
     db_sales_summary_final.rename(columns={'vendor style code': 'Style Code', 'brand': 'Brand','article type':'Category','successfull_orders':'Successfull Orders','unit_customer_paid_amount':'ASP','unit_cost':'COGS','unit_p/l':'Avg Profit per unit','roi':'ROI','return %':'Return','val %':'Revenue Contribution','profit %':'Profit Contribution','collection name':'Collection'}, inplace=True)
     st.table(data=db_sales_summary_final.iloc[st.session_state['page_number']-1])












brand=db_sales_summary_final.loc[st.session_state['page_number'],'Brand']
gender=db_sales_summary_final.loc[st.session_state['page_number'],'gender']
article_type =db_sales_summary_final.loc[st.session_state['page_number'],'Category']


db_checkaction_ros=db_styles_action[(db_styles_action['gender']==gender)& (db_styles_action['article type']==article_type) & (db_styles_action['brand']==brand) &(db_styles_action['metrics']=='ros') ]
db_checkaction_ros=db_checkaction_ros.reset_index()
if db_sales_summary_final.loc[st.session_state['page_number'],'ROS']>=db_checkaction_ros.a[0] :
    ros_action="A"
elif db_sales_summary_final.loc[st.session_state['page_number'],'ROS']>=db_checkaction_ros.b[0] :
    ros_action="B"
else :
    ros_action="C"

db_checkaction_roi=db_styles_action[(db_styles_action['gender']==gender)& (db_styles_action['article type']==article_type) & (db_styles_action['brand']==brand) &(db_styles_action['metrics']=='roi') ]

db_checkaction_roi=db_checkaction_roi.reset_index()
roi=db_sales_summary_final.loc[st.session_state['page_number'],'Avg Profit per unit']/db_sales_summary_final.loc[st.session_state['page_number'],'COGS']


if roi>=db_checkaction_roi.a[0] :
    roi_action="A"
elif roi>=db_checkaction_roi.b[0] :
    roi_action="B"
else :
    roi_action="C"


db_checkaction_returns=db_styles_action[(db_styles_action['gender']==gender)& (db_styles_action['article type']==article_type) & (db_styles_action['brand']==brand) &(db_styles_action['metrics']=='return %') ]

db_checkaction_returns=db_checkaction_returns.reset_index()
returns=db_sales_summary_final.loc[st.session_state['page_number'],'Return']
returns = returns.rstrip(returns[-1])
returns=int(returns)/100

if returns<=db_checkaction_returns.a[0] :
    returns_action="A"
elif returns<=db_checkaction_returns.b[0] :
    returns_action="B"
else :
    returns_action="C"


st.markdown("""
<style>
.big-font {
    font-size:30px !important;
    background-color: #44546a ;
    color: white;
}
</style>
""", unsafe_allow_html=True)

colm1,colm2,colm3=st.columns([3,1,3])
with colm1:
    st.markdown('<p class="big-font">System Suggestions</p>', unsafe_allow_html=True)
    st.markdown("""
<style>
.small-font {
    font-size:25px !important;
    background-color: #44546a ;
    color: white;
}
                
        .actions {-
            font-size:15px !important;
            background-color: white ;
                    outline-style: double;
            color: black;
            }
        
</style>
""", unsafe_allow_html=True)
    

    subcolm1,subcolm2=st.columns(2)
    with subcolm1:
        st.markdown('<p class="small-font">Selling Price</p>', unsafe_allow_html=True)
        st.markdown('<p class="small-font">PLA</p>', unsafe_allow_html=True)
        st.markdown('<p class="small-font">Replenishment</p>', unsafe_allow_html=True)
    with subcolm2:


        db_actual_actions=db_actual_actions[(db_actual_actions['ros']==ros_action)&(db_actual_actions['roi']==roi_action)&(db_actual_actions['return %']==returns_action)]
        
        db_actual_actions=db_actual_actions.reset_index()
        selling_price_1=db_actual_actions['selling_price'][0]
        sp_index=actual_actions_selling_price.index(selling_price_1)
        action_sp=st.selectbox("Select",actual_actions_selling_price,index=sp_index,label_visibility='collapsed')
        
        
        pla=db_actual_actions['pla'][0]
        pla_index=actual_actions_pla.index(pla)
        action_pla=st.selectbox("Select",actual_actions_pla,index=pla_index,label_visibility='collapsed')
        

        replenishment=db_actual_actions['replenishment'][0]
        replenishment_index=actual_actions_replenishment.index(replenishment)
        action_replenishment=st.selectbox("Select",actual_actions_replenishment,index=replenishment_index,label_visibility='collapsed')
    
with colm2 :
         subsubcol1,subsubcol2=st.columns(2)
         
         with subsubcol1 :
             st.write("")
             st.write("")
             st.write("")
             st.write("")
             st.write("")
             if st.button("✅ ", use_container_width=True):
                try :
                        style_code=db_sales_summary_final.loc[st.session_state['page_number'],'Style Code']
                        
                        action_items = meta.tables['action_items']

                        stmt = (update(action_items).
                                            where(action_items.c.vendor_style_code == style_code).
                                            values(selling_price=action_sp))
                       
                        with engine.connect() as con:
                            con.execute(stmt)
                        
                except :
                        action_items1 = meta.tables['action_items']
                        insert_statement =action_items1.insert().values(selling_price=action_sp,vendor_style_code=db_sales_summary_final.loc[st.session_state['page_number'],'Style Code'])
                        insert_statement
                        with engine.connect() as conn:
                            conn.execute(insert_statement)                      
                        
         with subsubcol2:
             st.write("")
             st.write("")
             st.write("")
             st.write("")
             st.write("")
             if st.button("✪ ", use_container_width=True):
                st.session_state['page_number'] -= 1     
         with subsubcol1 :

             if st.button(" ✅", use_container_width=True):
                
                st.session_state['page_number'] -= 1     
         with subsubcol2:
             
             if st.button(" ✪", use_container_width=True):
                st.session_state['page_number'] -= 1     
        
         with subsubcol1 :

             if st.button(" ✅ ", use_container_width=True):
                st.session_state['page_number'] -= 1     
         with subsubcol2:
             
             if st.button(" ✪ ", use_container_width=True):
                st.session_state['page_number'] -= 1     
with colm3 :
        st.markdown("""
<style>
.remarks {-
    font-size:35px !important;
    background-color: #44546a ;
    color: white;
}
</style>
""", unsafe_allow_html=True)
        remarks=db_actual_actions['remarks'][0]
        html_remarks = f"""<p class="remarks">{remarks}</p>"""
        st.markdown(html_remarks, unsafe_allow_html=True)








     
