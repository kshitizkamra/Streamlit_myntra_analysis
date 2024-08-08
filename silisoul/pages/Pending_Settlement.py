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


conn = st.connection("my_database")

db_sales=conn.query("select * from final_sales;")
db_settlement=conn.query("select * from final_settlement;")

db_sales_final=db_sales[~(db_sales['sale_order_code'].isin(db_settlement['order_release_id']))]

total_customer_paid=db_sales_final['total_amount'].sum().round()

db_sales_final

st.write(" Total Settlement pending for - INR  " + str(total_customer_paid) + "/- of sales")