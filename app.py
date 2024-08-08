import streamlit as st
import pandas as pd
import numpy as np
import sys
import glob
import os
from sqlalchemy import create_engine,MetaData,Table, Column, Numeric, Integer, VARCHAR, update
import altair as alt
import plotly.express as px


conn = st.connection("my_database")

db_settlement_1234=conn.query("select * from trial_1;")
db_settlement_1234
