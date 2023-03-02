import pandas as pd
import streamlit as st



st.set_page_config(page_title= "CLV Predication")

st.image(
    "Food.jpg",
    use_column_width = True,
)

st.title("CLV Predication")

st.write("")


with st.expander("Attribute/Filter"):
  

  st.number_input(
    'C_BIRTH_YEAR:',
    0)
  st.number_input(
    'CA_ZIP:',
    0)
  st.number_input(
    'CD_DEP_COUNT:',
    0)
    
with st.expander("About this app"):
    st.write('## XGBoost model to predict CLV')

    st.write('Type inputs here......')
