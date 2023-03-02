import pandas as pd
import streamlit as st

# st.set_page_config(page_title= "CLV Predication")

# st.image(
#     "Food.jpg",
#     use_column_width = True,
# )
st.title("XGBoost model to predict CLV")

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
#Categorical
    st.selectbox(
      'CD_Gender',
      ('Option1',
       'Option2',
       'Option3C_BIRTH_YEAR',
       'CD_GENDER', 
       'CD_MARITAL_STATUS', 
       'CD_CREDIT_RATING', 
       'CD_EDUCATION_STATUS', 
       'CD_DEP_COUNT'),
     )

