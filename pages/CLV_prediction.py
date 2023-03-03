import pandas as pd
import streamlit as st
from streamlit.report_thread import get_report_ctx
from streamlit.server.server import Server

st.set_page_config(page_title= "Customer Lifetime Value")

st.image(
    "src/1649251328-maximize-your-clv.webp",
    use_column_width = True,
)

st.title("XGBoost model to predict CLV")

with st.expander("Attribute/Filter"):
    col1, col2 = st.columns(2,gap = "medium")
    with col1:
        st.markdown('#### Numeric Features')
        st.number_input(
        'C_BIRTH_YEAR:',
        0)
        st.number_input(
        'CA_ZIP:',
        0)
        st.number_input(
        'CD_DEP_COUNT:',
        0)
      
    with col2:
        st.markdown('#### Categorical Features ')
        st.selectbox(
          'CD_Gender',
          ('Option1',
           'Option2',
           'Option3',
           'CD_GENDER', 
           'CD_MARITAL_STATUS', 
           'CD_CREDIT_RATING', 
           'CD_EDUCATION_STATUS', 
           'CD_DEP_COUNT'),
         )
         
        st.selectbox(
          'CD_MARITAL_STATUS',
          ('Option1',
           'Option2',
           'Option3',
           'CD_GENDER', 
           'CD_MARITAL_STATUS', 
           'CD_CREDIT_RATING', 
           'CD_EDUCATION_STATUS', 
           'CD_DEP_COUNT'),
         )
        st.selectbox(
          'CD_CREDIT_RATING',
          ('Option1',
           'Option2',
           'Option3',
           'CD_GENDER', 
           'CD_MARITAL_STATUS', 
           'CD_CREDIT_RATING', 
           'CD_EDUCATION_STATUS', 
           'CD_DEP_COUNT'),
         )
        st.selectbox(
          'CD_EDUCATION_STATUS',
          ('Option1',
           'Option2',
           'Option3',
           'CD_GENDER', 
           'CD_MARITAL_STATUS', 
           'CD_CREDIT_RATING', 
           'CD_EDUCATION_STATUS'),
         )
#Output

if st.button('Submit'):
    st.button('Reset')
    col1, col2 = st.columns(2,gap = "medium")
    with col1:
        st.markdown('''
        --------------------------------
        ## Customer Lifetime Value:

        ''')
    with col2:    
        st.markdown('''
        
        *<Modeling result ouput on here>*
        ''')
       
    
    
elif st.button('Reset'):
   
     st.write('<reset explander value>')
    
else:
     st.markdown(
         '''
         ----------  
         ## Customer Lifetime Value:
         Please setup filter value！！
         
         ''')
    

