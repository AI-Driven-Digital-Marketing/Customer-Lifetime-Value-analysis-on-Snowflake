import pandas as pd
import streamlit as st
# from streamlit.report_thread import get_report_ctx
# from streamlit.server.server import Server

st.set_page_config(page_title= "Customer Lifetime Value")

st.image(
    "src/1649251328-maximize-your-clv.webp",
    width = 600,
)

st.title("XGBoost model to predict CLV")

with st.expander("Attribute/Filter"):
    col1, col2 = st.columns(2,gap = "medium")
    with col1:
        st.markdown('#### Numeric Features')
        st.number_input(
        'C_BIRTH_YEAR:', min_value=1924,
        max_value=2020)
        
        json_dict=json.load(open('zip_json.json'))
        options = list(json_dict.items())
        st.selectbox('CA_ZIP', options, index=0, format_func=lambda x: x.title(), typeahead=True)
        
    with col2:
        st.markdown('#### Categorical Features ')
        st.selectbox(
          'CD_Gender',
          ('M',
           'F'),
         )
         
        st.selectbox(
          'CD_MARITAL_STATUS',
          ('S',
           'D',
           'W',
           'U', 
           'M'),
         )
        st.selectbox(
          'CD_CREDIT_RATING',
          ('option1'),
         )
        st.selectbox(
          'CD_EDUCATION_STATUS',
          ('Advanced Degree',
           'Secondary',
           '2 yr Degree',
           '4 yr Degree', 
           'Unknown', 
           'Primary', 
           'College'),)
        st.selectbox(
          'CD_DEP_COUNT',
          ('0',
           '1'),
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
         11111
         ''')
         
    

