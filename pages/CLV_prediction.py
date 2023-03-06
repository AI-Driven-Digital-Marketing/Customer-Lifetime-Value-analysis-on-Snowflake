import pandas as pd
import streamlit as st
import json
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
        
#         json_dict=json.load(open('zip_json.json'))
        

        options_dict = {
        "name": "John",
        "age": 30,
        "gender": "male"
        }

#         # Convert JSON dictionary to list of key-value pairs
#         options = list(json_dict.items())


#         st.selectbox('CA_ZIP', options, index=0, format_func=lambda x: x.title(), typeahead=True)

#         options_dict = {"Option 1": 1, "Option 2": 2, "Option 3": 3}

        # Define a lambda function to format the options
        format_func = lambda option_value: next(label for label, value in options_dict.items() if value == option_value)

        # Create the searchable selectbox
        selected_option = st.multiselect("Select an option", options=options_dict, format_func=format_func)
        
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
         
    

