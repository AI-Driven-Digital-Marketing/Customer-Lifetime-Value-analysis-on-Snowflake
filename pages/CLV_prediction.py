import pandas as pd
import streamlit as st

# st.set_page_config(page_title= "CLV Predication")

# st.image(
#     "Food.jpg",
#     use_column_width = True,
# )
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
# Submit widget

        st.button(
        'Submit'
        )
        st.button(
        'Cancel'
        )
      
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
st.markdown('''
            ----
            ## Customer Lifetime Value:
            
            ''')
if st.button('Submit'):
    st.write('<Modeling result here>')
elif st.button('Cancel'):
    st.write('<reset explander value>')
else:
    st.write('Please setup attributes')
    

