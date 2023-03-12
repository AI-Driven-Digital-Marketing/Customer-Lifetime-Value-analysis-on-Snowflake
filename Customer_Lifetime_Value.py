import pandas as pd
import streamlit as st

# st.write([[1969, '66060','M','U','Low Risk','2 yr Degree', 1]])
st.set_page_config(page_title= "Sqlalchemy Query")

st.image(
    "src/1649251328-maximize-your-clv.webp",
    width = 600,
)

col1, col2 = st.columns(2,gap = "medium")
with col1:
    model_select = st.radio('Select the Model here:',
                           [ 'XGBoost','Linear Regression']
                            #[ 'XGBoost']
                           )


with col2:
    st.markdown('#### Numeric Features')
    Cus_by = st.number_input('Customer Birth Year:', 
                    min_value=1924,
                    max_value=2020, 
                    help = 'Please type VALID birth Year!!(Range: 1924~2020)'
                            )

    Cs_zip = st.number_input( 'Customer Zip Code:', 
                    min_value= 601, 
                    max_value= 99981,
                    value  = 66668,
                    step = 1
                            )
    st.markdown('#### Categorical Features ')
    Cus_gender = st.selectbox('CD_Gender',
                              ['M', 'F'], 
                              help= 'M: Male, F: Female'
     )

    Cus_marital = st.selectbox( 'CD_MARITAL_STATUS',
                 ['S','D','W', 'U', 'M'],
     )
    Cus_credit = st.selectbox('CD_CREDIT_RATING',
                 ['Low Risk','Unknown','Good','High Risk']
       )

    Cus_edu = st.selectbox( 'CD_EDUCATION_STATUS',
                 ['Advanced Degree','Secondary','2 yr Degree','4 yr Degree','Unknown','Primary','College']
                )
    Cus_dep = st.selectbox( 'CD_DEP_COUNT',
                 ['0', '1'],
      )



col3, col4 = st.columns([8,2],gap = "medium")

with col3:

with col4:
    submit =  st.button('Submit')
    reset = st.button('Reset ')
