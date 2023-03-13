import pandas as pd
import numpy as np
import streamlit as st

# st.write([[1969, '66060','M','U','Low Risk','2 yr Degree', 1]])
st.set_page_config(page_title= "Sqlalchemy Query")

st.image(
    "src/data.jpeg",caption='Query the Data',
     width = 600,
)

col1, col2 = st.columns(2,gap = "medium")
with col1:
    Query_selection = st.radio('Select the Query here:',
                           [ 'Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8']
                           )


with col2:
    if Query_selection == 'Q1':
        st.markdown('#### Numeric Features')
        form = st.form(key='my-form')
        name = form.text_input('Enter your name')
        Cus_by = form.number_input('Customer Birth Year:', 
                        min_value=1924,
                        max_value=2020, 
                        help = 'Please type VALID birth Year!!(Range: 1924~2020)'
                                 
                                )
        
        submit = form.form_submit_button('Submit')

        st.write('Press submit to have your name printed below')
     

    elif Query_selection == 'Q2':
         Cs_zip = st.number_input( 'Customer Zip Code:', 
                    min_value= 601, 
                    max_value= 99981,
                    value  = 66668,
                    step = 1
                            )
    elif Query_selection == 'Q3':
         st.markdown('#### Categorical Features ')
         Cus_gender = st.selectbox('CD_Gender',
                                  ['M', 'F'], 
                                  help= 'M: Male, F: Female'
         )
        
    elif Query_selection == 'Q4':
         st.markdown('#### Categorical Features ')
         Cus_dep = st.selectbox( 'CD_DEP_COUNT',
                     ['0', '1'],
          )
    elif Query_selection == 'Q5':
     st.markdown('#### Categorical Features ')
     Cus_edu = st.selectbox( 'CD_EDUCATION_STATUS',
                     ['Advanced Degree','Secondary','2 yr Degree','4 yr Degree','Unknown','Primary','College']
                    )
        
    elif Query_selection == 'Q6':
     st.markdown('#### Categorical Features ')
     Cus_credit = st.selectbox('CD_CREDIT_RATING',
                     ['Low Risk','Unknown','Good','High Risk']
           )
        
    elif Query_selection == 'Q7':
     st.markdown('#### Categorical Features ')
     Cus_gender = st.selectbox('CD_Gender',
                              ['M', 'F'], 
                              help= 'M: Male, F: Female'
     )
    elif Query_selection == 'Q8':
     st.markdown('#### Categorical Features ')
     Cus_marital = st.selectbox( 'CD_MARITAL_STATUS',
                     ['S','D','W', 'U', 'M'],
         )
     
  
        
        
        
        
    else:
        st.markdown('Please Select your Query')

    
#     submit =  st.button('Submit')
#     reset = st.button('Reset ')
    
    
    
    
   
