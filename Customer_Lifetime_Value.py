import pandas as pd
import numpy as np
import streamlit as st
import json


#--Alchemy--
from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account}/d'.format(
        user='ESTPEGION',
        password='SnowFlake1234!',
        account='mg61873.ca-central-1.aws',
        database = 'SNOWFLAKE_SAMPLE_DATA',
        schema = 'TPCDS_SF10TCL',
        warehouse = 'COMPUTE_WH',
        role='accountadmin',
        numpy = True
    )
)

connection = engine.connect()
results = connection.execute('select current_version()').fetchone()


pd.read_sql_query('''USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;''',engine)


#--Alchemy End--

#import var_store
var_json = json.load(open('var_store.json'))




# st.write([[1969, '66060','M','U','Low Risk','2 yr Degree', 1]])
st.set_page_config(page_title= "Sqlalchemy Query")

st.image(
    "src/data.jpeg",caption='Query the Data',
     width = 600,
)

col1, col2 = st.columns(2,gap = "medium")
with col1:
    Query_selection = st.selectbox('Select the Query here:',
#   Query_selection = st.radio('Select the Query here:',               
                           [ 'Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8']
                           )


#with col2:
    if Query_selection == 'Q1':
        st.markdown('#### Features')
        form = st.form(key='my-form')
        year_input = form.number_input('year',min_value=1900,
                    max_value=2100, 
                    help = 'Input value not in range.(Range: 1900~2100)')
        state_input = form.selectbox('State', var_json['q1_state']
                      )
        agg_input = form.selectbox('Aggreagation Column',var_json['q1_agg'])
#         Cus_credit = st.selectbox('CD_CREDIT_RATING',
#                  ['Low Risk','Unknown','Good','High Risk']
#        )

#         Cus_by = form.number_input('Customer Birth Year:', 
#                         min_value=1924,
#                         max_value=2020, 
#                         help = 'Please type VALID birth Year!!(Range: 1924~2020)'
#                                 )

        
        submit = form.form_submit_button('Submit')

        st.write('Press submit to have your name printed below')
        
        if submit:
            q1_state = state_input
            q1_year = year_input
            q1_agg = agg_input

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
    
    
    
with col2:
    if Query_selection == 'Q1' and submit:
        q1 = '''with customer_total_return as
                (select sr_customer_sk as ctr_customer_sk
                ,sr_store_sk as ctr_store_sk
                ,sum({agg}) as ctr_total_return
                from store_returns
                ,date_dim
                where sr_returned_date_sk = d_date_sk
                and d_year = {year}
                group by sr_customer_sk
                ,sr_store_sk
                limit 500)
                 select  c_customer_id
                from customer_total_return ctr1
                ,store
                ,customer
                where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
                from customer_total_return ctr2
                where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
                and s_store_sk = ctr1.ctr_store_sk
                and s_state = \'{state}\'
                and ctr1.ctr_customer_sk = c_customer_sk
                order by c_customer_id
                 limit 5;'''.format(year = q1_year, state = q1_state, agg=q1_agg)
        
        st.write(pd.read_sql_query(q1 ,engine))
    
    
    
    
#st.write('## Introduction')
