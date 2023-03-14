import pandas as pd
import numpy as np
import streamlit as st
import json
from sqlalchemy import create_engine
st.set_page_config(page_title= "Sqlalchemy Query")

# initialize the engine and read chached data from json files
@st.cache_resource
def initialize():
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
    pd.read_sql_query('''USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;''',engine)
    var_json = json.load(open('var_store.json'))
    return engine, var_json

engine, var_json = initialize()

st.image(
    "src/data.jpeg",
    caption='Query the Data',
     use_column_width  = 'always',
)

# input GUI for user
# _,col1,_ = st.columns([1,8,1])
# _,col2,_ = st.columns([1,8,1])
col1, col2 = st.columns(2,gap = "medium")

with col1:
    # lai's contribution here
    Query_selection = st.selectbox('Select the Query here:',
                            [ 'Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8']
                           )
    st.markdown('#### Features')
    if Query_selection == 'Q1':
        
        form = st.form(key='Q1-form')
        year_input = form.number_input('year',
                                        min_value=1900,
                                        max_value=2100,
                                        value  = 2000,
                                        help = 'Input value not in range.(Range: 1900~2100)')
        state_input = form.selectbox('State', 
                                     var_json['q1_state']
                                     )
        
        agg_input = form.selectbox('Aggreagation Column',var_json['q1_agg'])

        submit = form.form_submit_button('Submit')

    elif Query_selection == 'Q2':
        form = st.form(key='Q2-form')
        year_input = form.number_input('year',
                                        min_value=1900,
                                        max_value=2100,
                                        value  = 2000,
                                        help = 'Input value not in range.(Range: 1900~2100)')
        submit = form.form_submit_button('Submit')
        
        
    elif Query_selection == 'Q3':
        pass
        
    elif Query_selection == 'Q4':
        pass
        
    elif Query_selection == 'Q5':
        pass
        
    elif Query_selection == 'Q6':
        pass
        
    elif Query_selection == 'Q7':
        pass
        
    elif Query_selection == 'Q8':
        pass
        
    else:
        st.markdown('Please Select your Query')
    st.write('Press submit run the query for chosen question.')

# outputs if click submit
with col2:
    if Query_selection == 'Q1' and submit:
        q1_state = state_input
        q1_year = year_input
        q1_agg = agg_input
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
                 limit 100;'''.format(year = q1_year, state = q1_state, agg = q1_agg)
        
        st.write(pd.read_sql_query(q1 ,engine))
    
    elif Query_selection == 'Q2' and submit:
        # the query runs super slow
        st.write(pd.read_sql_query('select * from WEB_SALES limit 10;',engine))
        q2 = f'''WITH wscs AS
  (SELECT sold_date_sk,
          sales_price
   FROM
     (SELECT ws_sold_date_sk sold_date_sk,
             ws_ext_sales_price sales_price
      FROM web_sales
      UNION ALL SELECT cs_sold_date_sk sold_date_sk,
                       cs_ext_sales_price sales_price
      FROM catalog_sales) sq1),
     wswscs AS
  (SELECT d_week_seq,
          sum(CASE
                  WHEN (d_day_name='Sunday') THEN sales_price
                  ELSE NULL
              END) sun_sales,
          sum(CASE
                  WHEN (d_day_name='Monday') THEN sales_price
                  ELSE NULL
              END) mon_sales,
          sum(CASE
                  WHEN (d_day_name='Tuesday') THEN sales_price
                  ELSE NULL
              END) tue_sales,
          sum(CASE
                  WHEN (d_day_name='Wednesday') THEN sales_price
                  ELSE NULL
              END) wed_sales,
          sum(CASE
                  WHEN (d_day_name='Thursday') THEN sales_price
                  ELSE NULL
              END) thu_sales,
          sum(CASE
                  WHEN (d_day_name='Friday') THEN sales_price
                  ELSE NULL
              END) fri_sales,
          sum(CASE
                  WHEN (d_day_name='Saturday') THEN sales_price
                  ELSE NULL
              END) sat_sales
   FROM wscs,
        date_dim
   WHERE d_date_sk = sold_date_sk
   GROUP BY d_week_seq
   limit 1000)
SELECT d_week_seq1,
       round(sun_sales1/sun_sales2, 2),
       round(mon_sales1/mon_sales2, 2),
       round(tue_sales1/tue_sales2, 2),
       round(wed_sales1/wed_sales2, 2),
       round(thu_sales1/thu_sales2, 2),
       round(fri_sales1/fri_sales2, 2),
       round(sat_sales1/sat_sales2, 2)
FROM
  (SELECT wswscs.d_week_seq d_week_seq1,
          sun_sales sun_sales1,
          mon_sales mon_sales1,
          tue_sales tue_sales1,
          wed_sales wed_sales1,
          thu_sales thu_sales1,
          fri_sales fri_sales1,
          sat_sales sat_sales1
   FROM wswscs,
        date_dim
   WHERE date_dim.d_week_seq = wswscs.d_week_seq
     AND d_year = {year_input}) y,
  (SELECT wswscs.d_week_seq d_week_seq2,
          sun_sales sun_sales2,
          mon_sales mon_sales2,
          tue_sales tue_sales2,
          wed_sales wed_sales2,
          thu_sales thu_sales2,
          fri_sales fri_sales2,
          sat_sales sat_sales2
   FROM wswscs,
        date_dim
   WHERE date_dim.d_week_seq = wswscs.d_week_seq
     AND d_year = {year_input}+1) z
WHERE d_week_seq1 = d_week_seq2-53
ORDER BY d_week_seq1
limit 1000;
'''
        st.write(pd.read_sql_query(q2 ,engine))
    elif Query_selection == 'Q3' and submit:
        pass
    elif Query_selection == 'Q4' and submit:
        pass
    elif Query_selection == 'Q5' and submit:
        pass
    elif Query_selection == 'Q6' and submit:
        pass
    elif Query_selection == 'Q7' and submit:
        pass
    elif Query_selection == 'Q8' and submit:
        pass
    elif submit:
        st.write('Wrong Question Number.')

    

