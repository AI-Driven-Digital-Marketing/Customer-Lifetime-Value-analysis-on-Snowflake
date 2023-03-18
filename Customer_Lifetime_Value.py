import pandas as pd
import numpy as np
import streamlit as st
import json
from sqlalchemy import create_engine
from datetime import date, timedelta
import time
from snowflake.sqlalchemy import URL
st.set_page_config(page_title= "Sqlalchemy Query")

# initialize the engine and read chached data from json files
@st.cache_resource
def initialize():
    engine = create_engine(URL(
            user='suckFlake',
            password='suckFlake123!',
            account='hn51231.east-us-2.azure',
            database = 'SNOWFLAKE_SAMPLE_DATA',
            schema = 'TPCDS_SF10TCL',
            warehouse = 'COMPUTE_WH',
            role='accountadmin',
            numpy = True
    ))
    
    connection = engine.connect()
    # pd.read_sql_query('''USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;''',engine)
    var_json = json.load(open('var_store.json'))
    return engine, var_json

engine, var_json = initialize()
pd.read_sql_query('''USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL;''',engine)
st.image(
    "src/data.jpeg",
    caption='Query the Data',
     use_column_width  = 'always',
)

# input GUI for user
_,col1,_ = st.columns([1,8,1])
_,col2,_ = st.columns([1,8,1])
#col1, col2 = st.columns(2,gap = "medium")

with col1:
    
    Query_selection = st.selectbox('Select the Query here:',
                            [ 'Q1','Q2','Q3','Q4','Q5','Q40','Q43','Q60']
                           )
    st.markdown('#### Features')
    if Query_selection == 'Q1':
        '''Find customers who have returned items more than 20% more often than the average customer returns for a
store in a given state for a given year.'''
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
        st.write('''Report the increase of weekly web and catalog sales from one year to the next year for each week. That is,
compute the increase of Monday, Tuesday, ... Sunday sales from one year to the following.''')
        form = st.form(key='Q2-form')
        year_input = form.number_input('year',
                                        min_value=1900,
                                        max_value=2100,
                                        value  = 2000,
                                        help = 'Input value not in range.(Range: 1900~2100)')
        submit = form.form_submit_button('Submit')
        
        
    elif Query_selection == 'Q3':
        '''
        Q3: Report the total extended sales price per item brand of a specific manufacturer for all sales in a specific month
        of the year.
        '''
        form = st.form(key='Q3-form')
        month_input = form.number_input('Month',
                                        min_value=1,
                                        max_value=12,
                                        value = 11,
                                        help = 'Input value should in this range.(Range: 1~12)')
        
        MANUFACT_input = form.number_input('MANUFACT',
                                        min_value=12,
                                        max_value=998,
                                        value = 128,
                                        help = 'Input value should in this range.(Range: 12~998)')
        
        aggc_input = form.selectbox('AGGC',var_json['q3_aggc'])                
        
        submit = form.form_submit_button('Submit')
        
        
    elif Query_selection == 'Q4':
        '''Find customers who spend more money via catalog than in stores. Identify preferred customers and their
country of origin.'''
        form = st.form(key='Q4-form')
        year_input = form.number_input('year',
                                        min_value=1900,
                                        max_value=2100,
                                        value  = 2000,
                                        help = 'Input value not in range.(Range: 1900~2100)')
        agg_input = form.selectbox('Aggregation Column',
                                  var_json['q4_agg'])
        
        submit = form.form_submit_button('Submit')
        
    elif Query_selection == 'Q5':
        '''Report sales, profit, return amount, and net loss in the store, catalog, and web channels for a 14-day window.
Rollup results by sales channel and channel specific sales method (store for store sales, catalog page for catalog
sales and web site for web sales)'''
        form = st.form(key='Q40-form')
        SALES_DATE = form.date_input("Select sales date here:",
                                   date(2000, 3, 11),
                                     date(1900, 1, 2),
                                     date(2100, 1, 1),
                                     help = 'accept range from 1900-01-02 to 2100-01-01'
                                    )
        submit = form.form_submit_button('Submit')        
        
    elif Query_selection == 'Q40':
        '''Compute the impact of an item price change on the sales by computing\
        the total sales for items in a 30 day period before and after the price change.\
        Group the items by location of warehouse where they were delivered from.'''
        form = st.form(key='Q40-form')
        SALES_DATE = form.date_input("Select sales date here:",
                                   date(2000, 3, 11),
                                     date(1900, 1, 2),
                                     date(2100, 1, 1),
                                     help = 'accept range from 1900-01-02 to 2100-01-01'
                                    )
        submit = form.form_submit_button('Submit')
        
    elif Query_selection == 'Q43':
        '''Report the sum of all sales from Sunday to Saturday for stores in a given data range by stores.
Qualification Substitution Parameters:'''
        
        form = st.form(key='Q43-form')
        year_input = form.number_input('Year',
                                        min_value=1900,
                                        max_value=2100,
                                        value  = 2000,
                                        help = 'Input value not in range.(Range: 1900~2100)')
        gmt = form.number_input('GMT',
                                min_value=-8.0,
                                max_value=-5.0,
                                value  = -5.0,
                                step=0.1,
                                help = 'Input value not in range.(Range: -8~-5)')
        submit = form.form_submit_button('Submit')
    elif Query_selection == 'Q60':
        
        '''
        Q60: What is the monthly sales amount for a specific month in a specific year, for items in a specific category,
        purchased by customers residing in a specific time zone. Group sales by item and sort output by sales amount.
        '''
        form = st.form(key='Q60-form')

        month_input = form.number_input('Month',
                                            min_value=1,
                                            max_value=12,
                                            value = 9,
                                            help = 'Input value should in this range.(Range: 1~12)')

        year_input = form.number_input('year',
                                            min_value=1900,
                                            max_value=2100,
                                            value  = 1998,
                                            help = 'Input value not in range.(Range: 1900~2100)')

        category_input = form.selectbox('I_Category',["Music","Sports","Children","Women",'None',
                                                              "Home","Electronics","Jewelry","Men","Shoes","Books"])
        submit = form.form_submit_button('Submit')
        
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
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q1 ,engine))
        st.snow()
    elif Query_selection == 'Q2' and submit:
        # the query runs super slow
       
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
        
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q2 ,engine))
        st.snow()
    elif Query_selection == 'Q3' and submit:
#         st.write(pd.read_sql_query('select * from WEB_SALES limit 10;',engine))
        q3 = f'''
        SELECT dt.d_year,
       item.i_brand_id brand_id,
       item.i_brand brand,
       sum(ss_ext_sales_price) sum_agg
FROM date_dim dt,
     store_sales,
     item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = {aggc_input}
  AND item.i_manufact_id = {MANUFACT_input}
  AND dt.d_moy={month_input}
GROUP BY dt.d_year,
         item.i_brand,
         item.i_brand_id
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
LIMIT 100;'''
        
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q3,engine))
        st.snow()
    elif Query_selection == 'Q4' and submit:
        agg_input = 'c_'+agg_input
        alis_input = 'customer_'+agg_input
        
        q4 = '''WITH year_total AS
  (SELECT c_customer_id customer_id,
          c_first_name customer_first_name,
          c_last_name customer_last_name,
          c_preferred_cust_flag customer_preferred_cust_flag,
          c_birth_country customer_birth_country,
          c_login customer_login,
          c_email_address customer_email_address,
          d_year dyear,
          sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total,
          's' sale_type
   FROM customer,
        store_sales,
        date_dim
   WHERE c_customer_sk = ss_customer_sk
     AND ss_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2)) year_total,
                    'c' sale_type
   FROM customer,
        catalog_sales,
        date_dim
   WHERE c_customer_sk = cs_bill_customer_sk
     AND cs_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year
   UNION ALL SELECT c_customer_id customer_id,
                    c_first_name customer_first_name,
                    c_last_name customer_last_name,
                    c_preferred_cust_flag customer_preferred_cust_flag,
                    c_birth_country customer_birth_country,
                    c_login customer_login,
                    c_email_address customer_email_address,
                    d_year dyear,
                    sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)) year_total,
                    'w' sale_type
   FROM customer,
        web_sales,
        date_dim
   WHERE c_customer_sk = ws_bill_customer_sk
     AND ws_sold_date_sk = d_date_sk
   GROUP BY c_customer_id,
            c_first_name,
            c_last_name,
            c_preferred_cust_flag,
            c_birth_country,
            c_login,
            c_email_address,
            d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_preferred_cust_flag
FROM year_total t_s_firstyear,
     year_total t_s_secyear,
     year_total t_c_firstyear,
     year_total t_c_secyear,
     year_total t_w_firstyear,
     year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_c_secyear.customer_id
  AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_c_firstyear.sale_type = 'c'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_c_secyear.sale_type = 'c'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.dyear = 2001
  AND t_s_secyear.dyear = 2001+1
  AND t_c_firstyear.dyear = 2001
  AND t_c_secyear.dyear = 2001+1
  AND t_w_firstyear.dyear = 2001
  AND t_w_secyear.dyear = 2001+1
  AND t_s_firstyear.year_total > 0
  AND t_c_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END > CASE
                WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                ELSE NULL
            END
  AND CASE
          WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
          ELSE NULL
      END > CASE
                WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
                ELSE NULL
            END
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_preferred_cust_flag
LIMIT 100;'''
        q4 = q4.replace('c_preferred_cust_flag', agg_input).replace('customer_preferred_cust_flag', alis_input).replace('2001', str(year_input))
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q4,engine))
        st.snow()
    elif Query_selection == 'Q5' and submit:
        with open('src/05.txt', 'r') as file:
            q5 = file.read()
        q5 = q5.replace('2000-08-23', str(SALES_DATE)).replace('2000-09-06',str(SALES_DATE+timedelta(days=14)))
        
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q5,engine))
        st.snow()
    elif Query_selection == 'Q40' and submit:
        q40 = f'''SELECT w_state,
       i_item_id,
       sum(CASE
               WHEN (cast(d_date AS date) < CAST (\'{SALES_DATE}\' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash,0)
               ELSE 0
           END) AS sales_before,
       sum(CASE
               WHEN (cast(d_date AS date) >= CAST (\'{SALES_DATE}\' AS date)) THEN cs_sales_price - coalesce(cr_refunded_cash,0)
               ELSE 0
           END) AS sales_after
FROM catalog_sales
LEFT OUTER JOIN catalog_returns ON (cs_order_number = cr_order_number
                                    AND cs_item_sk = cr_item_sk) ,warehouse,
                                                                  item,
                                                                  date_dim
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND i_item_sk = cs_item_sk
  AND cs_warehouse_sk = w_warehouse_sk
  AND cs_sold_date_sk = d_date_sk
  AND d_date BETWEEN CAST (\'{SALES_DATE-timedelta(days=60)}\' AS date) AND CAST (\'{SALES_DATE+timedelta(days=60)}\' AS date)
GROUP BY w_state,
         i_item_id
ORDER BY w_state,
         i_item_id
LIMIT 100;

        '''
        
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q40,engine))
        st.snow()
    elif Query_selection == 'Q43' and submit:
        q43 = f'''
SELECT s_store_name,
       s_store_id,
       sum(CASE
               WHEN (d_day_name='Sunday') THEN ss_sales_price
               ELSE NULL
           END) sun_sales,
       sum(CASE
               WHEN (d_day_name='Monday') THEN ss_sales_price
               ELSE NULL
           END) mon_sales,
       sum(CASE
               WHEN (d_day_name='Tuesday') THEN ss_sales_price
               ELSE NULL
           END) tue_sales,
       sum(CASE
               WHEN (d_day_name='Wednesday') THEN ss_sales_price
               ELSE NULL
           END) wed_sales,
       sum(CASE
               WHEN (d_day_name='Thursday') THEN ss_sales_price
               ELSE NULL
           END) thu_sales,
       sum(CASE
               WHEN (d_day_name='Friday') THEN ss_sales_price
               ELSE NULL
           END) fri_sales,
       sum(CASE
               WHEN (d_day_name='Saturday') THEN ss_sales_price
               ELSE NULL
           END) sat_sales
FROM date_dim,
     store_sales,
     store
WHERE d_date_sk = ss_sold_date_sk
  AND s_store_sk = ss_store_sk
  AND s_gmt_offset = {gmt}
  AND d_year = {year_input}
GROUP BY s_store_name,
         s_store_id
ORDER BY s_store_name,
         s_store_id,
         sun_sales,
         mon_sales,
         tue_sales,
         wed_sales,
         thu_sales,
         fri_sales,
         sat_sales
LIMIT 100;
'''
        with st.spinner('Wait for it...'):
            st.write(pd.read_sql_query(q43,engine))
        st.snow()
    elif Query_selection == 'Q60' and submit:
        q60 = f'''
                WITH ss AS
        (SELECT i_item_id,
                sum(ss_ext_sales_price) total_sales
        FROM store_sales,
                date_dim,
                customer_address,
                item
        WHERE i_item_id IN
            (SELECT i_item_id
                FROM item
                WHERE i_category ={category_input})
            AND ss_item_sk = i_item_sk
            AND ss_sold_date_sk = d_date_sk
            AND d_year = {year_input}
            AND d_moy = {month_input}
            AND ss_addr_sk = ca_address_sk
            AND ca_gmt_offset = -5
        GROUP BY i_item_id limit 1000) limit 1000,
            cs AS
        (SELECT i_item_id,
                sum(cs_ext_sales_price) total_sales
        FROM catalog_sales,
                date_dim,
                customer_address,
                item
        WHERE i_item_id IN
            (SELECT i_item_id
                FROM item
                WHERE i_category ={category_input} limit 1000)
            AND cs_item_sk = i_item_sk
            AND cs_sold_date_sk = d_date_sk
            AND d_year = {year_input}
            AND d_moy = {month_input}
            AND cs_bill_addr_sk = ca_address_sk
            AND ca_gmt_offset = -5
        GROUP BY i_item_id ) limit 1000,
            ws AS
        (SELECT i_item_id,
                sum(ws_ext_sales_price) total_sales
        FROM web_sales,
                date_dim,
                customer_address,
                item
        WHERE i_item_id IN
            (SELECT i_item_id
                FROM item
                WHERE i_category = {category_input} limit 1000)
            AND ws_item_sk = i_item_sk
            AND ws_sold_date_sk = d_date_sk
            AND d_year = {year_input}
            AND d_moy = {month_input}
            AND ws_bill_addr_sk = ca_address_sk
            AND ca_gmt_offset = -5
        GROUP BY i_item_id
        limit 1000)
        
        SELECT i_item_id,
            sum(total_sales) total_sales
        FROM
        (SELECT *
        FROM ss
        UNION ALL SELECT *
        FROM cs
        UNION ALL SELECT *
        FROM ws limit 1000) tmp1
        GROUP BY i_item_id
        ORDER BY i_item_id,
                total_sales
        LIMIT 100;        
        '''
        with st.spinner('Wait for it...'):
            
            st.write(pd.read_sql_query(q60,engine))
    elif submit:
        st.write('Wrong Question Number.')

    


