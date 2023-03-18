import pandas as pd
import streamlit as st
import json
import numpy as np
import sys
import cachetools
import joblib
# from streamlit.report_thread import get_report_ctx
# from streamlit.server.server import Server
import snowflake.snowpark
from snowflake.snowpark import functions as F
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import PandasDataFrame,PandasSeries
from snowflake.snowpark.functions import udf, sum, col,array_construct,month,year,call_udf,lit,count
from snowflake.snowpark.version import VERSION

st.set_page_config(page_title= "Customer Lifetime Value")

st.image(
    "src/1649251328-maximize-your-clv.webp",
    width = 600,
)

#st.title("XGBoost model to predict CLV")
with st.expander("XGboost Metrics"):
    st.write('''
  "Training":    
  {
    "MAPE": 0.10485963767796148,    
    "R2": 0.0002156159931520074,    
    "RMSE": 3648.279975985239    
  },    
  "Test":    
  {
    "MAPE": 0.10488693273468783,    
    "R2": -4.803458124280624e-05,    
    "RMSE": 3649.372248018043    
  }
''')
with st.expander("Linear Regression Metrics"):
    st.write('''
  "Training":    
  {
    "MAPE": 0.10487981806292808,    
    "R2": 0.00018172684338746414,    
    "RMSE": 3648.985233540816    
  },    
  "Test":    
  {
    "MAPE": 0.10488251067703308,    
    "R2": -0.00020184804454337346,    
    "RMSE": 3648.8711500677236    
  }
''')
col1, col2 = st.columns(2,gap = "medium")
with col1:
    st.markdown('#### Numeric Features')
    Cus_by = st.number_input('Customer Birth Year:', 
                    min_value=1924,
                    max_value=2020, 
                     value  = 1998,        
                    help = 'Please type VALID birth Year!!(Range: 1924~2020)'
                            )

    Cs_zip = st.number_input( 'Customer Zip Code:', 
                    min_value= 601, 
                    max_value= 99981,
                    value  = 66668,
                    step = 1
                            )


with col2:
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
    model_select = st.radio('Select the Model here:',
                           [ 'XGBoost','Linear Regression']
                            #[ 'XGBoost']
                           )
with col4:
    submit =  st.button('Submit')
    reset = st.button('Reset ')
@st.cache_resource
def initialize_SF():
    connection_parameters = json.load(open('connection.json'))
    session = Session.builder.configs(connection_parameters).create()
    session.sql_simplifier_enabled = True
    ca_zip = json.load(open('src/zip_json.json'))    
    # set up feature engineering/inference warehouse
    session.use_warehouse('FE_AND_INFERENCE_WH')
    session.use_database('tpcds_xgboost')
    session.use_schema('demo')
    session.add_packages('snowflake-snowpark-python', 'scikit-learn', 'pandas', 'numpy', 'joblib', 'cachetools', 'xgboost', 'joblib')
    session.add_import("@ml_models_10T/model.joblib")  
    session.add_import("@ml_models_LR_10T/model_LR.joblib")
    return session, ca_zip
session, ca_zip = initialize_SF()

# choose model here
if model_select == 'XGBoost':    
    stage_name = 'ml_models_10T'
    model_name = 'model.joblib'
    
else:
    stage_name = 'ml_models_LR_10T'
    model_name = 'model_LR.joblib'

features = [ 'C_BIRTH_YEAR', 'CA_ZIP', 'CD_GENDER', 'CD_MARITAL_STATUS', 'CD_CREDIT_RATING', 'CD_EDUCATION_STATUS', 'CD_DEP_COUNT']

@cachetools.cached(cache={})
def read_file(filename):
    import os, joblib
    import_dir = sys._xoptions.get("snowflake_import_directory")
    if import_dir:
        with open(os.path.join(import_dir, filename), 'rb') as file:
            m = joblib.load(file)
            return m

@F.pandas_udf(session=session, max_batch_size=10000, is_permanent=True, stage_location=f'@{stage_name}', replace=True, name="clv_xgboost_udf")
def predict(df:  PandasDataFrame[int, str, str, str, str, str, int]) -> PandasSeries[float]:
    m = read_file(model_name)       
    df.columns = features
    return m.predict(df) 

# if click submit
if submit:
    typed_input = [[Cus_by, ca_zip.get(str(Cs_zip), '66668'),Cus_gender,Cus_marital,Cus_credit,Cus_edu, int(Cus_dep)]]
    #st.write(typed_input)
    input_df = session.create_dataframe(typed_input, schema=features)
    typed_output = input_df.select(*input_df,
                    predict(*input_df).alias('PREDICTION'))
    
    output = pd.DataFrame(typed_output.collect()).T
    output.columns = ['']
    st.write(output)
# if click reset  
# if reset:
#     st.write('放弃reset？')
    

    
