#!/usr/bin/env python
# coding: utf-8

# In[14]:


import snowflake.snowpark
from snowflake.snowpark import functions as F
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType, StringType, StructType, FloatType, StructField, DateType, Variant
from snowflake.snowpark.functions import udf, sum, col,array_construct,month,year,call_udf,lit,count
from snowflake.snowpark.version import VERSION
# Misc
import json
import pandas as pd
import numpy as np
import logging 
logger = logging.getLogger("snowflake.snowpark.session")
logger.setLevel(logging.ERROR)


# In[15]:


# Create Snowflake Session object
connection_parameters = json.load(open('connection.json'))
session = Session.builder.configs(connection_parameters).create()
session.sql_simplifier_enabled = True

snowflake_environment = session.sql('select current_user(), current_role(), current_database(), current_schema(), current_version(), current_warehouse()').collect()
snowpark_version = VERSION

# # Current Environment Details
# print('User                        : {}'.format(snowflake_environment[0][0]))
# print('Role                        : {}'.format(snowflake_environment[0][1]))
# print('Database                    : {}'.format(snowflake_environment[0][2]))
# print('Schema                      : {}'.format(snowflake_environment[0][3]))
# print('Warehouse                   : {}'.format(snowflake_environment[0][5]))
# print('Snowflake version           : {}'.format(snowflake_environment[0][4]))
# print('Snowpark for Python version : {}.{}.{}'.format(snowpark_version[0],snowpark_version[1],snowpark_version[2]))


# In[16]:


ca_zip = json.load(open('src/zip_json.json'))


# In[17]:


get_ipython().run_cell_magic('writefile', 'pages/CLV_prediction.py', 'import pandas as pd\nimport streamlit as st\nimport json\nimport numpy as np\nimport sys\nimport cachetools\nimport joblib\n# from streamlit.report_thread import get_report_ctx\n# from streamlit.server.server import Server\nimport snowflake.snowpark\nfrom snowflake.snowpark import functions as F\nfrom snowflake.snowpark.session import Session\nfrom snowflake.snowpark.types import PandasDataFrame,PandasSeries\nfrom snowflake.snowpark.functions import udf, sum, col,array_construct,month,year,call_udf,lit,count\nfrom snowflake.snowpark.version import VERSION\n\nst.set_page_config(page_title= "Customer Lifetime Value")\n\nst.image(\n    "src/1649251328-maximize-your-clv.webp",\n    width = 600,\n)\n\n#st.title("XGBoost model to predict CLV")\n\ncol1, col2 = st.columns(2,gap = "medium")\nwith col1:\n    st.markdown(\'#### Numeric Features\')\n    Cus_by = st.number_input(\'Customer Birth Year:\', \n                    min_value=1924,\n                    max_value=2020, \n                    help = \'Please type VALID birth Year!!(Range: 1924~2020)\'\n                            )\n\n    Cs_zip = st.number_input( \'Customer Zip Code:\', \n                    min_value= 601, \n                    max_value= 99981,\n                    value  = 66668,\n                    step = 1\n                            )\n\n\nwith col2:\n    st.markdown(\'#### Categorical Features \')\n    Cus_gender = st.selectbox(\'CD_Gender\',\n                              [\'M\', \'F\'], \n                              help= \'M: Male, F: Female\'\n     )\n\n    Cus_marital = st.selectbox( \'CD_MARITAL_STATUS\',\n                 [\'S\',\'D\',\'W\', \'U\', \'M\'],\n     )\n    Cus_credit = st.selectbox(\'CD_CREDIT_RATING\',\n                 [\'Low Risk\',\'Unknown\',\'Good\',\'High Risk\']\n       )\n\n    Cus_edu = st.selectbox( \'CD_EDUCATION_STATUS\',\n                 [\'Advanced Degree\',\'Secondary\',\'2 yr Degree\',\'4 yr Degree\',\'Unknown\',\'Primary\',\'College\']\n                )\n    Cus_dep = st.selectbox( \'CD_DEP_COUNT\',\n                 [\'0\', \'1\'],\n      )\n\n\n\ncol3, col4 = st.columns([8,2],gap = "medium")\n\nwith col3:\n    model_select = st.radio(\'Select the Model here:\',\n                           [ \'XGBoost\',\'Linear Regression\']\n                            #[ \'XGBoost\']\n                           )\nwith col4:\n    submit =  st.button(\'Submit\')\n    reset = st.button(\'Reset \')\n@st.cache_resource\ndef initialize_SF():\n    connection_parameters = json.load(open(\'connection.json\'))\n    session = Session.builder.configs(connection_parameters).create()\n    session.sql_simplifier_enabled = True\n    ca_zip = json.load(open(\'src/zip_json.json\'))    \n    # set up feature engineering/inference warehouse\n    session.use_warehouse(\'FE_AND_INFERENCE_WH\')\n    session.use_database(\'tpcds_xgboost\')\n    session.use_schema(\'demo\')\n    session.add_packages(\'snowflake-snowpark-python\', \'scikit-learn\', \'pandas\', \'numpy\', \'joblib\', \'cachetools\', \'xgboost==1.5.0\', \'joblib\')\n    session.add_import("@ml_models_10T/model.joblib")  \n    session.add_import("@ml_models_LR_10T/model_LR.joblib")\n    return session, ca_zip\nsession, ca_zip = initialize_SF()\n\n# choose model here\nif model_select == \'XGBoost\':    \n    stage_name = \'ml_models_10T\'\n    model_name = \'model.joblib\'\n    \nelse:\n    stage_name = \'ml_models_LR_10T\'\n    model_name = \'model_LR.joblib\'\n\nfeatures = [ \'C_BIRTH_YEAR\', \'CA_ZIP\', \'CD_GENDER\', \'CD_MARITAL_STATUS\', \'CD_CREDIT_RATING\', \'CD_EDUCATION_STATUS\', \'CD_DEP_COUNT\']\n\n@cachetools.cached(cache={})\ndef read_file(filename):\n    import os, joblib\n    import_dir = sys._xoptions.get("snowflake_import_directory")\n    if import_dir:\n        with open(os.path.join(import_dir, filename), \'rb\') as file:\n            m = joblib.load(file)\n            return m\n\n@F.pandas_udf(session=session, max_batch_size=10000, is_permanent=True, stage_location=f\'@{stage_name}\', replace=True, name="clv_xgboost_udf")\ndef predict(df:  PandasDataFrame[int, str, str, str, str, str, int]) -> PandasSeries[float]:\n    m = read_file(model_name)       \n    df.columns = features\n    return m.predict(df) \n\n# if click submit\nif submit:\n    typed_input = [[Cus_by, ca_zip.get(str(Cs_zip), \'66668\'),Cus_gender,Cus_marital,Cus_credit,Cus_edu, int(Cus_dep)]]\n    #st.write(typed_input)\n    input_df = session.create_dataframe(typed_input, schema=features)\n    typed_output = input_df.select(*input_df,\n                    predict(*input_df).alias(\'PREDICTION\'))\n    output = pd.DataFrame(typed_output.collect()).T\n    output.columns = [\'\']\n    st.write(output)\n# if click reset  \nif reset:\n    st.write(\'放弃reset？\')\n    \n\n    \n')


# In[18]:


get_ipython().run_cell_magic('writefile', 'Customer_Lifetime_Value.py', 'import pandas as pd\nimport numpy as np\nimport streamlit as st\n\n# st.write([[1969, \'66060\',\'M\',\'U\',\'Low Risk\',\'2 yr Degree\', 1]])\nst.set_page_config(page_title= "Sqlalchemy Query")\n\nst.image(\n    "src/data.jpeg",caption=\'Query the Data\',\n     width = 600,\n)\n\ncol1, col2 = st.columns(2,gap = "medium")\nwith col1:\n    Query_selection = st.selectbox(\'Select the Query here:\',\n#   Query_selection = st.radio(\'Select the Query here:\',               \n                           [ \'Q1\',\'Q2\',\'Q3\',\'Q4\',\'Q5\',\'Q6\',\'Q7\',\'Q8\']\n                           )\n\n\nwith col2:\n    if Query_selection == \'Q1\':\n        st.markdown(\'#### Numeric Features\')\n        form = st.form(key=\'my-form\')\n        name = form.text_input(\'Enter your name\')\n        Cus_by = form.number_input(\'Customer Birth Year:\', \n                        min_value=1924,\n                        max_value=2020, \n                        help = \'Please type VALID birth Year!!(Range: 1924~2020)\'\n                                 \n                                )\n        \n        submit = form.form_submit_button(\'Submit\')\n\n        st.write(\'Press submit to have your name printed below\')\n     \n\n    elif Query_selection == \'Q2\':\n         Cs_zip = st.number_input( \'Customer Zip Code:\', \n                    min_value= 601, \n                    max_value= 99981,\n                    value  = 66668,\n                    step = 1\n                            )\n    elif Query_selection == \'Q3\':\n         st.markdown(\'#### Categorical Features \')\n         Cus_gender = st.selectbox(\'CD_Gender\',\n                                  [\'M\', \'F\'], \n                                  help= \'M: Male, F: Female\'\n         )\n        \n    elif Query_selection == \'Q4\':\n         st.markdown(\'#### Categorical Features \')\n         Cus_dep = st.selectbox( \'CD_DEP_COUNT\',\n                     [\'0\', \'1\'],\n          )\n    elif Query_selection == \'Q5\':\n     st.markdown(\'#### Categorical Features \')\n     Cus_edu = st.selectbox( \'CD_EDUCATION_STATUS\',\n                     [\'Advanced Degree\',\'Secondary\',\'2 yr Degree\',\'4 yr Degree\',\'Unknown\',\'Primary\',\'College\']\n                    )\n        \n    elif Query_selection == \'Q6\':\n     st.markdown(\'#### Categorical Features \')\n     Cus_credit = st.selectbox(\'CD_CREDIT_RATING\',\n                     [\'Low Risk\',\'Unknown\',\'Good\',\'High Risk\']\n           )\n        \n    elif Query_selection == \'Q7\':\n     st.markdown(\'#### Categorical Features \')\n     Cus_gender = st.selectbox(\'CD_Gender\',\n                              [\'M\', \'F\'], \n                              help= \'M: Male, F: Female\'\n     )\n    elif Query_selection == \'Q8\':\n     st.markdown(\'#### Categorical Features \')\n     Cus_marital = st.selectbox( \'CD_MARITAL_STATUS\',\n                     [\'S\',\'D\',\'W\', \'U\', \'M\'],\n         )\n     \n  \n        \n        \n        \n        \n    else:\n        st.markdown(\'Please Select your Query\')\n\n    \n#     submit =  st.button(\'Submit\')\n#     reset = st.button(\'Reset \')\n    \n    \n    \n#st.write(\'## Introduction\')\n')


# In[ ]:




