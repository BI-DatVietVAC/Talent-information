import streamlit as st
import pandas as pd
import numpy as np
from pytz import country_names
from st_aggrid import AgGrid, GridUpdateMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect
from urllib.parse import urlparse
import psycopg2
import psycopg2.extras as extras
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
from datetime import date, datetime
from sqlalchemy import create_engine
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)

postgres_uri = st.secrets['database']['postgres_uri']


def nan_to_null(f,
                _NULL=psycopg2.extensions.AsIs('NULL'),
                _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL

def create_postgres_conn(postgres_uri = postgres_uri):
    parsed_uri = urlparse(postgres_uri)
    psycopg2.extensions.register_adapter(float, nan_to_null)
    conn = psycopg2.connect(
        host=parsed_uri.hostname,
        port=parsed_uri.port,
        database=parsed_uri.path[1:],
        user=parsed_uri.username,
        password=parsed_uri.password)
    return conn

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    modify = st.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            left.write("↳")
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    _min,
                    _max,
                    (_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].str.contains(user_text_input)]

    return df

@st.experimental_memo
def load_data(table_name = 'bi_talent_information_test'):
    conn = create_postgres_conn()
    cur = conn.cursor()
    SQL = f'''
    SELECT * FROM {table_name}
    '''
    cur.execute(SQL)
    rows = cur.fetchall()
    df = pd.DataFrame.from_records(rows, 
                                   columns=['talent_code','talent_name','full_name','level','gender','last_modified',
                                            'birthday','job_title','award','street','ward','district','province','region',
                                            'agent','height','weight','size_shoes','phone','urls','talent_country']
                                   )
    conn.close()
    return df

@st.experimental_memo
def to_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0.00'}) 
    worksheet.set_column('A:A', None, format1)  
    writer.save()
    processed_data = output.getvalue()
    return processed_data

def check_existed_talent(df, action_type = 'Import'):
    talent_name = tuple([row.upper() for row in list(df["talent_name"].unique())]) if len(list(df["talent_name"].unique())) > 1 else f'''  ('{list(df["talent_name"].unique())[-1].upper()}')  '''
    talent_code = tuple([row.upper() for row in list(df["talent_code"].unique())]) if len(list(df["talent_code"].unique())) > 1 else f'''  ('{list(df["talent_code"].unique())[-1].upper()}')  '''
    conn = create_postgres_conn()
    cur = conn.cursor()
    if action_type == 'Import':
        action_sql = f'''
                WHERE UPPER(talent_name) IN {talent_name}
            OR UPPER(talent_code) IN {talent_code}
            '''
    elif action_type == 'Update':
        action_sql = f'''
                WHERE UPPER(talent_code) IN {talent_code}
            '''
    SQL = f'''
    SELECT * FROM bi_talent_information_test
            {action_sql}
    '''
    cur.execute(SQL)
    rows = cur.fetchall()
    conn.close() 
    if len(rows) == 0:
        if action_type == 'Import': 
            st.write(f" 👍 👍  Talent Name : {', '.join(df['talent_name'].unique().tolist())} is not in Talent information")
        elif action_type == 'Update':
            st.write(f" 🧨🧨  Talent Code : {', '.join(df['talent_code'].unique().tolist())} is not in Talent information. You must make sure Talent code is existed to Update information!!!")
        return True
    else:
        df = pd.DataFrame.from_records(rows, 
                                   columns=['talent_code','talent_name','full_name','level','gender','last_modified',
                                            'birthday','job_title','award','street','ward','district','province','region',
                                            'agent','height','weight','size_shoes','phone','urls','talent_country']
                                    )
        if action_type == 'Import':
            st.write(f"🧨🧨  Talent Name Or Talent Code : {', '.join(df['talent_name'].unique().tolist())} Or {', '.join(df['talent_code'].unique().tolist())} are existed in Talent information. Please help to double check before import!!!")
        elif action_type == 'Update':
            st.write(f"🧨🧨  Talent Code : {', '.join(df['talent_code'].unique().tolist())} are existed in Talent information!!!")
        df_xlsx = to_excel(df)

        st.download_button(
        "Press to Download",
        df_xlsx,
        "file_talent_duplicate.xlsx",
        "application/vnd.ms-excel"
        )
        return False

# The code below is for the title and logo.
st.set_page_config(page_title="Dataframe with editable cells", page_icon="💾")
#conn = init_connection()
df = load_data()

st.title("Dataframe with editable cells")
st.write("")
st.markdown(
    """This is a demo of a dataframe with editable cells, powered by 
[streamlit-aggrid](https://pypi.org/project/streamlit-aggrid/). 
You can edit the cells by clicking on them, and then export 
your selection to a `.csv` file (or send it to your Snowflake DB!)"""
)

st.dataframe(filter_dataframe(df))

st.write("")
st.write("")

st.info("💡 Please make sure to follow by format of the below sample!!!")

df_xlsx = to_excel(df.head(1))

st.download_button(
   "Press to Download the Sample",
   df_xlsx,
   "file_talent_sample.xlsx",
   "application/vnd.ms-excel"
)
st.write("")
st.write("")
st.subheader("① Choose a new talent file")

flag_import = False
flag_update = True
flag_uploadfile = False



def insert_db(import_df, table_name = 'bi_talent_information_test'):
        
    conn = create_postgres_conn()
    tuples = [tuple(x) for x in import_df.to_numpy()]
    cols = ','.join(list(import_df.columns))
    query = """
        INSERT INTO %s as f (%s)
        VALUES %%s
        ;""" % (table_name, cols)
    cursor = conn.cursor()
    extras.execute_values(cursor, query, tuples)
    conn.commit()
    st.write(f"Insert into successful !!!")
    conn.close()

def update_db( update_df ,table_name = 'bi_talent_information_test'):
    talent_code = tuple([row.upper() for row in list(update_df["talent_code"].unique())]) if len(list(update_df["talent_code"].unique())) > 1 else f'''  ('{list(update_df["talent_code"].unique())[-1].upper()}')  '''
    conn = create_postgres_conn()
    query = f'''
        DELETE FROM {table_name}
        WHERE talent_code IN {talent_code}

        '''
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    engine = create_engine(postgres_uri)
    update_df.to_sql(table_name, con=engine, if_exists='append', index=False)
    st.write(f"Update successful !!!")
    conn.close()  


uploaded_file = st.file_uploader("")
if uploaded_file is not None:
    flag_uploadfile = True
    format_file = uploaded_file.name.split(".")[-1]
    if format_file not in ['csv','xlsx']:
        st.write("Please choose file again. Just support XLSX and CSV files!!!")
    else:
        if format_file == 'xlsx':
            uploaded_file_df = pd.read_excel(uploaded_file)
        else:
            uploaded_file_df = pd.read_csv(uploaded_file)
        uploaded_file_df["last_modified"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.write(uploaded_file_df)

        

if flag_uploadfile:
    option = st.sidebar.radio('How would you like to choose the action?', options=['Import', 'Update'], index=0, horizontal=True)
    
    
    if option == 'Import':
        addline = st.sidebar.radio('Check the data in file!!!', options=['yes', 'no'], index=1, horizontal=True)
        if addline == 'yes':
            flag_import = check_existed_talent(uploaded_file_df)
            if flag_import:
                st.sidebar.button("Import", on_click=insert_db(uploaded_file_df))
        elif addline == 'no':
            st.write(" 💡 You only have one way to Import the file, which is to check the file before Importing the file!!! ")


    elif option == 'Update':
        addline = st.sidebar.radio('Check the data in file!!!', options=['yes', 'no'], index=1, horizontal=True)
        if addline == 'yes':
            flag_update = check_existed_talent(uploaded_file_df, action_type=option)
            if not flag_update:
                st.sidebar.button("Update", on_click=update_db(uploaded_file_df))
        elif addline == 'no':
            st.write(" 💡 You only have one way to Update the file, which is to check the file before Updating the file!!! ")

else:
    st.write(" 💡 Please upload file and make sure your upload file that follow the format!!! ")











    



    
