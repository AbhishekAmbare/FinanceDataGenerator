import streamlit as st
from data_model import generateFinanceData_by_Path

st.title('Finance Data Controller')

st.write('Below are parameters to enter.')

parquet_path = st.text_input('Enter Parquet file path with file name')

if parquet_path:
    records = generateFinanceData_by_Path(parquet_path)
    st.write('Created file with records ' + str(records))