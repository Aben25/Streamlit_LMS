import streamlit as st
import pandas as pd
import zipfile
import os
import io
from sqlalchemy import create_engine, MetaData, Table
import numpy as np
import re
from sqlalchemy.exc import NoSuchTableError
from streamlit import components

# Add iframe
iframe_html = """
<iframe width="100%" frameborder="0" src="https://artba.sisense.com/app/main/dashboards/63fd3153d50e0a003581db12?embed=true"></iframe>
"""
# Function to extract date from filename
def extract_date(filename):
    date_str = re.search("(\d{8})", filename).group(0)
    return f"{date_str[6:8]}/{date_str[4:6]}/{date_str[0:4]}"

st.title('Upload CSV or ZIP files')

uploaded_files = st.file_uploader("Choose CSV or ZIP files", type=['csv', 'zip'], accept_multiple_files=True)
components.v1.html(iframe_html, height=600)

if uploaded_files:
    # Connect to the database
    engine = create_engine(
        "mysql+pymysql://admin:mixpanelADOBEANALYTICS25@artba-instance-1.cpgyvvfgvirs.us-east-1.rds.amazonaws.com:3306/LMS")

    # Check if a file with "group" in its name exists
    group_file_exists = any("group" in f.name.lower() for f in uploaded_files)
    if group_file_exists:
        try:
            tbl = Table('group', MetaData(), autoload_with=engine)
            tbl.drop(engine, checkfirst=True)
        except NoSuchTableError:
            pass

    progress = st.progress(0)
    for i, uploaded_file in enumerate(uploaded_files):
        if uploaded_file.type == 'application/zip':
            with zipfile.ZipFile(uploaded_file, 'r') as zip_ref:
                for filename in zip_ref.namelist():
                    with zip_ref.open(filename) as file:
                        df = pd.read_csv(file, sep=';')
                        table_name = filename.split("-")[0]
                        df["date_period"] = extract_date(filename)
                        df.reset_index(inplace=True)
                        df.rename(columns={'index': 'uid'}, inplace=True)

                        # Convert columns to appropriate types
                        for column in df.columns:
                            try:
                                if 'score' in column.lower():
                                    df[column] = df[column].replace(['-', ''], np.nan).str.replace('%', '').astype(float)
                                elif 'date' in column.lower():
                                    df[column] = pd.to_datetime(df[column], errors='coerce')
                                elif 'duration' in column.lower():
                                    df[column] = pd.to_timedelta(df[column], errors='coerce')
                                elif 'viewed' in column.lower():
                                    df[column] = df[column].replace(['-', ''], np.nan).str.replace('%', '').astype(float)
                            except Exception:
                                df[column] = np.nan

                        # Write the DataFrame to the database
                        if "group" in table_name.lower():
                            df.to_sql(table_name, engine, if_exists="append", index=False)
                        else:
                            df.to_sql(table_name, engine, if_exists="replace", index=False)

        elif uploaded_file.type == 'text/csv':
            df = pd.read_csv(uploaded_file, sep=';')
            filename = uploaded_file.name
            table_name = filename.split("-")[0]
            df["date_period"] = extract_date(filename)
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'uid'}, inplace=True)

            # Convert columns to appropriate types
            for column in df.columns:
                try:
                    if 'score' in column.lower():
                        df[column] = df[column].replace(['-', ''], np.nan).str.replace('%', '').astype(float)
                    elif 'date' in column.lower():
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif 'duration' in column.lower():
                        df[column] = pd.to_timedelta(df[column], errors='coerce')
                except Exception:
                    df[column] = np.nan

            # Write the DataFrame to the database
            if "group" in table_name.lower():
                df.to_sql(table_name, engine, if_exists="append", index=False)
            else:
                df.to_sql(table_name, engine, if_exists="replace", index=False)

        progress.progress((i+1)/len(uploaded_files))

    st.write("File processing completed.")
