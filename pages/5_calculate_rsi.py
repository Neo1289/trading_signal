import streamlit as st
import os
from pathlib import Path
from os import walk
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Build the full path to data.csv
data_path = Path(__file__).parent.parent
data_path_str = os.path.join(str(data_path),"technical_indicators","testing_area")

# File to match
current_file = Path(__file__)
current_name = current_file.name[2:]
import_current_name = current_name[:-3]

# Find matching directory and data.csv
matching_data_file = None
for dirpath, dirnames, filenames in walk(data_path_str):
    for dirname in dirnames:
        if dirname == import_current_name:
            matching_data_file = os.path.join(dirpath, dirname, "data.csv")
            break
    if matching_data_file:
        break

@st.cache_data(ttl=60)
def load_data(matching_data_file):
    df = pd.read_csv(matching_data_file)
    return df


def streamlit_page():
    st.title("Bitcoin RSI")

    if st.button("🔄"):
        st.session_state.clear()
        st.rerun()

    df = load_data(matching_data_file)

    if 'period_slider' not in st.session_state:
        st.session_state['period_slider'] = len(df)

    period = st.slider("Select Period (days)", min_value=10, max_value=len(df), value=len(df), step=10, key="period_slider")
    df_filtered = df.tail(period)

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_filtered['timestamp'],
        y=df_filtered['rsi_14'],
        mode='lines',
        name='Signal Line',
        line=dict(color='orange')
    ))


    st.plotly_chart(fig)

    st.write("most recent data")

    st.dataframe(df_filtered)

if __name__ == "__main__":
    streamlit_page()