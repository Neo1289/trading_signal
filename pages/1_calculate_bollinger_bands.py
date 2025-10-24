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
    st.title("Calculate bollinger bands")

    if st.button("🔄"):
        st.session_state.clear()
        st.rerun()

    df = load_data(matching_data_file)

    if 'period_slider' not in st.session_state:
        st.session_state['period_slider'] = 100


    period = st.slider("Select Period (days)", min_value=10, max_value=len(df), value=st.session_state['period_slider'], step=10, key='period_slider')
    df_filtered = df.tail(period)

    # Create subplots
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('Bollinger Bands', 'BB Width', '%B'),
        row_heights=[0.5, 0.25, 0.25]
    )

    # Try using index if date is not a column
    if 'date' not in df.columns:
        x_data = df_filtered.index
    else:
        x_data = df_filtered['date']

    # Bollinger Bands
    fig.add_trace(go.Scatter(x=x_data, y=df_filtered['price'], name='Price', line=dict(color='blue')), row=1, col=1)
    fig.add_trace(
        go.Scatter(x=x_data, y=df_filtered['upper_band'], name='Upper Band', line=dict(color='red', dash='dash')),
        row=1, col=1)
    fig.add_trace(
        go.Scatter(x=x_data, y=df_filtered['middle_band'], name='Middle Band', line=dict(color='gray', dash='dash')),
        row=1, col=1)
    fig.add_trace(
        go.Scatter(x=x_data, y=df_filtered['lower_band'], name='Lower Band', line=dict(color='green', dash='dash')),
        row=1, col=1)

    # BB Width
    if 'bb_width' in df.columns:
        fig.add_trace(go.Scatter(x=x_data, y=df_filtered['bb_width'], name='BB Width', line=dict(color='purple')),
                      row=2, col=1)

    # %B
    if 'percent_b' in df.columns:
        fig.add_trace(go.Scatter(x=x_data, y=df_filtered['percent_b'], name='%B', line=dict(color='orange')), row=3,
                      col=1)
        # Add reference lines at 0, 0.5, and 1
        fig.add_hline(y=0, line_dash="dot", line_color="gray", row=3, col=1)
        fig.add_hline(y=0.5, line_dash="dot", line_color="gray", row=3, col=1)
        fig.add_hline(y=1, line_dash="dot", line_color="gray", row=3, col=1)

    fig.update_layout(height=800, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)


    st.write("Most recent data")
    st.dataframe(df_filtered)

if __name__ == '__main__':
    streamlit_page()