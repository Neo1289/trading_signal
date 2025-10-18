import streamlit as st
def main_page():

    st.set_page_config(page_title="Home", page_icon="🏠")
    st.title("🏠 Home Page")
    st.write("""
    Welcome to the Technical Indicators Testing Area
    
    This application provides a comprehensive testing environment for various technical analysis indicators.
    You can explore different financial indicators, visualize market data, and test trading strategies
    in an interactive web interface powered by Streamlit.
    
    Features:
    - Interactive charts and visualizations
    - Real-time technical indicator calculations
    - Backtesting capabilities for trading strategies
    - Data analysis tools for market research
    
    Use the sidebar navigation to explore different sections and start testing your indicators.""")

    st.subheader("Available contents")

    st.page_link("pages/3_calculate_ma.py", label="📊 Calculate Moving Averages")
    st.page_link("pages/1_calculate_bollinger_bands.py", label="📊 Boliinger Bands")
    st.write("...")

if __name__ == "__main__":
    main_page()