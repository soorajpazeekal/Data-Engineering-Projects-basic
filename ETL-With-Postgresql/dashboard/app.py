import sql as sql
import streamlit as st

st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)

st.header("citibike Trip data dashboard")

with st.sidebar:
    option = st.selectbox(
    'Choose a rideable_type',
    sql.get_all_bike_types())

col1, col2, col3 = st.columns(3)
with st.container():
    col1.metric(label="Count Till date", value=sql.count_bikes_with_names(option), delta=None)
    value = sql.count_member_types(option)
    col2.metric(label="Total Member Count VS Casual(User who have valid membership)", 
                value=value[0], 
                delta=value[1])
    value=sql.fav_start_station(option)
    col3.metric(label="Most Popular Starting Locations", value=value[0], delta=value[1], delta_color="inverse")
    st.header('Most Popular Starting Locations (by number of bike types)')
    st.map(sql.plot_map_starting(option))
    st.header('Most Popular Ending Locations (by number of bike types)')
    st.map(sql.plot_map_ending(option))
    

