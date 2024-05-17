import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go


st.set_page_config(
    page_title="NYC Taxi Trips",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)


def query_trips_per_day() -> pd.DataFrame:
    """
    Average number of trips recorded each day.
    """

    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb') as con:
        data = con.sql(
            """
            with staging as (
                select
                    extract(year from pickup_datetime) as year,
                    extract(month from pickup_datetime) as month,
                    extract(day from pickup_datetime) as day,
                    *
                from yellow_tripdata
            )
            select year, month, day, count(*) as num_trips
            from staging
            group by year, month, day
            """
        )
        stg = con.sql('select make_date(year, month, day) as date, num_trips from data order by date')
        df = con.sql("select * from stg where date >= '2023-11-01'").df()
    return df

def plot_trips_per_day(df: pd.DataFrame) -> go.Figure:
    fig = px.line(df, x='date', y='num_trips', title='Total trips per day')
    return fig

def query_farebox_per_day() -> pd.DataFrame:
    """
    Average number of trips recorded each day.
    """

    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb') as con:
        data = con.sql(
            """
            with staging as (
                select
                    extract(year from pickup_datetime) as year,
                    extract(month from pickup_datetime) as month,
                    extract(day from pickup_datetime) as day,
                    *
                from yellow_tripdata
            )
            select year, month, day, avg(total_amount) as farebox
            from staging
            group by year, month, day
            """
        )
        stg = con.sql('select make_date(year, month, day) as date, farebox from data order by date')
        df = con.sql("select * from stg where date >= '2023-11-01'").df()
    return df

def plot_farebox_per_day(df: pd.DataFrame) -> go.Figure:
    fig = px.line(df, x='date', y='farebox', title='Average daily farebox per day')
    return fig


fig = plot_trips_per_day(query_trips_per_day())
st.plotly_chart(fig)

fig = plot_farebox_per_day(query_farebox_per_day())
st.plotly_chart(fig)