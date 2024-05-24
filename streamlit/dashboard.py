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

    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
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

    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
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

def query_avg_minutes_per_trip() -> pd.DataFrame:
    """
    Average number of trips recorded each day.
    """

    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql("""
            select
                pickup_datetime::date as date,
                avg(extract(epoch from (dropoff_datetime - pickup_datetime)))/60 as avg_trip_duration_min
            from yellow_tripdata
            group by pickup_datetime::date
            order by pickup_datetime::date
        """)

        df = con.sql("select * from data where date >= '2023-11-01'").df()
    return df

def plot_avg_minutes_per_trip(df: pd.DataFrame) -> go.Figure:
    fig = px.line(df, x='date', y='avg_trip_duration_min', title='Average trip duration (minutes)')
    return fig

def query_percent_credit_card() -> pd.DataFrame:
    """
    TODO
    """
    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql("""
            select
                pickup_datetime::date as date,
                sum(case when payment_type = 1 then 1 else 0 end)::float/count(*) as pct_credit_card
            from yellow_tripdata
            group by pickup_datetime::date
            order by pickup_datetime::date
        """)

        df = con.sql("select * from data where date >= '2023-11-01'").df()
    return df

def plot_percent_credit_card(df: pd.DataFrame) -> go.Figure:
    fig = px.line(df, x='date', y='pct_credit_card', title='Percent of trips paid with credit card')
    return fig

def query_pct_trips_shared() -> pd.DataFrame:
    """
    TODO
    """
    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql("""
            select
                pickup_datetime::date as date,
                sum(case when passenger_count > 1 then 1 else 0 end)::float/count(*) as pct_shared_trip
            from yellow_tripdata
            group by pickup_datetime::date
            order by pickup_datetime::date
        """)

        df = con.sql("select * from data where date >= '2023-11-01'").df()
    return df

def plot_pct_trips_shared(df: pd.DataFrame) -> go.Figure:
    fig = px.line(df, x='date', y='pct_shared_trip', title='Percent of trips shared by 2 or more passengers')
    return fig


def query_count_zone(pickup=True) -> pd.DataFrame:
    if pickup:
        which = 'pickup'
    else:
        which = 'dropoff'

    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql(f"""
            select
                {which}_location_id,
                count(*) as zone_count
            from yellow_tripdata
            group by {which}_location_id
        """)

        df = con.sql(f"""
        select
            borough,
            zone,
            zone_count as trip_count
        from data
        join taxi_zones
        on data.{which}_location_id = taxi_zones.location_id
        order by zone_count desc
        """).df()

    return df

def plot_count_pickup_zone(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(df, x='zone', y='zone_count', title='Total pickups per zone')
    return fig

def query_busiest_hour_of_day() -> pd.DataFrame:
    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql("""
        select
            extract(hour from pickup_datetime) as hour,
            count(*) as trips_per_hour
        from yellow_tripdata
        group by 1
        order by 1 asc
        """)

        df = con.sql("""
        select * from data
        """).df()

    return df

def plot_busiest_hour_of_day(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(df, x='hour', y='trips_per_hour', title='Total pickups per hour')
    return fig

def query_payment_type() -> pd.DataFrame:
    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql("""
        select
            case
                when payment_type = 1 then 'Credit card'
                when payment_type = 2 then 'Cash'
                else 'Other'
            end as payment_type,
            count(*) as payment_count
        from yellow_tripdata
        group by 1
        """).df()

        return data

def plot_payment_type(df: pd.DataFrame) -> go.Figure:
    fig = px.pie(df, values='payment_count', names='payment_type')
    return fig

def query_passengers_per_trip() -> pd.DataFrame:
    with duckdb.connect('/home/felipe/repos/nyc-taxi/data/staging/nyc_taxi.duckdb', read_only=True) as con:
        data = con.sql("""
        select
            passenger_count,
            count(*) as num_trips
        from yellow_tripdata
        where passenger_count > 0
        group by 1
        """).df()

        return data

def plot_passengers_per_trip(df: pd.DataFrame) -> go.Figure:
    fig = px.bar(df, x='passenger_count', y='num_trips')
    return fig


fig = plot_trips_per_day(query_trips_per_day())
st.plotly_chart(fig)

fig = plot_farebox_per_day(query_farebox_per_day())
st.plotly_chart(fig)

fig = plot_avg_minutes_per_trip(query_avg_minutes_per_trip())
st.plotly_chart(fig)

fig = plot_percent_credit_card(query_percent_credit_card())
st.plotly_chart(fig)

fig = plot_pct_trips_shared(query_pct_trips_shared())
st.plotly_chart(fig)

df = query_count_zone(pickup=True)
df
df = query_count_zone(pickup=False)
df

fig = plot_busiest_hour_of_day(query_busiest_hour_of_day())
st.plotly_chart(fig)

fig = plot_payment_type(query_payment_type())
st.plotly_chart(fig)

fig = plot_passengers_per_trip(query_passengers_per_trip())
st.plotly_chart(fig)