import yaml

import pandas as pd
import streamlit as st
import pydeck as pdk
import altair as alt
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, countDistinct, max as smax, log as slog

with open("config.yaml", "r") as fh:
    config = yaml.load(fh, Loader=yaml.CLoader)

spark = SparkSession.builder.appName('streamlit-ais-broadcasts').getOrCreate()

source_table_name = config["data"]["source_table"]
source_table = spark.table(source_table_name)

if not spark.catalog.isCached(source_table_name):
    source_table.cache()


@st.cache
def ts_counts(dummy=True) -> pd.DataFrame:
    count_by_day_df = (
        source_table
        .groupBy("date")
        .agg(
            count("date").alias("broadcasts"),
            countDistinct("mmsi").alias("vessels")
        )
        .orderBy("date")
    )
    return count_by_day_df.toPandas()


@st.cache
def vessel_counts(ts_lb: datetime.date, ts_ub: datetime.date) -> pd.DataFrame:
    ws = Window().partitionBy().orderBy()

    vessel_count_sdf = (
        source_table
        .where(col("date").between(ts_lb.isoformat(), ts_ub.isoformat()))
        .groupBy("h3")
        .agg(countDistinct("mmsi").alias("vessels"))
        .withColumn("vessel_count_max", smax(col("vessels")).over(ws))
        .withColumn("cell_colour", 255 * slog("vessels") / slog("vessel_count_max"))
    )
    return vessel_count_sdf.toPandas()


st.set_page_config(page_title="AIS broadcasts", layout="wide")
st.title("US coastal cargo traffic (2018)")
st.header("Automatic Identification System (AIS) data collected for all cargo vessels sailing around the US coastline")
st.markdown("See the NOAA Office for Coastal Management [page](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/index.html) for more detail.")
cols = st.beta_columns([2, 3])

ts_counts_pdf = ts_counts()

broadcast_tsfilter_start, broadcast_tsfilter_end = (
    cols[0].slider(
        label="Select a time interval for analysis:",
        value=(ts_counts_pdf["date"].min(), ts_counts_pdf["date"].max()),
        format="YYYY-MM-DD")
)

vessel_count_pdf = vessel_counts(broadcast_tsfilter_start, broadcast_tsfilter_end)

# Define a layer to display on a map
layer = pdk.Layer(
    "H3HexagonLayer", vessel_count_pdf,
    pickable=True, stroked=False, filled=True, extruded=False,
    get_hexagon="h3", get_fill_color="[cell_colour, 0, 0]", get_line_color=[255, 255, 255],
    line_width_min_pixels=0
)

# Set the viewport location
view_state = pdk.ViewState(latitude=35, longitude=-100, zoom=3)

# Render
cols[1].pydeck_chart(pdk.Deck(
    map_provider="mapbox", map_style="satellite", api_keys=config["apikeys"],
    layers=[layer], initial_view_state=view_state,
    tooltip={"text": "Count: {vessels}"}
))

ts_counts_filtered_pdf = ts_counts_pdf[
    (ts_counts_pdf["date"] >= broadcast_tsfilter_start) &
    (ts_counts_pdf["date"] <= broadcast_tsfilter_end)
]

ts_chart_base = (
    alt.Chart(ts_counts_filtered_pdf)
    .encode(alt.X('date:T', axis=alt.Axis(title=None)))
)
ts_chart_vessels = (
    ts_chart_base
    .mark_line(stroke='#e37b12', interpolate='monotone', tooltip=True)
    .encode(y=alt.Y("vessels:Q", axis=alt.Axis(titleColor="#e37b12")))
)
ts_chart_broadcasts = (
    ts_chart_base
    .mark_line(stroke='#7932a8', interpolate='monotone', tooltip=True)
    .encode(y=alt.Y("broadcasts:Q", axis=alt.Axis(titleColor="#7932a8")))
)

cols[0].altair_chart(alt.layer(ts_chart_vessels, ts_chart_broadcasts).resolve_scale(y="independent"))
