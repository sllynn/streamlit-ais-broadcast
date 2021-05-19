import yaml
import pandas as pd
import streamlit as st
import pydeck as pdk
import altair as alt
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, countDistinct, max as smax, log as slog


class Dataset:
    def __init__(self, config):
        self.spark = SparkSession.builder.appName('streamlit-ais-broadcasts').getOrCreate()
        self.source_table_name = config["data"]["source_table"]
        self.source_table = self.spark.table(self.source_table_name)

        if not self.spark.catalog.isCached(self.source_table_name):
            self.source_table.cache()

        self.datetime_lbound = None
        self.datetime_ubound = None
        self.datetime_filter_lbound = None
        self.datetime_filter_ubound = None

    @property
    def time_series_data(self):
        @st.cache
        def ts_counts(dummy=True):
            return (
                self.source_table
                .groupBy("date")
                .agg(
                    count("date").alias("broadcasts"),
                    countDistinct("mmsi").alias("vessels")
                )
                .orderBy("date")
            ).toPandas()
        pdf = ts_counts()
        self.datetime_lbound = pdf["date"].min()
        self.datetime_ubound = pdf["date"].max()
        return pdf

    @property
    def vessel_location_data(self):
        @st.cache
        def vessel_counts(dttm_lb: datetime.date, dttm_ub: datetime.date):
            ws = Window().partitionBy().orderBy()
            return (
                self.source_table
                    .where(col("date").between(dttm_lb.isoformat(), dttm_ub.isoformat()))
                    .groupBy("h3")
                    .agg(countDistinct("mmsi").alias("vessels"))
                    .withColumn("vessel_count_max", smax(col("vessels")).over(ws))
                    .withColumn("cell_colour", 255 * slog("vessels") / slog("vessel_count_max"))
            ).toPandas()
        return vessel_counts(self.datetime_filter_lbound, self.datetime_filter_ubound)


class Dashboard:

    def __init__(self, config):
        st.set_page_config(page_title="AIS broadcasts", layout="wide")
        st.title("US coastal cargo traffic (2018)")
        st.header(
            "Automatic Identification System (AIS) data collected for all cargo vessels sailing around the US coastline")
        info_url = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2018/index.html"
        st.markdown(f"See the NOAA Office for Coastal Management [page]({info_url}) for more detail.")
        self.cols = st.beta_columns([2, 3])
        dataset = Dataset(config)
        time_series_data = dataset.time_series_data
        dataset.datetime_filter_lbound, dataset.datetime_filter_ubound = \
            self.add_slider(dataset.datetime_lbound, dataset.datetime_ubound)
        filtered_time_series_data = time_series_data[
            (time_series_data["date"] >= dataset.datetime_filter_lbound) &
            (time_series_data["date"] <= dataset.datetime_filter_ubound)
        ]
        self.plot_ts_charts(filtered_time_series_data)
        with st.spinner("Querying vessel locations..."):
            map_data = dataset.vessel_location_data
        with st.spinner("Rendering map..."):
            self.plot_map_view(map_data)
        st.success("Complete.")

    def add_slider(self, ts_lb, ts_ub):
        return (
            self.cols[0].slider(
                label="Select a time interval for analysis:",
                value=(ts_lb, ts_ub),
                format="YYYY-MM-DD"
            )
        )

    def plot_ts_charts(self, pdf: pd.DataFrame):
        ts_chart_base = (
            alt.Chart(pdf)
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

        self.cols[0].altair_chart(
            altair_chart=alt.layer(ts_chart_vessels, ts_chart_broadcasts).resolve_scale(y="independent"),
            use_container_width=True
        )

    def plot_map_view(self, pdf: pd.DataFrame):
        # Define a layer to display on a map
        layer = pdk.Layer(
            "H3HexagonLayer", pdf,
            pickable=True, stroked=False, filled=True, extruded=False,
            get_hexagon="h3", get_fill_color="[cell_colour, 0, 0]", get_line_color=[255, 255, 255],
            line_width_min_pixels=0
        )

        # Set the viewport location
        view_state = pdk.ViewState(latitude=35, longitude=-100, zoom=3)

        # Render
        self.cols[1].pydeck_chart(pdk.Deck(
            map_provider="mapbox", map_style="satellite", api_keys=config["apikeys"],
            layers=[layer], initial_view_state=view_state,
            tooltip={"text": "Count: {vessels}"}
        ))


if __name__ == "__main__":
    with open("config.yaml", "r") as fh:
        config = yaml.load(fh, Loader=yaml.CLoader)

    dashboard = Dashboard(config)
