import logging
import os
import traceback
from typing import Union, Dict

import pandas as pd
import requests
import streamlit as st
import pydeck as pdk
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s')
logger = logging.getLogger("chicago_crime")
logger.setLevel(logging.INFO)

FLASK_BASE_URL = os.environ['FLASK_APP_BASE_URL']


class StreamlitDashboardManager:
    @classmethod
    def get_primary_types(cls):
        url = f'{FLASK_BASE_URL}/api/crimes/primary_types'
        response = requests.get(url).json()
        if response['code'] != 200:
            raise Exception(response['message'])
        return response['data']

    @classmethod
    def get_crimes_of_primary_type(cls, primary_type: str):
        url = f'{FLASK_BASE_URL}/api/crimes/'
        request_body = {'primary_type': primary_type}
        response = requests.post(url, json=request_body).json()
        if response['code'] != 200:
            raise Exception(response['message'])
        return response['data']

    @classmethod
    def load_crimes_of_type_into_df(cls, crimes_data: Dict[str, Union[str, float]]):
        try:
            crimes_df = pd.DataFrame(crimes_data)
            crimes_df['date'] = pd.to_datetime(crimes_df['date'], format='%Y-%m-%d')
            return crimes_df
        except Exception as ex:
            raise ex

    @classmethod
    def create_crimes_map(cls, crimes_df: pd.DataFrame):
        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=41.8781,
                longitude=-87.6298,
                zoom=11,
                pitch=50,
            ),
            layers=[
                pdk.Layer(
                    'HexagonLayer',
                    data=crimes_df,
                    get_position='[lon, lat]',
                    radius=100,
                    elevation_scale=4,
                    elevation_range=[0, 400],
                    pickable=True,
                    extruded=True,
                ),
                pdk.Layer(
                    'ScatterplotLayer',
                    data=crimes_df,
                    get_position='[lon, lat]',
                    get_color='[200, 30, 0, 160]',
                    get_radius=100,
                ),
            ],
        ))

    @classmethod
    def filter_crimes_df_based_on_date(cls, crimes_df, user_date_input):
        if len(user_date_input) == 2 and user_date_input[0] <= user_date_input[1]:
            user_date_input = tuple(map(pd.to_datetime, user_date_input))
            start_date, end_date = user_date_input
            crimes_df = crimes_df.loc[crimes_df['date'].between(start_date, end_date)]
        return crimes_df

    @classmethod
    def start_dashboard(cls):
        st.title('Chicago Crimes Map')
        try:
            primary_types = cls.get_primary_types()
            selected_primary_type = st.selectbox('Crime Type', primary_types, index=1)
            crimes_of_primary_type = cls.get_crimes_of_primary_type(selected_primary_type)
            crimes_df = cls.load_crimes_of_type_into_df(crimes_of_primary_type)
            user_date_input = st.date_input(
                "Crimes Date",
                value=(crimes_df['date'].min(), crimes_df['date'].max()),
                min_value=crimes_df['date'].min(),
                max_value=crimes_df['date'].max()
            )
            crimes_df = cls.filter_crimes_df_based_on_date(crimes_df, user_date_input)
            st.write(f'Found {len(crimes_df)} crimes of type "{selected_primary_type}" between selected dates')
            cls.create_crimes_map(crimes_df)
        except Exception as ex:
            # We must track this exceptions by sending them to a alerting channel
            logger.error(f'{ex}\nTrace: {traceback.format_exc()}')
            return st.write('Oops! there is an error, we are working on it...')


if __name__ == '__main__':
    StreamlitDashboardManager.start_dashboard()
