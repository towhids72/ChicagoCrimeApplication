import datetime
import logging
import os
from typing import Union, Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st
import pydeck as pdk
from dotenv import load_dotenv

from utilities.log_utils import LogUtils

load_dotenv()

# get the flask base url from environment variables
FLASK_BASE_URL = os.environ['FLASK_APP_BASE_URL']

logger = LogUtils.get_logger(logger_name='streamlit_dashboard', level=logging.ERROR)


class StreamlitDashboardManager:
    """A class to manage Streamlit dashboard"""

    @classmethod
    def get_primary_types(cls) -> List[str]:
        """This method calls internal flask API to get primary types,
        it checks the status code, then decides to return data or raise exception.

        Returns:
            A list of crimes primary types.
        """

        url = f'{FLASK_BASE_URL}/api/crimes/primary_types'
        response = requests.get(url).json()
        # check if API returns correct data, otherwise raise an exception
        if response['code'] != 200:
            raise Exception(response['message'])
        # sort primary type alphabetically
        primary_types = sorted(response['data'], key=lambda x: x)
        return primary_types

    @classmethod
    def get_crimes_of_primary_type(cls, primary_type: str) -> List[Dict[str, Union[float, str]]]:
        """This method calls internal flask API to get crimes data,
        it checks the status code, then decides to return data or raise exception.

        Args:
            primary_type (str): A string to get crimes data

        Returns:
            A list of crimes that contains crime location and date in a dict.
        """

        url = f'{FLASK_BASE_URL}/api/crimes/?primary_type={primary_type}'
        response = requests.get(url).json()
        # if API doesn't return crimes data, we must raise an exception
        if response['code'] != 200:
            raise Exception(response['message'])
        return response['data']

    @classmethod
    def load_crimes_of_type_into_df(cls, crimes_data: List[Dict[str, Union[str, float]]]) -> pd.DataFrame:
        """Convert crimes data to a :class:`DataFrame`

        Args:
            crimes_data (list): A list of crimes that contains crime location and date in a dict.

        Returns:
            A pandas DataFrame of crimes data
        """
        try:
            # load crimes data into DataFrame
            crimes_df = pd.DataFrame(crimes_data)
            # convert DataFrame "date" column to datetime type, so we can filter DataFrame based on dates
            crimes_df['date'] = pd.to_datetime(crimes_df['date'], format='%Y-%m-%d')
            return crimes_df
        except Exception as ex:
            raise ex

    @classmethod
    def create_crimes_map(cls, crimes_df: pd.DataFrame):
        """Create a pydeck_chart of crimes DataFrame,
        initialize latitude and longitude is Chicago city center.

        Args:
            crimes_df: A pandas DataFrame of crimes data
        """
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
                    get_position='[lon, lat]',  # the latitude and longitude column names in DataFrame
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
    def filter_crimes_df_based_on_date(
            cls, crimes_df: pd.DataFrame, user_date_input: Tuple[datetime.datetime]
    ) -> pd.DataFrame:
        """Filter given :class:`DataFrame` between two given dates

        Args:
            crimes_df: A pandas DataFrame
            user_date_input: A tuple of two selected date

        Returns:
            A pandas DataFrame
        """

        # todo check dates types
        # check if user selected two dates, otherwise we shouldn't filter dataframe until user selects second date
        if len(user_date_input) == 2:
            user_date_input = tuple(map(pd.to_datetime, user_date_input))
            start_date, end_date = user_date_input
            # filter dataframe between selected dates
            crimes_df = crimes_df.loc[crimes_df['date'].between(start_date, end_date)]
        return crimes_df

    @classmethod
    def start_dashboard(cls):
        """This method loads Streamlit dashboard"""

        # set a title for our dhashboard
        st.title('Chicago Crimes Map')
        try:
            # get crimes primary types to create a dropdown menu of them
            primary_types = cls.get_primary_types()
            # create a dropdown menu with fetched primary types, first item in the list will be default
            selected_primary_type = st.selectbox('Crime Type', primary_types)
            # get crimes data based on the selected primary type
            crimes_of_primary_type = cls.get_crimes_of_primary_type(selected_primary_type)
            # convert crimes data to a DataFrame, so we can create a map of it
            crimes_df = cls.load_crimes_of_type_into_df(crimes_of_primary_type)
            # create a date input and let user change dates
            # default values will be the minimum and maximum of fetched crimes dates
            user_date_input = st.date_input(
                "Crimes Date",
                value=(crimes_df['date'].min(), crimes_df['date'].max()),
                min_value=crimes_df['date'].min(),
                max_value=crimes_df['date'].max()
            )
            # filter crimes Dataframe based on user selected dates
            crimes_df = cls.filter_crimes_df_based_on_date(crimes_df, user_date_input)
            # let user know how many crimes are shown on the map
            st.write(f'Found {len(crimes_df)} crimes of type "{selected_primary_type}" between selected dates')
            # everything is fine, show the map!
            cls.create_crimes_map(crimes_df)
        except Exception:
            # it looks like we got in trouble, check the logs
            # let user know that error is happened
            # We must track this exceptions by sending them to an alerting channel
            logger.exception('Error occurred while getting crimes data and creating map')
            return st.write('Oops! there is an error, we are working on it...')


if __name__ == '__main__':
    StreamlitDashboardManager.start_dashboard()
