Classes
=======

Depending on which data the user wants to work with, users can use different classes within the package.
Currently the package contains the following classes:

1. :ref:`WTKLedClient1224 <WTKLedClient1224>`

2. :ref:`WTKLedClientHourly <WTKLedClientHourly>`


.. _WTKLedClient1224:

WTKLedClient1224
----------------
This class provides functionality for working with hourly average time-series data from WTK-LED Climate called as WTK-LED 1224. The dataset is organized such that, 
for a specific location and year, it contains 288 rows. Each row represents the hourly average values for a particular hour of the day across 
each month (12 months * 24 hours = 288 rows per file).

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.download_1224_data

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.fetch_timeseries_1224

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.fetch_windspeed_map_1224

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.fetch_winddirection_map_1224

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.fetch_filtered_data_1224

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.compute_average_windspeed_1224

.. autofunction:: windwatts_data.wtk_client_1224.WTKLedClient1224.compute_statistic_1224

.. _WTKLedClientHourly:

WTKLedClientFullHourly
----------------------
This class provides functionality for working with full hourly time-series data of WTK-LED Climate. The dataset contains 8,760 rows (365 days * 24 hours), 
with each row representing values for a specific year, month, day, and hour. This structure enables high-resolution temporal analysis on an hourly scale throughout the year.

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.download_hourly_data

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.fetch_timeseries

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.fetch_windspeed_map

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.fetch_winddirection_map

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.fetch_filtered_data

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.compute_average_windspeed

.. autofunction:: windwatts_data.wtk_client_hourly.WTKLedClientHourly.compute_statistic
