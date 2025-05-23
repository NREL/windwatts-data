import os
import pandas as pd
from .client_base import client_base

class WTKLedClient1224(client_base):
    """
    This class is called WTKLedClient1224 because the source data is called "Wind ToolKit" 
    and this data is hourly averages of windspeed, winddirection etc at different heights across each month.
    """
    def __init__(self, config_path: str = None):
        # Load configuration
        super().__init__(config_path, data='wtk')
    
    def download_1224_data(self,
        years: list[int]= None,
        lat: float= None,
        long: float= None,
        n_nearest: int = 1,
        varset: str = 'all',
        local_dir: str = 'downloads'
        ) -> list[str]:
        """
        Download CSV.GZ files containing timeseries data at specific location(s) for specific year(s).

        :param years: List of years (e.g., [2001, 2002]) for which data should be downloaded.(Required)
        :type years: list[int] or None
        :param lat: Latitude of the target location.(Required)
        :type lat: float or None
        :param long: Longitude of the target location.(Required)
        :type long: float or None
        :param n_nearest: Number of nearest locations to include in the download. Must be between 1 and 16. Default is 1.(Optional)
        :type n_nearest: int
        :param varset: Variable set to filter the data. Default is 'all'.(Takes only default value for now)
        :type varset: str
        :param local_dir: Local directory to save the downloaded files. Default is 'downloads'.(Optional)
        :type local_dir: str
        :raises TypeError: If `lat` or `long` or `years` is None.
        :raises ValueError: If `years` is not a list of integers, if `lat` or `long` are not valid numbers, 
                            or if `n_nearest` is not between 1 and 16.
        :raises RuntimeError: If the local directory cannot be created or if the nearest location(s) cannot be determined.
        :return: A list of file paths for the successfully downloaded files.
        :rtype: list[str]
        """
        if lat is None or long is None:
            raise ValueError("Parameters 'lat' and 'long' must be provided.")
        
        if years is None:
            raise ValueError("At least 1 value for the Parameter 'years' must be specified.")
        
        if not isinstance(years, list) or not all(isinstance(y, int) for y in years):
            raise TypeError("Parameter 'years' must be a list of integers.")
    
        if not isinstance(lat, (int, float)) or not isinstance(long, (int, float)):
            raise TypeError("Parameters 'lat' and 'long' must be valid numeric values.")
        
        if (lat is None and long is not None) or (long is None and lat is not None):
            raise ValueError("Both 'lat' and 'long' must be provided together for location filtering.")
        
        if not (1 <= n_nearest <= 16):
            raise ValueError("Parameter 'n_nearest' must be between 1 and 16.")
        
        try:
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
        except OSError as e:
            raise RuntimeError(f"Failed to create local directory '{local_dir}': {e}")

        # Step 1: Find the nearest index
        try:
            if n_nearest == 1:
                indexes = [self.find_nearest_location(lat, long)]
            else:
                indexes = self.find_n_nearest_locations(lat, long, n_nearest)
        except Exception as e:
            raise RuntimeError(f"Failed to determine nearest locations: {e}")

        # Step 2: Construct and download files
        downloaded_files = []
        for index in indexes:
            for year in years:
                s3_key = f"1224/year={year}/varset={varset}/index={index}/{index}_{year}_{varset}.csv.gz"
                local_file_path = os.path.join(local_dir, f"{index}_{year}_{varset}.csv.gz")
                try:
                    print(f"Downloading {s3_key} from S3...")
                    self.s3.download_file(self.bucket_name, s3_key, local_file_path, ExtraArgs={'RequestPayer': 'requester'})
                    downloaded_files.append(local_file_path)
                    print(f"Downloaded: {local_file_path}")
                except Exception as e:
                    print(f"Failed to download {s3_key}: {str(e)}")

        return downloaded_files
    
    def fetch_filtered_data_1224(self,
        columns: list[str] = None,
        years: list[int] = None,
        months: list[int] = None,
        hours: list[int] = None,
        lat: float = None,
        long: float = None,
        heights: list[float] = None,
        n_nearest: int = 1,
        varset: str = "all"
        ) -> pd.DataFrame:
        """
        Generalized function to fetch filtered data (timeseries and map), given filters based on location(s), time and height(s).

        :param columns: List of column names to include in the query. If None, all columns will be fetched.
        :type columns: list[str] or None
        :param years: List of years to filter the data (e.g., [2020, 2021]). If None, all years will be considered.
        :type years: list[int] or None
        :param months: List of months to filter the data (e.g., [1, 2, 12] for January, February, and December). Range(1-12). If None, all months will be considered.
        :type months: list[int] or None
        :param hours: List of hours to filter the data (e.g., [1, 6, 12, 18] for specific hours). Range(1-24). If None, all hours will be considered.
        :type hours: list[int] or None
        :param lat: Latitude for location filtering. Must be provided with `long` for location-based filtering. If None, all locations will be considered.(Expensive Operation)
        :type lat: float or None
        :param long: Longitude for location filtering. Must be provided with `lat` for location-based filtering. If None, all locations will be considered.(Expensive Operation)
        :type long: float or None
        :param heights: List of heights to filter data columns, such as windspeed or wind direction at specific heights.
        :type heights: list[float] or None
        :param n_nearest: Number of nearest locations to include in the query. Must be between 1 and 16. Default is 1.
        :type n_nearest: int
        :param varset: Variable set to filter data. Default is "all".
        :type varset: str
        :return: A pandas DataFrame containing the filtered data(map or timeseries) based on the specified parameters.
        :rtype: pandas.DataFrame
        """
        # 1. Validate Latitude and Longitude
        if lat is not None and not isinstance(lat, (int, float)):
            raise TypeError("Parameter 'lat' must be a numeric value (int or float).")
        if long is not None and not isinstance(long, (int, float)):
            raise TypeError("Parameter 'long' must be a numeric value (int or float).")
        if (lat is None and long is not None) or (long is None and lat is not None):
            raise ValueError("Both 'lat' and 'long' must be provided together for location filtering.")
        

        # 2. Validate `n_nearest`
        if not isinstance(n_nearest, int) or not (1 <= n_nearest <= 16):
            raise ValueError("Parameter 'n_nearest' must be an integer between 1 and 16.")

        # 3. Validate `columns`
        if columns is not None:
            if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
                raise ValueError("Parameter 'columns' must be a list of strings representing column names.")

        # 4. Validate `years`
        if years is not None:
            if not isinstance(years, list) or not all(isinstance(year, int) for year in years):
                raise ValueError("Parameter 'years' must be a list of integers representing years.")

        # 5. Validate `months`
        if months is not None:
            if not isinstance(months, list) or not all(isinstance(month, int) and 1 <= month <= 12 for month in months):
                raise ValueError("Parameter 'months' must be a list of integers (1-12).")

        # 6. Validate `hours`
        if hours is not None:
            if not isinstance(hours, list) or not all(isinstance(hour, int) and 1 <= hour <= 24 for hour in hours):
                raise ValueError("Parameter 'hours' must be a list of integers (1-24).")

        # 7. Validate `heights`
        if heights is not None:
            if not isinstance(heights, list) or not all(isinstance(height, (int, float)) for height in heights):
                raise ValueError("Parameter 'heights' must be a list of numeric values (int or float).")
            if columns is not None:
                raise ValueError("Please specify either 'columns' or 'heights', not both.")

        
        self._reset_index_(lat,long)
        
        # Ensure all requested columns exist
        if columns is not None:
            invalid_columns = [col for col in columns if col not in self.column_names]
            if invalid_columns:
                raise ValueError(f"The following columns are invalid: {', '.join(invalid_columns)}")
        else:
            columns = self.column_names.copy()
            if 'index' in columns:
                columns.remove('index')
        
        
        # If heights are specified, find the nearest lower and higher columns for each height
        if heights:
            try:
                columns = self.find_relevant_columns(heights)
                columns.extend(['mohr', 'year'])
            except Exception as e:
                raise RuntimeError("Failed to find relevant columns for specified heights.") from e

        # Construct the SELECT clause
        columns_str = ', '.join(columns)
        
        query = f"SELECT {columns_str}"
        
        if lat is None and long is None:
            query += f", regexp_extract(\"$path\", '.*/index=([^/]+)/.*', 1) AS index"
        else:
            query += f", index"
        
        query += f" FROM {self.athena_table_name} WHERE 1=1"

        # Add filters dynamically
        if years:
            year_list = ', '.join([f"'{year}'" for year in years])
            query += f" AND year IN ({year_list})"
            
        if months:
            month_list = ', '.join([str(month) for month in months])
            query += f" AND CAST(mohr AS INT) / 100 IN ({month_list})"
        
        if hours:
            hour_list = ', '.join([str(hour) for hour in hours])
            query += f" AND CAST(mohr AS INT) % 100 IN ({hour_list})"
            
        if varset:
            query += f" AND varset = '{varset}'"

        if lat is not None and long is not None:
            try:
                if n_nearest == 1:
                    index = self.find_nearest_location(lat, long)
                    if not index:
                        raise ValueError("No valid nearest location found.")
                    query += f" AND index = '{index}'"
                else:
                    indexes = self.find_n_nearest_locations(lat, long, n_nearest)
                    if not indexes:
                        raise ValueError("No valid nearest locations found.")
                    index_list = ', '.join([f"'{idx}'" for idx in indexes])
                    query += f" AND index IN ({index_list})"
            except Exception as e:
                raise RuntimeError("Failed to process location-based filtering.") from e
                
            result_df = self.query_athena(query)
        else:
            result_df = self.query_athena(query,return_result_location=True)
        
        return result_df
    

    def compute_statistic_1224(self,
        columns: list[str] = None,
        statistic: str = 'AVG',
        lat: float = None,
        long: float = None,
        n_nearest: int = 1,
        heights: list[float] = None,
        years: list[int] = None,
        months: list[int] = None,
        hours: list[int] = None,
        group_by_year: bool = False,
        group_by_index: bool = False,
        group_by_month: bool = False,
        group_by_hour: bool = False,
        order_by: str = None,
        order_direction: str = 'ASC',
        varset: str = "all"
        ) -> pd.DataFrame:
        """
        Calculate a specified statistic (e.g., AVG, SUM) for selected columns or columns with specific height.
        Optionally, filter the result by n_nearest neighbours, years, months and hours.

        :param columns: List of column names to calculate statistics for. If None, all columns will be considered.
        :type columns: list[str] or None
        :param statistic: The statistical operation to perform (e.g., 'AVG', 'SUM', 'MIN', 'MAX'). Default is 'AVG'.
        :type statistic: str, optional
        :param lat: Latitude for location filtering. Must be provided with `long` for location-based filtering. If None, all locations will be considered. (Computationally Expensive Operation)
        :type lat: float or None
        :param long: Longitude for location filtering. Must be provided with `lat` for location-based filtering. If None, all locations will be considered. (Computationally Expensive Operation)
        :type long: float or None
        :param n_nearest: Number of nearest locations to include in the query. Must be between 1 and 16. Default is 1.
        :type n_nearest: int, optional
        :param heights: List of heights to dynamically filter columns, such as wind speed or wind direction.
        :type heights: list[float] or None
        :param years: List of years to filter the data (e.g., [2020, 2021]). If None, all years will be considered.
        :type years: list[int] or None
        :param months: List of months to filter the data (e.g., [1, 2, 12]). If None, all months will be considered. Range(1-12).
        :type months: list[int] or None
        :param hours: List of hours to filter the data (e.g., [1, 6, 12, 18]). If None, all hours will be considered. Range(1-24)
        :type hours: list[int] or None
        :param group_by_year: Whether to group results by year. Default is False.
        :type group_by_year: bool, optional
        :param group_by_index: Whether to group results by index. Default is False.
        :type group_by_index: bool, optional
        :param group_by_month: Whether to group results by month. Default is False.
        :type group_by_month: bool, optional
        :param group_by_hour: Whether to group results by hour. Default is False.
        :type group_by_hour: bool, optional
        :param order_by: Column name or computed field to order the results by. Optional.
        :type order_by: str or None
        :param order_direction: Direction of ordering, either 'ASC' (default) or 'DESC'.
        :type order_direction: str, optional
        :param varset: Variable set to filter data. Default is "all".
        :type varset: str
        :return: A pandas DataFrame containing the statistical results based on the specified filters and groupings.
        :rtype: pandas.DataFrame
        """
        ## 1. Validate Latitude and Longitude
        if lat is not None and not isinstance(lat, (int, float)):
            raise TypeError("Parameter 'lat' must be a numeric value (int or float).")
        if long is not None and not isinstance(long, (int, float)):
            raise TypeError("Parameter 'long' must be a numeric value (int or float).")
        if (lat is None and long is not None) or (long is None and lat is not None):
            raise ValueError("Both 'lat' and 'long' must be provided together for location filtering.")

        ## 2. Validate `n_nearest`
        if not isinstance(n_nearest, int) or not (1 <= n_nearest <= 16):
            raise ValueError("Parameter 'n_nearest' must be an integer between 1 and 16.")

        ## 3. Validate `columns`
        if columns is not None:
            if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
                raise ValueError("Parameter 'columns' must be a list of valid column names (strings).")

        ## 4. Validate `years`
        if years is not None:
            if not isinstance(years, list) or not all(isinstance(year, int) for year in years):
                raise ValueError("Parameter 'years' must be a list of integers representing years.")
        
        ## 5. Validate `months`
        if months is not None:
            if not isinstance(months, list) or not all(isinstance(month, int) and 1 <= month <= 12 for month in months):
                raise ValueError("Parameter 'months' must be a list of integers representing months.")
            
        ## 6. Validate `hours`
        if hours is not None:
            if not isinstance(hours, list) or not all(isinstance(hour, int) and 1 <= hour <= 24 for hour in hours):
                raise ValueError("Parameter 'hours' must be a list of integers representing hours.")
        
        ## 7. Validate `heights`
        if heights is not None:
            if not isinstance(heights, list) or not all(isinstance(height, (int, float)) for height in heights):
                raise ValueError("Parameter 'heights' must be a list of numeric values (int or float).")
            if columns is not None:
                raise ValueError("Specify either 'columns' or 'heights', not both.")


        self._reset_index_(lat,long)
        
        # Ensure all requested columns exist
        if columns is not None:
            for column in columns:
                if column not in self.column_names:
                    raise ValueError(f"Column '{column}' does not exist in the table.")
        else:
            columns = self.column_names.copy()
            unwanted_cols_list=['mohr','varset','year','index']
            for col in unwanted_cols_list:
                try:
                    columns.remove(col)
                except ValueError:
                    pass

        # Filter columns by heights, if specified
        if heights:
            columns = self.find_relevant_columns(heights)

        # Construct the SELECT clause for statistical computation
        select_clause = [f"{statistic}({col}) AS {col}_{statistic.lower()}" for col in columns]
        
        # Add grouping columns conditionally
        group_by_columns = []
        
        if n_nearest > 1 and group_by_index:
            select_clause.append("index")
            group_by_columns.append("index")
        
        if group_by_year:
            select_clause.append("year")
            group_by_columns.append("year")
        
        if group_by_month:
            select_clause.append("CAST(mohr AS INT) / 100 AS month")
            group_by_columns.append("CAST(mohr AS INT) / 100")
        
        if group_by_hour:
            select_clause.append("CAST(mohr AS INT) % 100 AS hour")
            group_by_columns.append("CAST(mohr AS INT) % 100")
        
        # Ensure the order_by column is in the SELECT clause
        if order_by and order_by.lower() not in [col.split(" AS ")[-1] for col in select_clause]:
            raise ValueError(f"The order_by column '{order_by}' must be included in the SELECT statement. Here are the selected columns for this query: {', '.join(select_clause)}")
        
        query = f"SELECT {', '.join(select_clause)} FROM {self.athena_table_name} WHERE 1=1"
        
        # Add filters for location
        if lat is not None and long is not None:
            try:
                if n_nearest == 1:
                    index = self.find_nearest_location(lat, long)
                    query += f" AND index IN ('{index}')"
                else:
                    indexes = self.find_n_nearest_locations(lat, long, n_nearest)
                    if indexes:
                        index_list = ', '.join([f"'{idx}'" for idx in indexes])
                        query += f" AND index IN ({index_list})"
            except Exception as e:
                raise RuntimeError("Failed to process location-based filtering for given lat and long.") from e

        # Add filters for years
        if years:
            years_list = ', '.join([f"'{year}'" for year in years])
            query += f" AND year IN ({years_list})"

        # Add filters for months
        if months:
            month_list = ', '.join([str(month) for month in months])
            query += f" AND CAST(mohr AS INT) / 100 IN ({month_list})"
        
        # Add filters for hours
        if hours:
            hour_list = ', '.join([str(hour) for hour in hours])
            query += f" AND CAST(mohr AS INT) % 100 IN ({hour_list})"

        if varset:
            query += f" AND varset = '{varset}'"   
        
        if group_by_columns:
            query += f" GROUP BY {', '.join(group_by_columns)}"
        
        # Add ORDER BY clause if specified
        if order_by:
            if order_direction.upper() not in ['ASC', 'DESC']:
                raise ValueError("Invalid order_direction. Use 'ASC' or 'DESC'.")
            query += f" ORDER BY {order_by} {order_direction.upper()}"
        
        result_df = self.query_athena(query, reduce_poll=True)
        
        return result_df
    
    def compute_average_windspeed_1224(self,
        lat: float = None,
        long: float = None,
        height: float = None,
        group_by_year: bool = False,
        group_by_month: bool = False,
        group_by_hour: bool = False,
        order_by: str = None,
        order_direction: str = 'ASC',
        varset: str = "all"
        ) -> pd.DataFrame:
        """
        Calculate windspeed average (Overall, Yearly, Monthly or Hourly Averages) for a specified height at a specific location.

        :param lat: Latitude for location filtering. This parameter is required.
        :type lat: float
        :param lon: Longitude for location filtering. This parameter is required.
        :type lon: float
        :param heights: List of heights for which windspeed statistics are calculated. This parameter is required.
        :type heights: list[float]
        :param group_by_year: If True, calculates yearly averages. If False, calculates a global average across all years. Default is False.
        :type group_by_year: bool, optional
        :param group_by_month: If True, calculates monthly averages. If False, calculates a global average across all months. Default is False.
        :type group_by_month: bool, optional
        :param group_by_hour: If True, calculates hourly averages. If False, calculates a global average across all hours. Default is False.
        :type group_by_hour: bool, optional
        :param order_by: Column name or computed field to order the results by. Optional.
        :type order_by: str or None
        :param order_direction: Direction of ordering, either 'ASC' (default) or 'DESC'. Optional.
        :type order_direction: str, optional
        :param varset: Variable set to filter data. Default is "all".
        :type varset: str
        :return: A pandas DataFrame containing the windspeed statistics, grouped by year, month, or hour if specified.
        :rtype: pandas.DataFrame
        """
        ## 1. Validate Latitude and Longitude
        if lat is not None and not isinstance(lat, (int, float)):
            raise TypeError("Parameter 'lat' must be a numeric value (int or float).")
        if long is not None and not isinstance(long, (int, float)):
            raise TypeError("Parameter 'long' must be a numeric value (int or float).")
        if (lat is None and long is not None) or (long is None and lat is not None):
            raise ValueError("Both 'lat' and 'long' must be provided together for location filtering.")
        
        # Validate height
        if height is None or not isinstance(height, (int, float)):
            raise ValueError("Parameter 'height' must be a numeric value (int or float) and cannot be None.")
    
        self._reset_index_(lat,long)
        
        columns = self.column_names.copy()

        # Find the relevant windspeed column based on height
        if height is not None:
            windspeed_column = f"windspeed_{height}m"
            if windspeed_column not in columns:
                raise ValueError(f"Column '{windspeed_column}' does not exist in the table.")
        else:
            raise ValueError("Please specify height")
        
        # Construct the SELECT clause
        avg_column_name = f"{windspeed_column}_avg"  # Reflect height in the column name
        select_clause = [f"AVG({windspeed_column}) AS {avg_column_name}"]

        # Add grouping columns conditionally
        group_by_columns = []
        # Include year in the SELECT clause if yearly is True
        if group_by_year:
            select_clause.append("year")
            group_by_columns.append("year")
        
        if group_by_month:
            select_clause.append("CAST(mohr AS INT) / 100 AS month")
            group_by_columns.append("CAST(mohr AS INT) / 100")
        
        if group_by_hour:
            select_clause.append("CAST(mohr AS INT) % 100 AS hour")
            group_by_columns.append("CAST(mohr AS INT) % 100")
        
        # Ensure the order_by column is in the SELECT clause
        if order_by and order_by.lower() not in [col.split(" AS ")[-1] for col in select_clause]:
            raise ValueError(f"The order_by column '{order_by}' must be included in the SELECT statement. Here are the selected columns for this query: {', '.join(select_clause)}")
        
        query = f"SELECT {', '.join(select_clause)} FROM {self.athena_table_name} WHERE 1=1"
        
        # Add filters for location
        if lat is not None and long is not None:
            index = self.find_nearest_location(lat, long)
            query += f" AND index IN ('{index}')"
        
        if varset:
            query += f" AND varset = '{varset}'"
        
        if group_by_columns:
            query += f" GROUP BY {', '.join(group_by_columns)}"
        
        # Add ORDER BY clause if specified
        if order_by:
            if order_direction.upper() not in ['ASC', 'DESC']:
                raise ValueError("Invalid order_direction. Use 'ASC' or 'DESC'.")
            query += f" ORDER BY {order_by} {order_direction.upper()}"
        
        # Execute the query and return the result as a DataFrame
        result_df = self.query_athena(query, reduce_poll=True)
        return result_df
    
    def fetch_timeseries_1224(self,
        lat: float = None, 
        long: float = None, 
        heights: list[float] = None, 
        years: list[int] = None,
        n_nearest = 1,
        varset: str = "all") -> pd.DataFrame:
        """
        Fetch windspeed and wind direction time series for a given latitude, longitude, and height(s).
        Optionally, filter the data by specific year(s) and fetch data for the nearest location(s).

        :param lat: Latitude of the target location. This parameter is required.
        :type lat: float
        :param long: Longitude of the target location. This parameter is required.
        :type long: float
        :param heights: List of heights (e.g., [10.0, 50.0]) for which wind data should be retrieved. This parameter is required.
        :type heights: list[float]
        :param years: List of years (e.g., [2001, 2002]) to filter the data. This parameter is optional.
        :type years: list[int] or None
        :param n_nearest: Number of nearest locations to include in the query. Must be between 1 and 16. Default is 1.
        :type n_nearest: int
        :param varset: Variable set to filter data. Default is "all".
        :type varset: str
        :return: A pandas DataFrame containing windspeed and wind direction time series data.
        :rtype: pandas.DataFrame
        """
        # Validate inputs
        if lat is None or long is None:
            raise ValueError("Parameters 'lat' and 'long' must be provided.")
        # Validate heights
        if not heights or not isinstance(heights, list):
            raise TypeError("Parameter 'heights' must be a non-empty list.")
        if not all(isinstance(height, (int, float)) for height in heights):
            raise ValueError("All elements in 'heights' must be numeric values (int or float).")
        
        if not isinstance(n_nearest, int) or not (1 <= n_nearest <= 16):
            raise ValueError("Parameter 'n_nearest' must be an integer between 1 and 16.")
        
        if not isinstance(lat, (int, float)):
            raise ValueError("Parameter 'lat' must be a numeric value (int or float).")
        if not isinstance(long, (int, float)):
            raise ValueError("Parameter 'long' must be a numeric value (int or float).")
        
        # Validate years
        if years is not None:
            if not isinstance(years, list):
                raise TypeError("Parameter 'years' must be a list of integers.")
            if not all(isinstance(year, int) for year in years):
                raise ValueError("All elements in 'years' must be integers.")
        
        self._reset_index_(lat,long)
        
        # Find the nearest relevant columns for the specified height
        try:
            columns = self.find_relevant_columns(heights)
            columns = [col for col in columns if col.startswith('windspeed') or col.startswith('winddirection')]
            if not columns:
                raise ValueError("Could not find relevant columns for 'windspeed' or 'winddirection' at the specified height.")
            columns.extend(['year', 'mohr','index'])
        except Exception as e:
            raise RuntimeError("Failed to find relevant columns for the specified height.") from e
        

        # Construct the query
        columns_str = ', '.join(columns)
        query = f"SELECT {columns_str} FROM {self.default_athena_table_name} WHERE 1=1"

        # Add years filter if provided
        if years:
            years_list = ', '.join([f"'{year}'" for year in years])
            query += f" AND year IN ({years_list})"

        # Add location filter
        try:
            if n_nearest == 1:
                index = self.find_nearest_location(lat, long)
                query += f" AND index IN ('{index}')"
            else:
                indexes = self.find_n_nearest_locations(lat, long, n_nearest)
                if indexes:
                    index_list = ', '.join([f"'{idx}'" for idx in indexes])
                    query += f" AND index IN ({index_list})"
        except Exception as e:
            raise RuntimeError("Failed to process location-based filtering for given lat and long.") from e
        
        if varset:
            query += f" AND varset = '{varset}'"

        # Execute the query
        try:
            result_df = self.query_athena(query)
        except Exception as e:
            raise RuntimeError("Failed to execute query and fetch results.") from e

        return result_df
    
    def fetch_windspeed_map_1224(self, 
        height: float = None, 
        year: int = None,
        month: int = None,
        hour: int = None,
        varset: str = "all")-> pd.DataFrame:
        """
        Fetch windspeed map data for specified height for a specific year, month and hour.

        :param height: Height (e.g. 10, 100, 200..) for which windspeed data should be retrieved. This parameter is required.
        :type height: float
        :param year: Year to filter the data. This parameter is required. Range(2001-2020).
        :type year: int
        :param month: Month (e.g. 1, 2, 12...) to filter the data. This parameter is required. Range(1-12).
        :type month: int
        :param hour: Hour (e.g. 1, 6, 12, 18...) to filter the data. This parameter is required. Range(1-24).
        :type hour: int
        :param varset: Variable set to filter data. Default is "all".
        :type varset: str
        :return: A pandas DataFrame containing windspeed map data.
        :rtype: pandas.DataFrame
        """
        # Validate heights
        if height is None:
            raise ValueError("Parameter 'height' is required.")
        if not isinstance(height, (int, float)):
            raise TypeError("Parameter 'height' must be of 'int' or 'float' type.")
        
        if year is None or month is None or hour is None:
            raise ValueError("Values for parameters 'year', 'month' and 'hour' list must be specified.")
        
        # 4. Validate `years`
        if year is not None:
            if not isinstance(year, int):
                raise ValueError("Parameter 'year' must be a integer.")

        # 5. Validate `months`
        if month is not None:
            if not isinstance(month, int) or not 1<=month<=12:
                raise ValueError("Parameter 'month' must be a integer with range (1-12).")

        # 7. Validate `hours`
        if hour is not None:
            if not isinstance(hour, int) or not 1<=hour<=24:
                raise ValueError("Parameter 'hour' must be a integer with range (1-24).")
        
        self._reset_index_(None,None)
        
        # Find the nearest relevant columns for the specified height
        try:
            columns = self.find_relevant_columns([height])
            columns = [col for col in columns if col.startswith('windspeed')]
            if not columns:
                raise ValueError("Could not find relevant columns for 'windspeed' at the specified height.")
        except Exception as e:
            raise RuntimeError("Failed to find relevant columns for the specified height.") from e
        

        # Construct the query
        columns_str = ', '.join(columns)
        
        query = f"SELECT {columns_str}"
        query += f", regexp_extract(\"$path\", '.*/index=([^/]+)/.*', 1) AS index"
        query += f" FROM {self.athena_table_name} WHERE 1=1"

        if year:
            query += f" AND year = '{year}'"

        if month:
            query += f" AND CAST(mohr AS INT) / 100 = {month}"

        if hour:
            query += f" AND CAST(mohr AS INT) % 100 = {hour}"

        if varset:
            query += f" AND varset = '{varset}'"

        # Execute the query
        try:
            result_df = self.query_athena(query, return_result_location=True)
        except Exception as e:
            raise RuntimeError("Failed to execute query and fetch results.") from e

        return result_df
        
    def fetch_winddirection_map_1224(self, 
        height: float = None, 
        year: int = None,
        month: int = None,
        hour: int = None,
        varset: str = "all")-> pd.DataFrame:
        """
        Fetch winddirection map data for specified height, year, month and hour.

        :param height: Height (e.g. 10, 100, 200..) for which wind data should be retrieved. This parameter is required.
        :type height: float
        :param year: Year to filter the data. This parameter is required. Range(2001-2020).
        :type year: int
        :param month: Month (e.g. 1, 2, 12...) to filter the data. This parameter is required. Range(1-12).
        :type month: int
        :param hour: Hour (e.g. 1, 6, 12, 18...) to filter the data. This parameter is required. Range(1-24).
        :type hour: int
        :return: A pandas DataFrame containing winddirection map data.
        :param varset: Variable set to filter data. Default is "all".
        :type varset: str
        :rtype: pandas.DataFrame
        """
        # Validate heights
        if height is None:
            raise ValueError("Parameter 'height' is required.")
        if not isinstance(height, (int, float)):
            raise TypeError("Parameter 'height' must be of 'int' or 'float' type.")
        
        if year is None or month is None or hour is None:
            raise ValueError("Values for parameters 'year', 'month' and 'hour' list must be specified.")
        
        # 4. Validate `years`
        if year is not None:
            if not isinstance(year, int):
                raise ValueError("Parameter 'year' must be a integer.")

        # 5. Validate `months`
        if month is not None:
            if not isinstance(month, int) or not 1<=month<=12:
                raise ValueError("Parameter 'month' must be a integer with range (1-12).")

        # 7. Validate `hours`
        if hour is not None:
            if not isinstance(hour, int) or not 1<=hour<=24:
                raise ValueError("Parameter 'hour' must be a integer with range (1-24).")
        
        self._reset_index_(None,None)
        
        # Find the nearest relevant columns for the specified height
        try:
            columns = self.find_relevant_columns([height])
            columns = [col for col in columns if col.startswith('winddirection')]
            if not columns:
                raise ValueError("Could not find relevant columns for 'winddirection' at the specified height.")
        except Exception as e:
            raise RuntimeError("Failed to find relevant columns for the specified height.") from e
        

        # Construct the query
        columns_str = ', '.join(columns)
        
        query = f"SELECT {columns_str}"
        query += f", regexp_extract(\"$path\", '.*/index=([^/]+)/.*', 1) AS index"
        query += f" FROM {self.athena_table_name} WHERE 1=1"

        if year:
            query += f" AND year = '{year}'"

        if month:
            query += f" AND CAST(mohr AS INT) / 100 = {month}"

        if hour:
            query += f" AND CAST(mohr AS INT) % 100 = {hour}"
        
        if varset:
            query += f" AND varset = '{varset}'"

        # Execute the query
        try:
            result_df = self.query_athena(query, return_result_location=True)
        except Exception as e:
            raise RuntimeError("Failed to execute query and fetch results.") from e

        return result_df