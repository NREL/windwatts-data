import pandas as pd
from .client_base import client_base
from scipy.interpolate import BarycentricInterpolator

class WindwattsWTKClient(client_base):
    """
    WindwattsWTKClient interfaces with Wind ToolKit data to fetch wind speed timeseries
    and perform analysis like global, yearly, monthly, and hourly averages at a given height.
    It can also interpolate wind speeds at heights not explicitly present in the dataset.
    """
    def __init__(self, config_path: str = None):
        """
        Initialize the WindwattsWTKClient.

        :param config_path: Optional path to the config file.
        :type config_path: str or None
        """
        super().__init__(config_path, data='wtk')
        self.global_avg : float = None
        self.yearly_avg  : float= None
        self.monthly_avg : float = None
        self.hourly_avg : float = None
        self.valid_avg_types = ['global', 'yearly', 'monthly', 'hourly']

    def windspeed_interpolated_d1(self, windspeeds, heights, target_height):
        """
        Linearly interpolate windspeed at a specified height based on two known height-windspeed pairs.

        :param windspeeds: Wind speed values at two known heights, aligned with `heights`.
        :type windspeeds: pandas.Series
        :param heights: List containing two known heights.
        :type heights: list[int]
        :param target_height: Height at which to estimate the windspeed.
        :type target_height: int
        :return: Interpolated wind speed at the target height, rounded to 2 decimal places.
        :rtype: float
        """
        # Extract the two known heights
        h1, h2 = heights[0], heights[1]
        # Extract windspeeds at two known heights
        w1, w2 = windspeeds.iloc[0], windspeeds.iloc[1]
        # Apply linear interpolation formula and round the result
        return round(w1 + (target_height - h1) * (w2 - w1) / (h2 - h1),2)
    
    def interpolate_windspeed(self, height) -> pd.DataFrame:
        """
        Interpolates windspeed for a height not explicitly present in the dataset.

        :param height: Desired height at which to interpolate windspeed.
        :type height: float
        :raises ValueError: If suitable model heights are not found for interpolation.
        :return: Updated dataframe containing a new interpolated windspeed column.
        :rtype: pandas.DataFrame
        """
        # Identify columns relevant to the specified height for interpolation
        model_heights = self.find_relevant_columns([height], windspeed_interpolation=True)

        # Map available windspeed columns to their respective heights
        windspeed_model_heights_dict = {}
        for col in model_heights:
            if 'windspeed' in col:
                height_str = col.split('_')[1][:-1]
                height_int = int(height_str)
                windspeed_model_heights_dict[height_int] = col

        if len(windspeed_model_heights_dict) != 2:
            raise ValueError(f"Expected 2 model heights for interpolation, got {len(windspeed_model_heights_dict)}")
        
        print(f"Interpolating windspeed at height: {height} using model heights: {list(windspeed_model_heights_dict.keys())}")

        # Extract the DataFrame with only relevant windspeed columns adjacent to height to be interpolated
        model_heights_df = self.df[windspeed_model_heights_dict.values()].copy()

        # Apply interpolation row-wise:
        # For each row, collect windspeed values from available heights (as a Series)
        # Pass these values, the list of heights, and the target height to the interpolation function and store the result as new windspeed column in dataframe
        self.df[f"windspeed_{height}m"] = model_heights_df.apply(lambda windspeeds: self.windspeed_interpolated_d1(windspeeds, list(windspeed_model_heights_dict.keys()), height), axis=1)

    def fetch_global_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate the global windspeed average at a specified location and height.

        :param lat: Latitude of the location.
        :type lat: float
        :param long: Longitude of the location.
        :type long: float
        :param height: Hub height in meters.
        :type height: int
        :raises RuntimeError: If the computation fails.
        :return: Dictionary containing the global average windspeed rounded to 2 decimals.
            Example:
            {
                "global_avg": 5.32
            }
        :rtype: dict
        """
        
        if not self._prepare_df_for_aggregation(lat, long, height, 'global'):
            return {"global_avg": self.global_avg}
        
        try:
            self.global_avg = float(round(self.df[f'windspeed_{height}m'].mean(),2))
        except Exception as e:
            raise RuntimeError(f"Failed to calculate global average for windspeed_{height}m.") from e
        
        return {
            "global_avg": self.global_avg
        }
    
    def fetch_yearly_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate yearly windspeed averages at a specified location and height.

        :param lat: Latitude of the location.
        :type lat: float
        :param long: Longitude of the location.
        :type long: float
        :param height: Hub height in meters.
        :type height: int
        :raises RuntimeError: If the computation fails.
        :return: Dictionary containing a list of yearly average windspeeds.
            Example:
            {
                "yearly_avg": [
                    {"year": 2020, "windspeed_100m": 5.23},
                    {"year": 2021, "windspeed_100m": 5.34}
                ]
            }
        :rtype: dict
        """
        
        if not self._prepare_df_for_aggregation(lat, long, height, 'yearly'):
            return {"yearly_avg": self.yearly_avg}
        
        try:
            yearly_avg_df = self.df.groupby('year')[f'windspeed_{height}m'].mean().reset_index().round(2).sort_values(by='year', ascending=True)
            self.yearly_avg = yearly_avg_df.to_dict(orient='records')
        except Exception as e:
            raise RuntimeError(f"Failed to calculate yearly average for windspeed_{height}m.") from e
        
        return {
                "yearly_avg": self.yearly_avg      
        }
    
    def fetch_monthly_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
        Calculate monthly windspeed averages at a specified location and height.

        :param lat: Latitude of the target location.
        :type lat: float
        :param long: Longitude of the target location.
        :type long: float
        :param height: Hub height at which to calculate the windspeed average.
        :type height: int
        :raises RuntimeError: If computation fails or data is not available.
        :return: Dictionary containing a list of monthly average windspeeds, each rounded to 2 decimals.
            Example:
            {
                "monthly_avg": [
                    {"month": 1, "windspeed_100m": 5.12},
                    {"month": 2, "windspeed_100m": 5.45},
                    {"month": 12, "windspeed_100m": 6.10}
                ]
            }
        :rtype: dict
        """
        
        if not self._prepare_df_for_aggregation(lat, long, height, 'monthly'):
            return {"monthly_avg": self.monthly_avg}
    
        self.df['month']=self.df['mohr']//100

        try:
            monthly_avg_df = self.df.groupby('month')[f'windspeed_{height}m'].mean().reset_index().round(2).sort_values(by='month', ascending=True)
            self.monthly_avg = monthly_avg_df.to_dict(orient='records')
        except Exception as e:
            raise RuntimeError(f"Failed to calculate monthly average for windspeed_{height}m.") from e
        
        return {
                "monthly_avg": self.monthly_avg        
        }
    
    def fetch_hourly_avg_at_height(self,
        lat: float = None,
        long: float = None,
        height: int = None) -> dict:
        """
         Calculate hourly windspeed averages at a specified location and height.

        :param lat: Latitude of the target location.
        :type lat: float
        :param long: Longitude of the target location.
        :type long: float
        :param height: Hub height at which to calculate the windspeed average.
        :type height: int
        :raises RuntimeError: If computation fails or data is not available.
        :return: Dictionary containing a list of hourly average windspeeds, each rounded to 2 decimals.
            Example:
            {
                "hourly_avg": [
                    {"hour": 0, "windspeed_100m": 5.05},
                    {"hour": 1, "windspeed_100m": 4.98},
                    {"hour": 23, "windspeed_100m": 5.22}
                ]
            }
        :rtype: dict
        """
        
        if not self._prepare_df_for_aggregation(lat, long, height, 'hourly'):
            return {"hourly_avg": self.hourly_avg}
        
        self.df['hour'] = self.df['mohr']%100

        try:
            hourly_avg_df = self.df.groupby('hour')[f'windspeed_{height}m'].mean().reset_index().round(2).sort_values(by='hour', ascending=True)
            self.hourly_avg = hourly_avg_df.to_dict(orient='records')  
        except Exception as e:
            raise RuntimeError(f"Failed to calculate hourly average for windspeed_{height}m.") from e
        
        return {
                "hourly_avg": self.hourly_avg   
        }