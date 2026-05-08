import pandas as pd
from datetime import date

def get_yearly_data(year) -> pd.DataFrame:
    """
    Returns electric & hot water dataframe for specified year
    """
    yearly_elec_path = f"data/processed/electrical-energy/electrical_energy_{year}.csv"
    yearly_hot_water_path = f"data/processed/hot-water-energy/hot_water_energy_{year}.csv"
        
    return pd.read_csv(yearly_elec_path), pd.read_csv(yearly_hot_water_path)

def get_merged_data(electric_df: pd.DataFrame, hot_water_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns merged electric & hot water dataframe for corresponding year
    """
    electric_df = electric_df.melt(
        id_vars=['ts'], 
        var_name='building_name', 
        value_name='electrical_energy'
    )

    hot_water_df = hot_water_df.melt(
        id_vars=['ts'],
        var_name='building_name',
        value_name='hot_water_energy'
    )

    merged_df = pd.merge(electric_df,
                        hot_water_df,
                        on=['ts', 'building_name'],
                        how='outer')
    
    merged_df['ts'] = merged_df['ts'].str.replace(' Rel', '', regex=False)
        
    merged_df = (
        merged_df
        .convert_dtypes()                 # switch to nullable dtypes (Float64, Int64)
        .where(pd.notna(merged_df), None) # replace missing with None
    )

    return merged_df
    
def get_building_data() -> pd.DataFrame:
    """
    Returns merged dataframe from yearly data of electrical & hot water data
    """
    curr_year = date.today().year
    dfs = []

    for year in range(2021, curr_year):
        elec_df, hot_water_df = get_yearly_data(year)
        yearly_merged_df = get_merged_data(elec_df, hot_water_df)
        dfs.append(yearly_merged_df)

    return pd.concat(dfs, ignore_index = True)