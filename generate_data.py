
import pandas as pd
import random 
import uuid 
from datetime import datetime, timedelta
from typing import List, Union, Optional
from pathlib import Path
from dotenv import load_dotenv
from tenable.io import TenableIO


def generate_random_indexes(index_size: int, df_size: int) -> List[int]:
    '''generate a list of random indexes to use with a dataframe with df_size records'''
    indexes = set()
    while len(indexes) < index_size:
        indexes.add(random.randint(0, df_size-1))
    return list(indexes)
        
    
def get_random_agents(agent_df: pd.DataFrame, number: int) -> pd.DataFrame:
    '''copy some number of random records from agent_df, generate a unique uuid for each record '''
    indexes = generate_random_indexes(number, len(agent_df))
    df = agent_df.iloc[indexes].copy().reset_index(drop=True)
    df['uuid'] = [str(uuid.uuid4()) for i in range(number)] 
    return df


def next_day(agent_df: pd.DataFrame, num_new: int, num_removed: int):
    '''add num_new records and remove num_removed records agent_df '''
    if num_removed > len(agent_df):
        num_removed = len(agent_df)
    new_agents = get_random_agents(agent_df, num_new)
    # remove the first num_removed rows and append new agents
    return pd.concat([agent_df.iloc[num_removed:], new_agents]).reset_index(drop=True)


def write_csv_files(agent_df, day_parameters, data_folder):
    '''For each item in day_parametrs, generate a new day of data and write it to a csv file
    
    Args:
        agent_df: dataframe with agent records
        day_parameters: list of dictionaries with deltas the next day dict(num_new=10, num_removed=30),
        data_folder: folder that contains the date folders

        
    '''

    today = datetime.now()
    data_folder = Path(data_folder)
    data_folder.mkdir(exist_ok=True)
    
    day_df = agent_df.copy()
    day_df['date'] = [today.date() for i in range(len(day_df))]
    date_path = data_folder / str(today.date())
    date_path.mkdir(exist_ok=True)
    day_df.to_csv(date_path / f'agents.csv', index=False)
    print(f'day_df has {day_df.shape} on {today.date()}')
    
    for day, params in enumerate(day_parameters):
        the_date = (today + timedelta(days=day)).date()
        
        day_df = next_day(day_df, **params)
        day_df['date'] = [the_date for i in range(len(day_df))]
        print(f'day_df has {day_df.shape} on {the_date}')

        date_path = data_folder / str(the_date)
        date_path.mkdir(exist_ok=True)
        day_df.to_csv(date_path / f'agents.csv', index=False)
        
    
def generate_test_data(agent_df: pd.DataFrame, data_folder: str):
    '''Generate test data for the dashboard'''
    day_parameters = [
        # dict(num_new=80, num_removed=0),
        # dict(num_new=80, num_removed=0),
        dict(num_new=20, num_removed=0),
        dict(num_new=20, num_removed=0),
        dict(num_new=10, num_removed=0),
        dict(num_new=5, num_removed=5),
        dict(num_new=20, num_removed=5),
        dict(num_new=10, num_removed=50),
        dict(num_new=30, num_removed=0),
        dict(num_new=0, num_removed=0),
        dict(num_new=0, num_removed=5),
        # dict(num_new=80, num_removed=0),
        # dict(num_new=80, num_removed=0),
        dict(num_new=20, num_removed=0),
        dict(num_new=20, num_removed=0),
        dict(num_new=10, num_removed=0),
        dict(num_new=5, num_removed=5),
        dict(num_new=20, num_removed=5),
        dict(num_new=10, num_removed=50),
        dict(num_new=30, num_removed=0),
        dict(num_new=0, num_removed=0),
        dict(num_new=0, num_removed=5)
    ]
    write_csv_files(agent_df, day_parameters, data_folder)
