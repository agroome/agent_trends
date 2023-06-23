
import pandas as pd
import panel as pn
from datetime import datetime, timedelta
from random import randint
from typing import List, Union, Optional
from uuid import uuid4
from pathlib import Path
import hvplot.pandas
import holoviews as hv
from dotenv import load_dotenv
from tenable.io import TenableIO

hv.extension('bokeh')

# display_columns = [
# 'id', 'uuid', 'name', 'ip','last_scanned', 'linked_on', 'last_connect', 'status', 'network_name', 'groups'
# ]

def download_agents(
        tio: TenableIO, 
        data_folder: Union[str, Path], 
        date_str: Optional[str] = None,
        columns: Optional[List[str]] = None
        ) -> pd.DataFrame:
    '''Download and clean agent records from Tenable.io.

    Download the dataframe to a folder with inside data_folder with the name of either the current
    date or the date_str parameter. For example, if data_folder is './data' and date_str is '2023-07-15', 
    the file will be './data/2023-07-15/agents.csv'. The dataframe is also returned.
    
    If date_str is None, the current date is used. If columns is None,
    all columns are kept. Otherwise, only the columns in the columns list are kept.
    
    Args:
        tio: TenableIO object
        data_folder: folder that contains the date folders
        date_str: name of folder inside data_dir, must be YYY-MM-DD format 
        columns: list of columns to keep in the dataframe
    
    '''

    df = pd.json_normalize(tio.agents.list())
    date_columns = ['last_scanned', 'linked_on', 'last_connect']
    df[date_columns] = df[date_columns].apply(pd.to_datetime, unit='s')

    # figure out path and create folder(s) if necessary
    if date_str is None:
        date_str = str(datetime.now().date())
    date_folder = Path(data_folder) / date_str
    date_folder.mkdir(exist_ok=True, parents=True)

    # df.to_csv(date_folder / f'agents.csv', index=False)
    # print(f'agents.csv saved to {date_folder}')
    return df



def generate_random_indexes(index_size: int, df_size: int) -> List[int]:
    '''generate a list of random indexes to use with a dataframe with df_size records'''
    indexes = set()
    while len(indexes) < index_size:
        indexes.add(randint(0, df_size-1))
    return list(indexes)
        
    
def get_random_agents(agent_df: pd.DataFrame, number: int) -> pd.DataFrame:
    '''copy some number of random records from agent_df, generate a unique uuid for each record '''
    indexes = generate_random_indexes(number, len(agent_df))
    df = agent_df.iloc[indexes].copy().reset_index(drop=True)
    df['uuid'] = [str(uuid4()) for i in range(number)] 
    return df


def next_day(agent_df: pd.DataFrame, num_new: int, num_removed: int):
    '''add num_new records and remove num_removed records agent_df '''
    if num_removed > len(agent_df):
        num_removed = len(agent_df)
    new_agents = get_random_agents(agent_df, num_new)
    # remove the first num_removed rows and append new agents
    return pd.concat([agent_df.iloc[num_removed:], new_agents]).reset_index(drop=True)


def write_csv_files(agent_df, day_parameters, data_folder='./data'):
    '''For each item in day_parametrs, generate a new day of data and write it to a csv file
    
    Args:
        agent_df: dataframe with agent records
        day_parameters: list of dictionaries with deltas the next day dict(num_new=10, num_removed=30),

        
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
        
    
def generate_test_data(agent_df: pd.DataFrame, data_folder: str = './data'):
    '''Generate test data for the dashboard'''
    day_parameters = [
        dict(num_new=20, num_removed=0),
        dict(num_new=20, num_removed=0),
        dict(num_new=50, num_removed=0),
        dict(num_new=10, num_removed=0),
        dict(num_new=0, num_removed=20),
        dict(num_new=20, num_removed=20),
        dict(num_new=10, num_removed=30),
        dict(num_new=30, num_removed=10),
        dict(num_new=0, num_removed=10),
        dict(num_new=0, num_removed=5)
    ]
    write_csv_files(agent_df, day_parameters, data_folder)


def main():
    load_dotenv()
    tio = TenableIO()
    agent_df = download_agents(tio, data_folder='./data')
    generate_test_data(agent_df, './data')


if __name__ == '__main__':
    main()

# def read_csv_files(data_root='./data', glob='*/agents.csv'):
#     return pd.concat([pd.read_csv(file) for file in Path(data_root).glob(glob)])

# df = read_csv_files()
# df['date'] = df['date'].map(pd.to_datetime)
# df = df.sort_values(by='date')

# def read_csv_files(data_root='./data', glob='*/agents.csv'):
#     return pd.concat([pd.read_csv(file) for file in Path(data_root).glob(glob)])

# def analyze_day(df, day):
#     yesterday = day - timedelta(days=1)
#     # filter dataframe and grab uuids for set operations
#     agents_today = set(df[df['date'] == day]['uuid'])
#     agents_yesterday = set(df[df['date'] == yesterday]['uuid'])
    
#     new_agents = agents_today - agents_yesterday
#     unlinked_agents = agents_yesterday - agents_today
    
#     today_folder = data_folder / str(day.date())
#     today_df = df[df['date'] == day]
#     yesterday_df = df[df['date'] == yesterday]

#     # write new to csv
#     pd.DataFrame(today_df[today_df['uuid'].isin(new_agents)]).to_csv(today_folder / 'new.csv', index=False)
#     # write missing to csv
#     pd.DataFrame(yesterday_df[yesterday_df['uuid'].isin(unlinked_agents)]).to_csv(today_folder / 'unlinked.csv', index=False)
    
#     output_record = {
#         'date': str(day.date()),
#         'total_agents': len(agents_today),
#         'new_agents': len(new_agents),
#         'unlinked_agents': len(unlinked_agents)
#     }
#     return output_record
    
    

# stats = pd.DataFrame.from_records([analyze_day(df, day) for day in df['date'].unique()])

# agent_df = read_csv_files(glob='*/agents.csv')

# date_count = agent_df[['date', 'uuid']].groupby('date').count()

# width = 1000
# line = stats.hvplot(x='date', y='total_agents', width=width)
# bars = stats.hvplot.bar(x='date', y=['new_agents', 'unlinked_agents'], width=width)

# width = 900
# line = hv.Curve(date_count, x='date', y='uuid', width=width)
# bars = stats.hvplot.bar(x='date', y=['new_agents', 'unlinked_agents'], width=width)

# output = pn.Column(
#     pn.Row(line, styles={'background': '#dfdfed'}), 
#     pn.Row(bars, styles={'background': '#dfdfed'}), 
#     styles={'background':'#222222'}, width=1000
# ).servable()

# # output.show()

# # from holoviews import opts

# # layout = hv.Layout(line + bars).opts(opts.Layout(shared_axes=False))



