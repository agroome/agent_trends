import re
import pandas as pd
import panel as pn
from datetime import datetime, timedelta
from typing import List, Union, Optional
from pathlib import Path
from tenable.io import TenableIO
from bars import create_bars

import warnings
warnings.filterwarnings("ignore", module="bokeh")


def download_agents(
        tio: TenableIO, 
        data_folder: Union[str, Path], 
        columns: Optional[List[str]] = None,
        save_csv: bool = True
        ) -> pd.DataFrame:
    '''Download and clean agent records from Tenable.io.

    Dowload agent to a folder inside data_folder. The name of the folder  will be the current
    date or the date_str parameter. For example, if data_folder is './data' and date_str is '2023-07-15', 
    the file will be './data/2023-07-15/agents.csv'. A dataframe with the results is also returned.
    
    If date_str is None, the current date is used. If columns is None,
    all columns are kept. Otherwise, only the columns in the columns list are kept.
    
    Args:
        tio: TenableIO object
        data_folder: folder that will contains the date folders, created if not found
        columns: list of columns to keep in the dataframe
        save_csv: if True, save the dataframe to a csv to file_path
    
    '''
    # download agent data from Tenable.io
    df = pd.json_normalize(tio.agents.list())
    date_columns = ['last_scanned', 'linked_on', 'last_connect']
    # convert unix timestamps to datetime
    df[date_columns] = df[date_columns].apply(pd.to_datetime, unit='s')

    # keep only the columns we want
    if columns is not None:
        df = df[columns]

    if save_csv:
        # compose path and create folder(s) including parent if necessary
        file_path = str(datetime.now().date())
        date_folder = Path(data_folder) / file_path
        file_path = date_folder / f'agents.csv'
        date_folder.mkdir(exist_ok=True, parents=True)
        df.to_csv(file_path, index=False)
        print(f'agents saved to saved to {file_path}')

    return file_path, df


def compare_dates(data_folder: Union[Path, str], current_date: str, previous_date: Optional[str] = None, ):
    '''compare values in current folder to values in the previous to compute deltas'''

    # get data from the folders we want to compare
    current_folder = Path(data_folder) / current_date
    current_df = pd.read_csv(f'{current_folder}/agents.csv')

    # if there is not a previous date, all agents are considered new
    statistics = {
        'date': current_date, 
        'total_agents': len(current_df),
        'new_agents': len(current_df),
        'unlinked_agents': 0
    }

    # if there is a previous date, compute and update stats for 'new_agent' and 'unlnked_agent' count
    if previous_date is not None:
        previous_folder = Path(data_folder) / previous_date
        previous_df = pd.read_csv(f'{previous_folder}/agents.csv')
        
        # create and compare sets of uuids to find deltas
        agents_current = set(current_df['uuid'])
        agents_previous = set(previous_df['uuid'])
        new_agents = agents_current - agents_previous
        unlinked_agents = agents_previous - agents_current
        
        # write new agents to csv file
        new_filter = current_df['uuid'].isin(new_agents)
        pd.DataFrame(current_df[new_filter]).to_csv(current_folder / 'new.csv', index=False)
        
        # write unlinked to csv file
        unlinked_filter = previous_df['uuid'].isin(unlinked_agents)
        pd.DataFrame(previous_df[unlinked_filter]).to_csv(current_folder / 'unlinked.csv', index=False)

        # add delta count to statistics
        statistics.update(dict(new_agents=len(new_agents), unlinked_agents=len(unlinked_agents)))
    
    return statistics




def compute_daily_statistics(data_folder: Union[str,Path], recompute_all: bool = False, save_image:bool = False):
    '''compute statistics for the most recent date in data_folder. if recompute all is True recompute for all dates'''

    unsorted_folders = [p.name for p in Path(data_folder).glob('*') if not p.name.startswith('.')]
    if not unsorted_folders:
        print(f'no date folders found in {data_folder}')
        return
    
    # convert to datetime to sort folder names in data_folder
    dates = sorted([pd.to_datetime(date).date() for date in unsorted_folders if bool(re.match(r'\d{4}-\d{2}-\d{2}', date))])
    current_date = str(dates.pop())
    previous_date = str(dates.pop()) if dates else None
    
    # compare dates and write statistics to csv
    stats = compare_dates(data_folder, current_date, previous_date)
    data_folder = Path(data_folder)
    current_folder = data_folder / current_date
    pd.DataFrame.from_records([stats]).to_csv(current_folder / 'stats.csv', index=False)

    current_date = previous_date
    
    while recompute_all and current_date is not None:
        previous_date = str(dates.pop()) if dates else None        
        # compare dates and write statistics to csv
        stats = compare_dates('./data', current_date, previous_date)
        current_folder = data_folder / current_date
        pd.DataFrame.from_records([stats]).to_csv(current_folder / 'stats.csv', index=False)
        current_date = previous_date

    # combine all stats files into one
    stats = pd.concat([pd.read_csv(file, parse_dates=['date']) for file in data_folder.glob('*/stats.csv')])
    stats = stats.sort_values(by='date')
    stats_file = data_folder / 'stats.csv'
    stats.to_csv(stats_file, index=False)
    print(f'statistics saved to {stats_file}')

    if save_image and not recompute_all:
        chart = create_bars(stats_file)
        chart.save(save_path=current_folder, filename='stats.png')
        print(f'statistics chart saved to {current_folder}/stats.png')