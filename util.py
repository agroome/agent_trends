import re
import pandas as pd
import panel as pn
from datetime import datetime, timedelta
from uuid import uuid4
from typing import List, Union, Optional
from pathlib import Path
import hvplot.pandas
import holoviews as hv
from dotenv import load_dotenv
from tenable.io import TenableIO

load_dotenv()
# hv.extension('bokeh')
pn.extension()

data_folder = Path('./data')


def download_agents(
        tio: TenableIO, 
        data_folder: Union[str, Path], 
        date_str: Optional[str] = None,
        columns: Optional[List[str]] = None
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
        date_str: name of folder inside data_dir, must be YYY-MM-DD format 
        columns: list of columns to keep in the dataframe
    
    '''

    df = pd.json_normalize(tio.agents.list())
    date_columns = ['last_scanned', 'linked_on', 'last_connect']
    df[date_columns] = df[date_columns].apply(pd.to_datetime, unit='s')

    # compute path and create folder(s) if necessary
    if date_str is None:
        date_str = str(datetime.now().date())
    date_folder = Path(data_folder) / date_str
    date_folder.mkdir(exist_ok=True, parents=True)

    df.to_csv(date_folder / f'agents.csv', index=False)
    print(f'agents saved to saved to {date_folder}/agents.csv')
    return date_str, df


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




def compute_daily_statistics(data_folder, recompute_all=False):
    '''compute statistics for the most recent date in data_folder. if recompute all is True recompute for all dates'''

    unsorted_folders = [p.name for p in Path(data_folder).glob('*') if not p.name.startswith('.')]
    if not unsorted_folders:
        print(f'no date folders found in {data_folder}')
        return
    
    # convert to datetime to sort
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
        
        print(f'compare dates: {current_date} - {previous_date}')
        print(stats)
        
        current_date = previous_date

    # combine all stats files into one
    stats = pd.concat([pd.read_csv(file, parse_dates=['date']) for file in data_folder.glob('*/stats.csv')])
    stats = stats.sort_values(by='date')
    stats.to_csv(data_folder / 'stats.csv', index=False)


def present_agent_stats(data_path: str, data_file: Optional[str] = 'stats.csv') -> pn.Column:
    '''present agent statistics in a panel dashboard with a line chart and a bar chart
    
    Args: 
        data_path: path to folder folder containing the summary stat
        data_file: name of the file containing the summary stats

    Returns:
        a panel column containing the charts

    Raises:
        FileNotFoundError: if the data file is not found
    '''

    stat_path = Path(data_path) / data_file
    if not stat_path.exists():
        raise FileNotFoundError(f'{stat_path} not found')

    stats = pd.read_csv(stat_path, parse_dates=['date'])
    print(f'reading stats from {stat_path.name} for {len(stats)} days')

    width = 1000    
    line = stats.hvplot(x='date', y='total_agents', width=width)
    bars = stats.hvplot.bar(x='date', y=['new_agents', 'unlinked_agents'], width=width)
    
    output = pn.Column(
        pn.Row(line, styles={'background': '#dfdfed'}), 
        pn.Row(bars, styles={'background': '#dfdfed'}), 
        styles={'background':'#222222'}, width=1000
    )
    
    return output




# present_data('./data')