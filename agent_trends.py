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
hv.extension('bokeh')

data_folder = Path('./data')


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
    return df


def read_csv_files(data_root='./data', glob='*/agents.csv'):
    '''Read all csv files in data_root that match glob and return a cominded dataframe'''
    df = pd.concat([pd.read_csv(file) for file in Path(data_root).glob(glob)])
    df['date'] = df['date'].map(pd.to_datetime)
    return df.sort_values(by='date')


def analyze_day(df, day):
    yesterday = day - timedelta(days=1)
    # filter dataframe and grab uuids for set operations
    agents_today = set(df[df['date'] == day]['uuid'])
    agents_yesterday = set(df[df['date'] == yesterday]['uuid'])
    
    new_agents = agents_today - agents_yesterday
    unlinked_agents = agents_yesterday - agents_today
    
    today_folder = data_folder / str(day.date())
    today_df = df[df['date'] == day]
    yesterday_df = df[df['date'] == yesterday]

    # write new to csv
    pd.DataFrame(today_df[today_df['uuid'].isin(new_agents)]).to_csv(today_folder / 'new.csv', index=False)
    # write missing to csv
    pd.DataFrame(yesterday_df[yesterday_df['uuid'].isin(unlinked_agents)]).to_csv(today_folder / 'unlinked.csv', index=False)
    
    output_record = {
        'date': str(day.date()),
        'total_agents': len(agents_today),
        'new_agents': len(new_agents),
        'unlinked_agents': len(unlinked_agents)
    }
    return output_record
    
    

def main():
    df = read_csv_files()

    stats = pd.DataFrame.from_records([analyze_day(df, day) for day in df['date'].unique()])

    agent_df = read_csv_files(glob='*/agents.csv')

    date_count = agent_df[['date', 'uuid']].groupby('date').count()

    width = 1000
    line = stats.hvplot(x='date', y=['total_agents'], width=width)
    bars = stats.hvplot.bar(x='date', y=['new_agents', 'unlinked_agents'], width=width)

    # width = 900
    # line = hv.Curve(date_count, x='date', y='uuid', width=width)
    # bars = stats.hvplot.bar(x='date', y=['new_agents', 'unlinked_agents'], width=width)

    output = pn.Column(
        pn.Row(line, styles={'background': '#dfdfed'}), 
        pn.Row(bars, styles={'background': '#dfdfed'}), 
        styles={'background':'#222222'}, width=1000
    ).servable()

    # output.show()

    # from holoviews import opts

    # layout = hv.Layout(line + bars).opts(opts.Layout(shared_axes=False))


if __name__ == '__main__':
    main()
