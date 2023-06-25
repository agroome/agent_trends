import pandas as pd
import hvplot.pandas
import panel as pn
from typing import Union
from pathlib import Path

def create_bars(file_path: Union[str, Path]):
    print("running create_bars")
    pn.extension()
    stats = pd.read_csv(file_path, parse_dates=['date'])
    print(f'read {len(stats)} records from {file_path}')
     
    bars = stats.hvplot.bar(x='date', y=['total_agents', 'new_agents', 'unlinked_agents'], rot=90, width=1200, height=400)
    bars = pn.Row(bars, sizing_mode='stretch_width')
    bars.servable()
    return bars

# create_bars('./data/stats.csv')
     