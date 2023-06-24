import pandas as pd
import hvplot.pandas
import panel as pn

pn.extension()


stats = pd.read_csv('data/stats.csv')
print(f'read {len(stats)} records from stats.csv')

bars = stats.hvplot.bar(x='date', y=['total_agents', 'new_agents', 'unlinked_agents'], rot=90, width=1200, height=400)
bars = pn.Row(bars, sizing_mode='stretch_width')
bars.servable()
