import click
import pandas as pd
import subprocess
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from tenable.io import TenableIO
from dotenv import load_dotenv
from util import download_agents, compute_daily_statistics, present_agent_stats
from generate_data import generate_test_data

import panel as pn

load_dotenv()


@click.group()
def cli():
    pass

@cli.command()
@click.option('--data-folder', default='./data', help='base folder for data defaults to ./data')
def daily_download(data_folder: str):
    '''Download agents from Tenable.io and write to a folder for the current day.

    The folder structure is as follows: ./<data_folder>/<date>/agents.csv
     '''

    tio = TenableIO()
    click.echo(f'Downloading agents from Tenable.io')
    download_agents(tio, data_folder, date_str=str(datetime.now().date()))
    click.echo(f'Computing summary statistics')
    compute_daily_statistics(data_folder)
    click.echo(f'Completed')


@cli.command()
@click.option('--data-folder', type=click.Path(), default='./data', help='base folder for data')
@click.option('--source-file', default='agents.csv', help='source file for selecting random agents')
def generate_example_data(data_folder: Path, source_file: str):
    '''generate test data in data_folder'''

    click.echo(f'This will overwrite any existing data in {data_folder}')
    click.echo(f'Use the --data-folder option to specify a different folder.')
    click.confirm(f'Continue?', abort=True)

    data_folder = Path(data_folder)
    data_folder.mkdir(exist_ok=True, parents=True)
    file_path = data_folder / source_file

    if not file_path.exists():
        click.echo(f'Generating random records from Tenable.io')
        tio = TenableIO()
        agent_df = pd.DataFrame.from_records(tio.agents.list())
        agent_df.to_csv(file_path, index=False)
    else:
        click.echo(f'Generating random records from source_file: {file_path}')
        agent_df = pd.read_csv(file_path)


    # generate random agent agent records and compute daily statistics
    generate_test_data(agent_df, data_folder)
    compute_daily_statistics(data_folder, recompute_all=True)

    print(f'generated test data in {data_folder}')


@cli.command()
def serve():
    '''serve the dashboard'''
    command = ["panel", "serve", "test.py", "--show"]
    command = "panel serve test.py --show"
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, errors = process.communicate()

    print("Output:", output.decode())
    print("Errors:", errors.decode())

    # click.echo(f"Output: {output.decode()}")
    # click.echo(f"Errors: {errors.decode()}")


# @cli.command()
# def serve():
#     '''serve the dashboard'''
#     pn.extension()
#     from .dashboard import Dashboard
#     dashboard = Dashboard()
#     dashboard.servable()

#     pn.serve({'/app': createApp},
#         port=5000, allow_websocket_origin=["127.0.0.1:8000"],
#         address="127.0.0.1", show=False)

if __name__ == '__main__':
    cli()