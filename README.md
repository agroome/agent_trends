# agent_trends

This module will perform a daily download of known agents from Tenable.io and create daily statistics for new and unlinked agents. The 'unlinked' agents are assumed by comparing the agents in the day's download to the agents from the previous day

## Installation

```
# 
# Clone repository into specified folder, i.e. the current folder:
# 
$ https://github.com/agroome/agent_trends.git ./agent_trends

#
# cd into the folder and create and activate a virtual environment
#
$ cd ./agent_trends
$ python -m venv venv

#
# Activate the environment (i.e, on Unix or OSX):
# 
$ source ./venv/bin/activate
(venv)$ 

#
# with virutual environment activated, install the local package using pip 
#
(venv)$ pip install .

#
# this will instaill a the 'tenb-agents' command that can be run while the venv is activated
# use tenb-agents --help or tenb-agents <cmd> --help for command syntax
#
(venv)$ pip install .

#
# Install API keys in a file in the package folder called .env
# Create a file called .env that looks like this, with your api_keys from Tenable.io
# This must be an administrator account to access agents.
#
TIO_ACCESS_KEY=YOURACCESSKEY123HERE
TIO_SECRET_KEY=YOURSECRETKEY123HERE
```
