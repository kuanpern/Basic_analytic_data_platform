## Basic Data Platform
Template for data analytic pipeline.

### Introduction
This template makes use of Airflow, and is suitable for non-realtime, periodic (> minutes) analytics.

 * For real-time streaming analytics, please refer to streaming_analytic_platform
 * For real-time interactive analytics, please refer to AlphaZeroOthello repo (for an example)
 * For near real-time, periodic analysics, please refer to Scflex architecture

![Alt text](images/airflow_screenshot_01.png?raw=true "airflow_screenshot_01")
![Alt text](images/airflow_screenshot_02.png?raw=true "airflow_screenshot_02")

### Installation

#### Install Airflow
```
cd $INSTALL_DIR
export AIRFLOW_HOME=$INSTALL_DIR/airflow
virtualenv -ppython3.5 venv; venv/bin/pip install -r requirements.txt
```

#### install python virtual environments
```
cd $project_home/AlgoModules/predictive_analytics/weather_predictor; virtualenv -ppython3.5 venv; venv/bin/python -m pip install -r requirements.txt
cd $project_home/Actuators; virtualenv -ppython3.5 venv; venv/bin/python -m pip install -r requirements.txt
cd $project_home/Ingestors; virtualenv -ppython3.5 venv; venv/bin/python -m pip install -r requirements.txt
cd $project_home/DAGs; virtualenv -ppython3.5 venv; venv/bin/python -m pip install -r requirements.txt
cd $project_home/ETL; virtualenv -ppython3.5 venv; venv/bin/python -m pip install -r requirements.txt
```

#### Start airflow 
```
export AIRFLOW_HOME=$INSTALL_DIR/airflow
export PATH=$PATH:$INSTALL_DIR/venv/bin/

# initialization
$INSTALL_DIR/venv/bin/airflow initdb

# start the web server
$INSTALL_DIR/venv/bin/airflow webserver -p 8080

# start the scheduler
$INSTALL_DIR/venv/bin/airflow scheduler
```


### File tree
```
/
├── sample_project
│   ├── Ingestors   : Functionalities to ingest data from data source
│   ├── ETL         : ETL functions
│   ├── AlgoModules : Statistical analyses, algorithms and ML models
│   ├── DAG         : DAG file to specify the dependencies. To be used by Airflow scheduler
│   ├── Actuators   : Functionalities to push output, alert, dashboard etc to subscriber
│   └── utils       : Utility functions
├── airflow         : airflow home directory
│   ├── airflow.cfg : airflow configuration file
│   └── dags -> ../sample_project/DAG/
├── conf : (demo purpose only) configuration file
├── data : (demo purpose only) data source
├── keys : (demo purpose only) keyfiles directory
├── logs : (demo purpose only) directory to store log files
└── README.md
```

