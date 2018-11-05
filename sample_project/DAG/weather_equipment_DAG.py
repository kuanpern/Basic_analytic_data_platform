import os
import yaml
import uuid
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from string import Template

# read configuration variables
conf_file = 'conf/weather-conf.yaml'
with open(conf_file) as fin:
	args = yaml.load(fin)
# end with
project_home = args['project_home']
assert type(project_home) == str, 'project home unspecified'

########################################################
############## DECLARE DAG AND PARAMETERS ##############
########################################################
DAG_name = 'weather_equipment_reminder'
dag = DAG(
  DAG_name, 
  schedule_interval = '0 20 * * *', # 8PM everyday
  default_args={
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime.datetime.now(),
  'email': ['admin@somecompany.com'],
  'email_on_failure': False,
  'email_on_retry'  : False,
  'retries': 1,
  'retry_delay': datetime.timedelta(minutes=1),
  # 'end_date': datetime(2020, 1, 1),
})
##########################################

##########################################
########### COMMAND TEMPLATES ############
##########################################
cmd_template = Template("""
export project_home="$project_home";
export conf_file="$conf_file";
cd {{ params.working_dir }}; {{ params.bash_cmd }}
""").substitute({'project_home': project_home, 'conf_file': conf_file})
##########################################


########################################
############### INGESTOR ###############
########################################
cmd_template = 'venv/bin/python download_weather_data.py --outdir {{OUTDIR}} --logfile {{LOGFILE}}'
OUTDIR  = project_home+'/data/dump/'
LOGFILE = project_home+'/logs/'+str(uuid.uuid4())+'.data-ingest.log'
_cmd = cmd_template.format(
  OUTDIR  = OUTDIR, 
  LOGFILE = LOGFILE
) # end _cmd
ING_task01 = BashOperator(task_id='Pull_weather_data',
  bash_command=cmd_template, dag=dag,
  params={
    'working_dir' : project_home + '/Ingestors', 
    'bash_cmd'    : _cmd
  }
) # end BashOperator
##########################################


##########################################
################### ETL ##################
##########################################
cmd_template = 'venv/bin/python ETL_main.py --indir {{INDIR}} --logfile {{LOGFILE}} --dbpath {{DBPATH}}'
_cmd = cmd_template.format(
  OUTDIR  = project_home+'/data/dump/',
  LOGFILE = project_home+'/logs/'+str(uuid.uuid4())+'.data-ETL.log'
) # end _cmd
ETL_task01 = BashOperator(task_id='Weather_data_ETL',
  bash_command=cmd_template, dag=dag,
  params={
    'working_dir': project_home + '/ETL', 
    'bash_cmd'   : _cmd
  }
) # end BashOperator
ETL_task01.set_upstream(ING_task01)
##########################################


########################################
############## ALGOMODULE ##############
########################################
cmd_template = 'venv/bin/python data_selection_and_labelling.py --outdir {{OUTDIR}} --logfile {{LOGFILE}} --dbpath {{DBPATH}}'
_cmd = cmd_template.format(
  OUTDIR  = 'data/',
  LOGFILE = project_home+'/logs/'+str(uuid.uuid4())+'.data-label.log', 
  DBPATH  = project_home+'/data/sg_weather.db'
) # end _cmd
ALG_task01 = BashOperator(task_id='Label_data',
  bash_command=cmd_template, dag=dag,
  params={
    'working_dir': project_home + '/AlgoModules', 
    'bash_cmd'   : _cmd
  }
) # end BashOperator
ALG_task01.set_upstream(ETL_task01)


cmd_template = 'venv/bin/python HMM_predictor.py --datadir {{DATADIR}} --dbpath {{DBPATH}} --logfile {{LOGFILE}} --config {{CONFIG}}'
_cmd = cmd_template.format(
  DATADIR = 'data/',
  DBPATH  = project_home+'/data/sg_weather.db',
  LOGFILE = project_home+'/logs/'+str(uuid.uuid4())+'.hmm-train.log',
  CONFIG="configs/hmm_predictor.yaml",
) # end _cmd
ALG_task02 = BashOperator(task_id='Train_HMM',
  bash_command=cmd_template, dag=dag,
  params={
    'working_dir': project_home + '/AlgoModules', 
    'bash_cmd'   : _cmd
  }
) # end BashOperator
ALG_task02.set_upstream(ALG_task01)

########################################
################ ACTUATOR ##############
########################################
cmd_template = 'venv/bin/python weather_email_alert.py --sender {{SENDER}} --logfile {{LOGFILE}} --pred_dbpath {{PRED_DBPATH}} --user_dbpath {{USER_DBPATH}}'
_cmd = cmd_template.format(
  SENDER      = 'kptan86@gmail.com',
  LOGFILE     = project_home+'/logs/'+str(uuid.uuid4())+'.send-email.log',
  PRED_DBPATH = project_home+'/data/sg_weather.db',
  USER_DBPATH = project_home+'/data/subscriber.db'
) # end _cmd
ACT_task01 = BashOperator(task_id='Send_reminder_email',
  bash_command=cmd_template, dag=dag,
  params={
    'working_dir': project_home + '/Actuators', 
    'bash_cmd'   : _cmd
  }
) # end BashOperator
ACT_task01.set_upstream(ALG_task02)
##########################################
