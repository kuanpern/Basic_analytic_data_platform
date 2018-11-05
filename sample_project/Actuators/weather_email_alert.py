# import libraries
import os
import sys
import time
import json
import logging
import datetime
import argparse
import sqlalchemy
import pandas as pd

# import actuator (note: in production, use function from actuators repo
import easymail
sendmail = easymail.sendmail

# read inputs
parser = argparse.ArgumentParser(description='Example send alert program')
parser.add_argument('--sender', type=str, required=True, help="sender email address")
parser.add_argument('--logfile', type=str, required=True, help="log file path")
parser.add_argument('--pred_dbpath', type=str, required=True, help="prediction database full path")
parser.add_argument('--user_dbpath', type=str, required=True, help="user database full path")
args = parser.parse_args()
pars = vars(args)

# parse inputs
sender      = pars['sender']
logfilename = pars['logfile']
pred_dbpath = pars['pred_dbpath']
user_dbpath = pars['user_dbpath']

# setup logging facilities
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfilename)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# read prediction
engine = sqlalchemy.create_engine('sqlite:///'+pred_dbpath)
df_pred = pd.read_sql('weather_prediction', con=engine)

# get latest prediction
last_pred = df_pred.sort_values(by='datetime').iloc[-1]['prediction']
last_pred = json.loads(last_pred)

# open subscriber list
engine = sqlalchemy.create_engine('sqlite:///'+user_dbpath)
df_pred_subs = pd.read_sql('weather_prediction', con=engine)

selectd_IDs = []
for item in df_pred_subs.transpose().to_dict().values():
    params = json.loads(item['params'])
    thr = float(params['threshold'])/100.
    if (1-last_pred['0']) > thr:
        selectd_IDs.append(item['ID'])
    # end if
# end for
logger.info('sending alerts to %d subscriptions' % (len(selectd_IDs),))

# open subscriber email list
df_user = pd.read_sql('user', con=engine)
sel_items = df_user[df_user['ID'].isin(selectd_IDs)]
sel_items = sel_items.transpose().to_dict().values()

# usually a buffer layer here. skip for now.

# send email
subject = 'Bring weather equipment'
for subscriber in sel_items:
    username  = subscriber['name']
    tgt_email = subscriber['email']

    text = 'Hi {username}, please bring umbrelaa for the day'.format(
        username = username
    )
    logger.info('send email to user: "%s"' % (subscriber,))
    sendmail(sender, tgt_email, subject, text)
# end for

