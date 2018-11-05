# import libraries
import time
import datetime
import sqlalchemy
import pandas as pd
import argparse
import logging

# read inputs
parser = argparse.ArgumentParser(description='weather data selection and labelling')
parser.add_argument('--outdir',  type=str, required=True,  help="output dump directory")
parser.add_argument('--logfile', type=str, required=True,  help="log file path")
parser.add_argument('--nyear',   type=int, required=False, help="number of year data to use")
parser.add_argument('--dbpath',  type=str, required=True,  help="input database file full path")
args = parser.parse_args()
pars = vars(args)

# parse inputs
outdir      = pars['outdir']
dbpath      = pars['dbpath']
logfilename = pars['logfile']
nyear       = pars['nyear']

# setup logging facilities
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfilename)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# read data
logger.info('Read data')
engine = sqlalchemy.create_engine('sqlite:///'+dbpath)
df = pd.read_sql('raw_data', con=engine)

# select data
# - use only last 5 year data
logger.info('Select data')
cur_date = datetime.datetime.now()
cutoff_date = datetime.datetime(cur_date.year-nyear, cur_date.month, cur_date.day).isoformat('T').split('T')[0]
df_sel = df[df['Datetime'] > cutoff_date]

# data labelling
category_names = {
    0: 'sunny',
    1: 'drizzle',
    2: 'rainy',
    3: 'stormy'
} # end name

def categorize(s):
    if s < 50:
        return 0
    elif 50 <= s < 200:
        return 1
    elif 200 <= s < 500:
        return 2
    else:
        return 3
    # end if
# end def

logger.info('Label data')
_df = df_sel[['Datetime', 'Daily Rainfall Total (mm)']].groupby('Datetime').agg(sum)
_df['Datetime'] = _df.index

cats = list(map(categorize, _df['Daily Rainfall Total (mm)']))
logger.info('labels: ' + str({category_names[_]:cats.count(_) for _ in set(cats)}))
_df['category'] = cats

# save to file
outfilename = 'weather-data.label.%s.csv' % (str(int(time.time())),)
outfilename = outdir+'/'+outfilename
logger.info('write to file: %s' % (outfilename,))
_df.to_csv(outfilename, index=None)

