import os
import json
import glob
import time
import pandas as pd
import sqlalchemy
import logging
import argparse

# read inputs
parser = argparse.ArgumentParser(description='SG weather data ETL')
parser.add_argument('--indir',  type=str, required=True, help="input directory")
parser.add_argument('--logfile', type=str, required=True, help="log file path")
parser.add_argument('--dbpath', type=str, required=True, help="output database connection string")
parser.add_argument('--tcut', type=float, required=False, help="file expiry time (secs)", default=3110400)
args = parser.parse_args()
pars = vars(args)

# parse inputs
input_datadir = pars['indir']
logfilename   = pars['logfile']
db_path       = pars['dbpath']
dt_cut        = pars['tcut']

# setup logging facilities
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfilename)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# select relevant files 
def select_files(input_datadir, datatype, curtime, dt_cut):
	# get all files
	files = glob.glob(input_datadir+'/'+datatype+'/*')

	sel_files = []
	for filename in files:
		_timestamp = os.path.basename(filename).split('-')[0]
		_timestamp = int(_timestamp)
		dt = (curtime - _timestamp) / 1000.
		if dt < dt_cut:
			sel_files.append(filename)
		# end if
	# end for
	logger.info('%d files selected' % (len(sel_files),))
	return sel_files
# end def

def extract_data(filename, datatype):
	assert datatype in ['air-temperature', 'rainfall']
	with open(filename) as fin:
		data = json.load(fin)
	# end with

	# get station id dictionary
	station_ids = {}
	for dat in data['metadata']['stations']:
		#at = dat[0]
		station_ids[dat['device_id']] = dat['name']
	# end for

	item_data = data['items'][0]
	# get datetime
	_date = str(item_data['timestamp'].split('T')[0])

	recs = []
	for dat in item_data['readings']:
		# get station
		_station = station_ids[dat['station_id']]
		# get value
		value = dat['value']
		# make to dict
		_item = {
		  'Station': _station, 
		  'Datetime': _date,
		} # end item
		if datatype == 'rainfall':
			_item['Daily Rainfall Total (mm)'] = value
		elif datatype == 'air-temperature':
			_item['Mean Temperature (C)'] = value
		# end if

		recs.append(_item)
	# end for

	return recs
# end def

curtime = time.time()*1000
df_data = {}
for datatype in ['rainfall', 'air-temperature']:
	sel_files = select_files(input_datadir, datatype, curtime, dt_cut)

	Recs = []
	for filename in sel_files:
		Recs.extend(extract_data(filename, datatype))
	# end for

	# make dataframe
	df_content = pd.DataFrame(Recs)

	keys = ['Station', 'Datetime', 'Daily Rainfall Total (mm)', 
			'Highest 30 Min Rainfall (mm)', 'Highest 60 Min Rainfall (mm)', 
			'Highest 120 Min Rainfall (mm)', 'Mean Temperature (C)', 
			'Maximum Temperature (C)', 'Minimum Temperature (C)', 
			'Mean Wind Speed (km/h)', 'Max Wind Speed (km/h)']
	df = pd.DataFrame([], columns=keys)
	df = df.merge(df_content, how='outer')
	df = df[keys]

	# drop duplicates
	df = df.drop_duplicates(subset=['Station', 'Datetime'], keep='last')


	# make dataframe
	df_content = pd.DataFrame(Recs)
	df_data[datatype] = df_content
# end for

# join
keys = list(df_data.keys())
df_final = df_data[keys[0]]
for key in keys[1:]:
	df_final = df_final.merge(df_data[key], on=['Station', 'Datetime'], how='outer')
# end for


# update to database
logger.info('connecting database')
engine = sqlalchemy.create_engine('sqlite:///'+db_path)
df.to_sql('raw_data', con=engine, if_exists='replace', index=None)

