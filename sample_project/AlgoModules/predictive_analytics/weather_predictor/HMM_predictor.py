# HMM weather predictor

# Import library
import yaml
import glob
import dill
import json
import uuid
import time
import sqlite3
import logging
import datetime
import argparse
import tempfile
import numpy as np
import pandas as pd
from hmmlearn import hmm
from sklearn.utils import check_random_state

# read inputs
parser = argparse.ArgumentParser(description='SG weather HMM predictor')
parser.add_argument('--datadir', type=str, required=True, help="input data directory")
parser.add_argument('--dbpath',  type=str, required=True, help="output database full path")
parser.add_argument('--logfile', type=str, required=True, help="log file path")
parser.add_argument('--config',  type=str, required=True, help="configuration file")
args = parser.parse_args()
pars = vars(args)

# parse inputs
datadir     = pars['datadir']
dbpath      = pars['dbpath']
logfilename = pars['logfile']
configfile  = pars['config']

# setup logging facilities
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfilename)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# uuid of the job
_uuid = str(uuid.uuid4())
logger.info('UUID: %s' % (_uuid,))

# Read configuration file
logger.info('read configuration')
with open(configfile) as fin:
	_pars = yaml.load(fin)
# end with
max_iter     = _pars.get('max_iter',   100)
n_components = _pars.get('n_components', 3)
n_sample     = _pars.get('n_sample',  1000)

# Read data
files = glob.glob(datadir+'/weather-data.label.*.csv')
files.sort()
infile = files[-1]
logger.info('select latest label file: %s' % (infile,))
df = pd.read_csv(infile)
vals = df['category'].values
n_obs_states = len(set(list(vals)))

# Train model
logger.info('train model')
model = hmm.GaussianHMM(n_components=n_components, covariance_type="full", n_iter=max_iter)
vals = vals.reshape(-1, 1)
model.fit(vals);
logger.info('done')

# save model
logger.info('saving model to database')

# save and load as binary
with tempfile.NamedTemporaryFile() as fout:
	dill.dump(model, fout)
	fout.seek(0)
	_blob = fout.read()
	fout.close()
# end with
timestamp = str(int(time.time()))
model_bin = bytes(memoryview(_blob))

db = sqlite3.connect(dbpath)
db.execute('INSERT INTO weather_model (uuid, timestamp, bin) VALUES(?, ?, ?)', [_uuid, timestamp, model_bin])
db.commit()
db.close()
logger.info('done')

# Extract paramaters from model
states = model.decode(vals)[1]
emmission_mat = np.zeros([n_components, n_obs_states])

vals = [_[0] for _ in vals]
for _x, _y in zip(states, vals):
    emmission_mat[_x][_y] += 1
# end for

for row in range(len(emmission_mat)):
    emmission_mat[row] = emmission_mat[row, :] / sum(emmission_mat[row, :])
# end for

# Make predictions
logger.info('make prediction')
# sample from models
transmat_cdf = np.cumsum(model.transmat_, axis=1)
random_state = check_random_state(model.random_state)

next_state_prob = []
for i in range(n_sample):
    next_state = (transmat_cdf[states[-1]] > random_state.rand()).argmax()
    next_state_prob.append(next_state)
# end for

# compute probabilities
next_state_prob = {_state: next_state_prob.count(_state) for _state in set(next_state_prob)}

# cast to array format
swap = np.zeros([1, n_components])
for key, val in next_state_prob.items():
    swap[0, key] = val / n_sample
# end for
next_state_prob = swap

# total probability
prob = np.array(np.matrix(next_state_prob) * np.matrix(emmission_mat))[0]
prob = dict(enumerate(list(prob)))

# write result to database
logger.info('saving prediction to database')
prediction = json.dumps(prob)
timestamp = str(int(time.time()))
curdate = datetime.datetime.now().isoformat()

db = sqlite3.connect(dbpath)
db.execute('INSERT INTO weather_prediction (uuid, timestamp, datetime, prediction) VALUES(?, ?, ?, ?)', [_uuid, timestamp, curdate, prediction])
db.commit()
db.close()
logger.info('done')
