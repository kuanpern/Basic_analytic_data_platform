# import libraries
import os
import time
import json
import logging
import requests
import argparse

# read inputs
parser = argparse.ArgumentParser(description='Download weather data from SG gov website')
parser.add_argument('--outdir',  type=str, required=True, help="output dump directory")
parser.add_argument('--logfile', type=str, required=True, help="log file path")
args = parser.parse_args()
pars = vars(args)

# parse inputs
outdir      = pars['outdir']
logfilename = pars['logfile']

# setup logging facilities
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(logfilename)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def download_dump_data(outdir, datatype):
	logger.info('downloading %s data' % (datatype,))
	# output filename
	timestamp = str(int(time.time()*1000))
	_outdir = outdir+os.sep+datatype+os.sep
	outfilename = _outdir+os.sep+timestamp+'-'+datatype+'.json'
	
	# query data
	url = 'https://api.data.gov.sg/v1/environment/'+datatype
	logger.info('GET %s' % (url,))
	r = requests.get(url)

	# write to file
	logger.info('write to file: %s' % (outfilename,))
	content = r.json()
	with open(outfilename, 'w') as fout:
		json.dump(content, fout, indent=2)
	# end with
# end def

# Actually downloading
for datatype in ['rainfall', 'air-temperature']:
	try:
		download_dump_data(outdir, datatype)
	except Exception as e:
		logger.error(repr(e))
	# end try
# end for

