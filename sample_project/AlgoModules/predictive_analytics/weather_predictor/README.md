## Weather predictor
Predict the weather the next using a HMM model.

note: this is a toy model for demonstration purpose only.

### Installation
```
$ virtualenv -ppython3 venv
$ venv/bin/pip install --no-cache-dir -r requirements.txt
```


### Documentation
```
usage: data_selection_and_labelling.py [-h] --outdir OUTDIR --logfile LOGFILE
                                       [--nyear NYEAR] --dbpath DBPATH

weather data selection and labelling

optional arguments:
  -h, --help         show this help message and exit
  --outdir OUTDIR    output dump directory
  --logfile LOGFILE  log file path
  --nyear NYEAR      number of year data to use
  --dbpath DBPATH    input database file full path
```

```
usage: HMM_predictor.py [-h] --datadir DATADIR --dbpath DBPATH --logfile
                        LOGFILE --config CONFIG

SG weather HMM predictor

optional arguments:
  -h, --help         show this help message and exit
  --datadir DATADIR  input data directory
  --dbpath DBPATH    output database full path
  --logfile LOGFILE  log file path
  --config CONFIG    configuration file
```
