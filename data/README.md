## Database directory
note: this is a placeholder for data. In production these should be replaced by databases or data warehouse.

### File Tree
```
/
├── dump/           : production equivalent = data warehouse (e.g. Hadoop)
├── sg_weather.db   : SQL database (sqlite3) for SG weather
└── subscribers.db  : SQL database (sqlite3) for subscriber
```

#### sample data from "sg_weather.db"
```
Station,Datetime,Daily Rainfall Total (mm),Highest 30 Min Rainfall (mm),Highest 60 Min Rainfall (mm),Highest 120 Min Rainfall (mm),Mean Temperature (C),Maximum Temperature (C),Minimum Temperature (C),Mean Wind Speed (km/h),Max Wind Speed (km/h)
Boon Lay (West),1999-02-01,0.0,,,,,,,,
Boon Lay (West),1999-02-02,9.9,,,,,,,,
Boon Lay (West),1999-02-03,17.5,,,,,,,,
Boon Lay (West),1999-02-04,0.5,,,,,,,,
Boon Lay (West),1999-02-05,0.0,,,,,,,,
```

#### sample data from "subscribers.db"
```
username,email,subscribed,date_updated
kuanpern,kptan86@gmail.com,"[""weather_prediction"", ""S&P500_prediction""]",2018-01-01
admin,tankp@bii.a-star.edu.sg,"[""weather_prediction"", ""NPark_recommendation""]",2018-01-01
```
