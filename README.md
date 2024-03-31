# construction ETL

## Description

* Create pytho program to perform etl process.
* Create three tables named cites, project and employee.
* fetch data from url and insert to cites tables
* read csv file and insert data to employee table.
* get 4 parameters from user env, start_year, end_year and sleep_time.
* set enviorment according to choosen env.
* use start_year and end_year in project table for starting data, ending date and duration
* insert project data according to sleep time and also store in json file.

## How to run

* install requirements library
* `python .\main.py -env QA -start_year 2020 -end_year 2030 -sleep_time 4`

## switches

* -env: Choose enviorment QA or PROD
* -start_year: Provide start year
* -end_year: Provide End Year
* -sleep_time: Provide duration in seconds to sleep program

## How to Modify

* open config/config.json file to change configrations.
* open config/mysql_config.json to change enviorment.
