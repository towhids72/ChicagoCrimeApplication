# Chicago Crimes  Application

This application shows a map of Chicago crimes, which is filterable based on
crime type and also crimes date.

### How to run
### You can run application in development or in production

* _**Run in production using docker**_
  * Copy your Google credential file to the project root directory, we use this file to connect to Google BigQuery
  * Create a ".env" file in the project root directory, docker containers will look for this 
    file to load environment variables
  * Copy the deployment environment variables from ".env.docker.sample" file and replace necessary values
  * Make sure that you replaced you Google credential file name in ".env" file
  * Run below command, it will create a docker network that containers will use
    * `docker network create chicago_network`
  * Build and run all docker containers using below command
    * `docker-compose up -d --build`
  * Open http://0.0.0.0:8050 on your browser, you will see streamlit dashboard


* **_Run in development(locally)_**
    * Create a virtual environment with python 3.10, you can use conda:
      * `conda create --name=your_env_name python=3.10`
      * `conda activate your_env_name`
    * Copy your Google credential file to the project root directory, we use this file to connect to Google BigQuery
    * Create a ".env" file in the project root directory, we use this file to load environment variables
    * Copy the development environment variables from ".env.local.sample" file and replace necessary values
    * Make sure that you replaced you Google credential files name in ".env" file
    * Now install project requirements
      * `pip install -r requirements.txt`
    * Before running app, you can test Flask app and celery tasks by running
      * `pytest`
    * Start celery beat and celery worker
      * `celery -A celery_app.tasks beat --loglevel=INFO `
      * `celery -A celery_app.tasks worker -Q crimes --loglevel=INFO`
    * Start Flask app
      * `gunicorn --bind 0.0.0.0:8000 --workers=2 --threads=2 api.app:application`
    * Start Streamlit dashboard
      * `python -m streamlit run streamlit_dashboard.py --server.address 0.0.0.0 --server.port 8050`
    * Now open http://0.0.0.0:8050 on your browser, you will see streamlit dashboard


###Warnings
First time it may take a bit longer to load the map, it tries to cache the data, after that it will load faster


## Future Improvements
There are some improvements that we can implement in code, first somehow we should get notified when there is a critical
error(e.g. error while loading the streamlit dashboard). We can use tools like Sentry, or use alerting modules to
send the errors to a Telegram channel, Slack, etc.

In the current scenario, when we run the celery tasks, it will fetch and catch crimes data every 24 hours. Every API
call to get the crime data first tries to get the data from cache, if the cache was empty, it would query data from
Google BigQuery dataset(also saves data in cache).

_**Other possible scenarios would be:**_
* Instead of caching all crimes data at first, we could cache each crime data that is requested by API and set an 
  expiry key for it.
* Use Streamlit cache system. In this scenario API calls will decrease. In this case we should be careful of what to
  cache, when to cache and how long keep the cache(it depends on data and how it is updating).

Another improvement would be using an authentication system for API calls, or restrict requests that are not from other
internal services(e.g. just response to the requests that are sent by streamlit service).

As we are working with kind of BigData, and loading all data to RAM would be a disaster, we can filter data based on
some parameters, for example right now data is limited to just one type of crime and also limited to 2000 datapoints.

**_Other possible filters_**
* Get all crimes data with a date limit(e.g. limits of 3 months, and iterate over the crimes data with this limit)
* Get crimes data based on districts.
