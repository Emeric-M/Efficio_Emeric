# Efficio_Emeric
Cloud Analytics Case Study

## Install and run
All dependencies are listed (thanks to pip freeze) in requirements.txt
In order to have all dependencies, you can create your own local venv and run 
```
pip install -r requirements.txt
```

Create and fill file in *utils/secrets.py*, with connection to your local postgres instance
```python
POSTGRES_USER = ''
POSTGRES_PWD = ''
```
Then you can simply run main.py : 
```
python3 .\main_app\main.py
```

## Problems
### Problem 1 - Commit cost impact
Running the app will prompt input needed : 
 - start_date
 - end_date
 - commit amount

 Typically 
  - commit to $1/hour, using 1/1/2023 – 1/14/2023 = **$177.61** savings ($13840.81 total)
  - commit to $8/hour, using 1/1/2023 – 1/14/2023 = **$1344.58** savings ($12673.84 total)

### Problem 2 - Deploy at scale
Here are a few ideas to deploy this UseCase at scale
 - Filegateway to upload files to a S3 bucket
 - Snowpipe to upload to Snowflake
 - SecretManager to handle password
 - API exposure, so users can access to results through a web app, or an API client instead of command line

### Problem 3 - Find optimal
 - re-use program from #1
 - increment and iterate on commit_amount, until saving is negative. Save result of each iteration (in a list for example)
 - retrieve commit_ammount where saving is the highest 

## Implementation choice
	- After inserting data in Postgres, can perform multiple debug anlysis, without reading whole file
	- Use DB query engine (not Python compute)
    - In client_data, for the same operation (RUNINSTANCES and similar), usagetype and timeinterval, several rows can be found. I decided to sum ceil(round up) of usageamount based on these 3 fields as PK, as it's charged by hour. For other operations it's mostly charged by GB so no ceiling done
    - Some references from customer_data can't be found in saving plan, using publicondemanderate in this case (no discount applied)
    - When publicondemanderate is empty, considered as 0 (free)

## Improvments
    - Write unit and functionnal tests, to perform frequently
    - Write sql queries in sql files (better readability) and import it in python script
    - For this UseCase, I always re-create and load data. In a prod context, there is no need for a full reload at each query
	
