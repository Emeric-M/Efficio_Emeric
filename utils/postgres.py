import psycopg2
from .secrets import POSTGRES_USER,POSTGRES_PWD

def create_conn_postgres():
    conn = psycopg2.connect(
        host="localhost",
        database="Efficio_Emeric",
        user=POSTGRES_USER,
        password=POSTGRES_PWD)
    
    return conn,conn.cursor()

def create_tables():
    commands = (
        """
        DROP TABLE IF EXISTS SAVING_PLAN_RATES
        """,
        """
        CREATE TABLE SAVING_PLAN_RATES(
            SKU VARCHAR,
            CONTRACTLENGTH VARCHAR,
            PURCHASEOPTION VARCHAR,
            PLANTYPE VARCHAR,
            DISCOUNTEDSKU VARCHAR,
            USAGETYPE VARCHAR,
            OPERATION VARCHAR,
            SERVICECODE VARCHAR,
            RATECODE VARCHAR,
            UNIT VARCHAR,
            CURRENCY VARCHAR,
            RATE_TYPE VARCHAR,
            RATE VARCHAR,
            OD_RATE VARCHAR,
            UNIT_SAVINGS VARCHAR,
            PERC_SAVINGS VARCHAR
        )
        """,
        """
        DROP TABLE IF EXISTS CLIENT_USAGE_DATA_RAW
        """,
        """
        CREATE TABLE CLIENT_USAGE_DATA_RAW(
            LINEITEMDESCRIPTION VARCHAR,
            OPERATION VARCHAR,
            USAGETYPE VARCHAR,
            TIMEINTERVAL VARCHAR,
            PRODUCTCODE VARCHAR,
            USAGEAMOUNT VARCHAR,
            PUBLICONDEMANDRATE VARCHAR
        )
        """,
        """
        DROP TABLE IF EXISTS CLIENT_USAGE_DATA_CLEANED
        """,
        """
        CREATE TABLE CLIENT_USAGE_DATA_CLEANED(
            OPERATION VARCHAR,
            USAGETYPE VARCHAR,
            TIMEINTERVAL VARCHAR,
            START_TIME VARCHAR,
            END_TIME VARCHAR,
            SUM_USAGEAMOUNT NUMERIC,
            PUBLICONDEMANDRATE NUMERIC
        )
        """
    )

    conn = None
    try:
        conn,cur = create_conn_postgres()

        for command in commands:
            cur.execute(command)
        
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def clean_insert(table_name,file_name,separator):
    conn = None
    try:
        conn,cur = create_conn_postgres()

        cur.execute("TRUNCATE TABLE " + table_name)
        
        with open(file_name,'r') as file:
            next(file)  #skip header
            cur.copy_from(file,table_name,sep=separator)
            
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def client_raw_to_cleaned():
    command = """
    INSERT INTO CLIENT_USAGE_DATA_CLEANED
    SELECT
        OPERATION,
        USAGETYPE,
        TIMEINTERVAL,
        SPLIT_PART(TIMEINTERVAL,'/',1) AS START_TIME,
        SPLIT_PART(TIMEINTERVAL,'/',2) AS END_TIME,
        SUM(CEIL(USAGEAMOUNT::NUMERIC)) SUM_USAGEAMOUNT,
        CASE 
            WHEN PUBLICONDEMANDRATE = '' THEN 0
            ELSE PUBLICONDEMANDRATE::NUMERIC
        END PUBLICONDEMANDRATE
    FROM CLIENT_USAGE_DATA_RAW
	WHERE UPPER(OPERATION) LIKE 'RUNINSTANCES%'
    GROUP BY OPERATION,	USAGETYPE, TIMEINTERVAL, PUBLICONDEMANDRATE
	
	
	UNION ALL
	
	SELECT
        OPERATION,
        USAGETYPE,
        TIMEINTERVAL,
        SPLIT_PART(TIMEINTERVAL,'/',1) AS START_TIME,
        SPLIT_PART(TIMEINTERVAL,'/',2) AS END_TIME,
        SUM(USAGEAMOUNT::NUMERIC) SUM_USAGEAMOUNT,
        CASE 
            WHEN PUBLICONDEMANDRATE = '' THEN 0
            ELSE PUBLICONDEMANDRATE::NUMERIC
        END PUBLICONDEMANDRATE
    FROM CLIENT_USAGE_DATA_RAW
	WHERE UPPER(OPERATION) NOT LIKE 'RUNINSTANCES%'
    GROUP BY OPERATION,	USAGETYPE, TIMEINTERVAL, PUBLICONDEMANDRATE
    """

    conn = None
    try:
        conn,cur = create_conn_postgres()

        cur.execute(command)
    
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def load_data():
    clean_insert(
        table_name = 'saving_plan_rates',
        file_name = 'input_data/savingsplanrates.csv',
        separator = ',')
    
    clean_insert(
        table_name = 'client_usage_data_raw',
        file_name = 'input_data/currfile.csv',
        separator = '|')
    
    client_raw_to_cleaned()

def get_sum_extra_due(commit_hourly_amount,start_date,end_date):
    command_no_var = """
    with client_saving_ranked as (
    select 	
        client.operation,
        client.usagetype,
        client.timeinterval,
        client.start_time,
        client.end_time,
        client.sum_usageamount,
        client.publicondemandrate as od_rate,
        saving.rate as dis_rate,
        saving.perc_savings,
        row_number() over(
            partition by client.timeinterval
            order by saving.perc_savings desc, client.usagetype 
            --make sure output is always the same if 2 equals perc_saving on same timeinterval
        ) as row_number, --not mandatory, kept for readability
        {0} - sum(sum_usageamount::numeric * saving.rate::numeric)  over(
            partition by client.timeinterval
            order by saving.perc_savings desc, client.usagetype 
            --make sure output is always the same if 2 equals perc_saving on same timeinterval
        ) as saving_balance		 
    from client_usage_data_cleaned client
    left join saving_plan_rates saving
        on client.operation = saving.operation
        and client.usagetype = saving.usagetype
    where 
        client.start_time >= '{1}'
        and client.end_time < '{2}'
    ),
    client_with_prev_saving as (
    select
        *,
        lag(saving_balance,1) over(
            partition by timeinterval
            order by perc_savings desc, usagetype 
            --make sure output is always the same if 2 equals perc_saving on same timeinterval
        ) as prev_saving_balance	
    from client_saving_ranked
    ),
    client_saving_total as (
    select
        *,
        case
            when perc_savings is null then sum_usageamount * od_rate::numeric
            when saving_balance > 0 then 0 
            when (prev_saving_balance > 0 and saving_balance <= 0)
                then (dis_rate::numeric * sum_usageamount - prev_saving_balance) / (1 - perc_savings::numeric)
            else sum_usageamount * od_rate::numeric
        end as extra_due,
        sum_usageamount * od_rate::numeric as no_commit_price
    from client_with_prev_saving
    )
    select 
        round(sum(extra_due),2) as sum_extra_due,
	    round(sum(no_commit_price),2) as sum_no_commit_price
    from client_saving_total
    """

    command = command_no_var.format(commit_hourly_amount,start_date,end_date)

    conn = None
    try:
        conn,cur = create_conn_postgres()

        cur.execute(command)
        result = cur.fetchone()
    
        cur.close()
        conn.commit()

        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
