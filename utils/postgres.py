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
            PUBLICONDEMANDRATE VARCHAR
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
        PUBLICONDEMANDRATE
    FROM CLIENT_USAGE_DATA_RAW
	WHERE OPERATION LIKE 'RUNINSTANCES%'
    GROUP BY OPERATION,	USAGETYPE, TIMEINTERVAL, PUBLICONDEMANDRATE
	
	
	UNION ALL
	
	SELECT
        OPERATION,
        USAGETYPE,
        TIMEINTERVAL,
        SPLIT_PART(TIMEINTERVAL,'/',1) AS START_TIME,
        SPLIT_PART(TIMEINTERVAL,'/',2) AS END_TIME,
        SUM(USAGEAMOUNT::NUMERIC) SUM_USAGEAMOUNT,
        PUBLICONDEMANDRATE
    FROM CLIENT_USAGE_DATA_RAW
	WHERE OPERATION NOT LIKE 'RUNINSTANCES%'
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
    