import sys
import psycopg2
import logging


def pg_get_data(table_name, dbname, host, port, user, pwd,**kwargs):
    '''
    This function gets data from the postgres database
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,user=user, password=pwd)
        logging.info("Connecting to Database")
        cur = conn.cursor()
        cur.execute('''select message_id,
        day_of_week,
        date,
        sender, 
        subject,
        trim(body) as body
        from bbt.challenges 
        where 1 = 
            case when trim(to_char(current_date,'Day')) = 'Monday' and sender like '%Coding%' and body like '%Good morning%'then 1                
                 when trim(to_char(current_date,'Day')) = 'Tuesday' and sender like '%Product%' and body like '%Good morning%'then 1                
                 when trim(to_char(current_date,'Day')) = 'Wednesday' and sender like '%Coding%' and body like '%Good morning%'then 1                
                 when trim(to_char(current_date,'Day')) = 'Thursday' and sender like '%Product%' and body like '%Good morning%'then 1                
                 when trim(to_char(current_date,'Day')) = 'Friday' and sender like '%Science%' and body like '%Good morning%'then 1                
                 when trim(to_char(current_date,'Day')) = 'Saturday' and sender like '%Coding%' and body like '%Good morning%'then 1 
                 when trim(to_char(current_date,'Day')) = 'Sunday' and sender like '%Coding%' and body like '%Good morning%'then 1
                 when trim(to_char(current_date,'Day')) = 'Monday' and sender like '%Coding%' then 1
                 when trim(to_char(current_date,'Day')) = 'Tuesday' and sender like '%Coding%' then 1
                 when trim(to_char(current_date,'Day')) = 'Wednesday' and sender like '%Coding%' then 1
                 when trim(to_char(current_date,'Day')) = 'Thursday' and sender like '%Science%' then 1
                 when trim(to_char(current_date,'Day')) = 'Friday' and sender like '%Product%' then 1
                 
            else 0
            end
        order by date desc limit 1;
    ''')
        logging.info("Loading data from {}".format(table_name))
        query = cur.fetchone()
        post = query[5]
        conn.close()
        return post

    except Exception as e:
        logging.info("Error: {}".format(str(e)))
        sys.exit(1)
