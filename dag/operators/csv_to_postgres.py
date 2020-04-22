import sys
import psycopg2
import logging

def pg_load_table(file_path, table_name, dbname, host, port, user, pwd):
    '''
    This function upload csv to a target table
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        logging.info("Connecting to Database")
        cur = conn.cursor()
        f = open(file_path, "r")
        # Truncate the table first
        cur.execute("Truncate {} Cascade;".format(table_name))
        logging.info("Truncated {}".format(table_name))
        # Load table from the file with header
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
        cur.execute("commit;")
        logging.info("Loaded data into {}".format(table_name))
        conn.close()
        logging.info("DB connection closed.")

    except Exception as e:
        logging.info("Error: {}".format(str(e)))
        sys.exit(1)

