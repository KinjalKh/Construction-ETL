import mysql.connector
import common_lib

class MysqlHandling:
    def __init__(self, host, user, password, database, port = 3306,log_enable = True):

        self.log_enable = log_enable # get log status

        self.db_log = common_lib.Logger("DATABASE", log_enable=log_enable)

        self.db_log.info(f"CREATING CONNECTION FOR {user}@{host}.")
        self.db_log.debug("INIT: DATABASE CONNECTION.")

        try:
            # Creating Connection
            self.connection = mysql.connector.connect(
                host = host,
                user = user,
                password = password,
                port = port
            )
            self.db_log.info(f"CONNECTION ESTABLISHED FOR {user}@{host}")
            # Creating Cursor
            self.cursor = self.connection.cursor()
            self.db_log.showInfo("DATABASE CONNECTION ESTABLISHED")

        except Exception as e:
            self.db_log.showError('CONNECTION FAILUER', e)
            exit()

        self.__set_database(database)

    def __set_database(self, database):
        """
        - use provided database and create if not exists.
        """
        try:
            self.commit_query(f"CREATE DATABASE IF NOT EXISTS {database}")
            self.commit_query(f"use {database}")
        except Exception as e:
            self.db_log.showError('UNABLE TO CREATE/USE DATABASE', e)
            exit()

    def close(self):
        """
        Closing cursor and connection
        """
        self.db_log.debug("START: CLOSE CONNECTION")
        self.cursor.close()
        self.db_log.info("CURSOR CLOSE SUCCESS")
        self.connection.close()
        self.db_log.showInfo("DATABASE CONNECTION CLOSED")
        self.db_log.debug("END: CLOSE CONNECTION")


    
    def is_table_exist(self, name):
        """Find weather table is exists or not """

        self.cursor.execute(f'SHOW TABLES LIKE "{name}"')
        result = self.cursor.fetchone()

        if result : 
            print("table is exists")
        else:
            print("table not exists")


    def commit_query(self, query):
        """Create table using query"""

        self.db_log.info(F"RECIVED QUERRY: {query} ")

        try:
            self.db_log.debug('EXECUTING RECIVED QUERRY')
            self.cursor.execute(query)
            self.db_log.info('EXECUTION SUCCESS')
            self.db_log.debug('INITITATE COMMIT')
            self.connection.commit()
            self.db_log.info(f"SUCCESS COMMIT: {query}")

        except Exception as e:
            self.db_log.critical('FAILED TO EXECUTE QUERRY')
            self.db_log.error(e)
            raise Exception(e)
        

    def drop_table(self, name):
        """Droping table"""
        self.db_log.info(f"START: DROPPING TABLE {name}")
        query = f'DROP TABLE {name}'
        self.commit_query(query)
        self.db_log.info(f"END: DROPPING TABLE {name}")



    def fetch_query(self, query):
        """Fetching the Query"""
        self.db_log.debug(f"RECIVED: FETCH QUERY {query}")
        try:
            self.cursor.execute(query)
            self.db_log.info('FETCHING SUCCESS')
            return self.cursor.fetchall()
        
        except Exception as e: 
            self.db_log.critical('FETCHING UNSUCCESS')
            self.db_log.error(e)

    def fetch_one(self, query):
        """Fetching the Query"""
        self.db_log.debug(f"RECIVED: FETCH QUERY {query}")
        try:
            self.cursor.execute(query)
            self.db_log.info('FETCHING SUCCESS')
            return self.cursor.fetchone()[0]
        
        except Exception as e: 
            self.db_log.critical('FETCHING UNSUCCESS')
            self.db_log.error(e)
    
    def insert_many(self, query, values):
        """
            insert_statement = INSERT INTO TABLE_NAME (COLUMN1, COLUMNT2) VALUES (%s, %s)
            values = [(value, valu2), (value3, value4),...] 
        """
        try:
            self.db_log.debug("INSERT MANY: START")
            self.cursor.executemany(query, values)
            self.connection.commit()
            self.db_log.info("INSERT MANY: INSERTION SUCCESS")
            self.db_log.debug("INSERT MANY: END")
        except Exception as e:
            self.db_log.critical("INSERT MANY: INSERTION FAIELD")
            self.db_log.error(e)
            self.db_log.debug("INSERT MANY: END")