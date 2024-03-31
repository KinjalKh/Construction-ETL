import common_lib

class CitesTable:
    def __init__(self, database):
        self.log = common_lib.Logger("CITIES TABLE")
        self.db = database
        self.table_name = "cities"

    def create(self):
        try:
            self.log.debug("START: TABLE CREATION")
            query = "CREATE TABLE IF NOT EXISTS cities (id INT AUTO_INCREMENT PRIMARY KEY,name VARCHAR(255),state_id INT,state_code VARCHAR(10),state_name VARCHAR(255),country_name VARCHAR(255),created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,created_by VARCHAR(255))"
            self.db.commit_query(query)
            self.log.debug("END: TABLE CREATION")

        except Exception as e:
            self.log.showError("UNABLE TO CREATE TABLE", e)
            exit()


    def insert(self, values):
        try:
            self.log.debug("START: INSERTING DATA")
            query = "INSERT INTO cities (name, state_id, state_code, state_name, country_name, created_by) VALUES (%s, %s, %s, %s, %s, %s)"
            self.db.insert_many(query, values)
            self.log.debug("END: INSERTING DATA")

        except Exception as e:
            self.log.showError("UNABLE TO INSERT DATA", e)
            exit()


    def truncate(self):
        """
            TRUNCATING TABLE: DELETING ALL DATA FROM THE TABLE
        """
        try:
            self.log.debug("START: TRUNCATE")
            self.db.commit_query(f'TRUNCATE TABLE {self.table_name}')
            self.log.showInfo(f"{self.table_name} is TRUNCATED")
            self.log.debug("END: TRUNCATE")
        except Exception as e:
            self.log.warning("UNABLE TO TRUNCAT TABLE")
            self.log.debug("END: TRUNCATE")

    def last_id(self):
        """
            find last id
        """
        try:
            self.log.debug("START: FIND LAST ID")
            result = self.db.fetch_one(f"SELECT max(id) from {self.table_name}")
            self.log.debug("END: FIND LAST ID")
            return result
        except Exception as e:
            self.log.showError("UNABLE TO FIND  LAST ID", e)
            exit()
    
    def fetch_data(self, id):
        """
            Fetching Location from id
        """
        try:
            self.log.debug("START: FETCHING DATA")
            result = self.db.fetch_query(f"SELECT * from {self.table_name} WHERE id = {id}")
            self.log.debug("END: FETCHING DATA")
            return result
        except Exception as e:
            self.log.showError("UNABLE TO FETCHING DATA", e)
            exit()