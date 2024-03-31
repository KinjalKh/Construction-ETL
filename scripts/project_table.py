import common_lib

class ProjectTable:
    def __init__(self, database):
        self.log = common_lib.Logger("PROJECT TABLE")
        self.db = database
        self.table_name = "project"

    def create(self):
        try:
            self.log.debug("START: TABLE CREATION")
            query = "CREATE TABLE IF NOT EXISTS project ( project_id INT AUTO_INCREMENT PRIMARY KEY, project_name VARCHAR(255), location VARCHAR(255), budget VARCHAR(50), StartDate DATE, EndDate DATE, project_duration INT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, created_by VARCHAR(255))"
            self.db.commit_query(query)
            self.log.debug("END: TABLE CREATION")

        except Exception as e:
            self.log.showError("UNABLE TO CREATE TABLE", e)
            exit()


    def insert(self, project_name,location, budget,start_date,end_date,duration,user  ):
        try:
            self.log.debug("START: INSERTING DATA")
            query = f'INSERT INTO project (project_name, location, budget, StartDate, EndDate, project_duration, created_by) VALUES ("{project_name}","{location}", "{budget}", STR_TO_DATE("{start_date}", "%d-%m-%Y"), STR_TO_DATE("{end_date}", "%d-%m-%Y"), {duration}, "{user}")'
            self.db.commit_query(query)
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
            result = self.db.fetch_one(f"SELECT max(project_id) from {self.table_name}")
            self.log.debug("END: FIND LAST ID")
            return result
        except Exception as e:
            self.log.showError("UNABLE TO FIND  LAST ID", e)
            exit()