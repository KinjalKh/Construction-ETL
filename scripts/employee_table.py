import common_lib

class EmployeeTable:
    def __init__(self, database):
        self.log = common_lib.Logger("EMPLOYEE TABLE")
        self.db = database
        self.table_name = "employee"

    def create(self):
        try:
            self.log.debug("START: TABLE CREATION")
            query = "CREATE TABLE IF NOT EXISTS employee ( id INT AUTO_INCREMENT PRIMARY KEY, FirstName VARCHAR(255), LastName VARCHAR(255), FullName VARCHAR(510), Email VARCHAR(255), Salary DECIMAL(10, 2), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, created_by VARCHAR(255))"
            self.db.commit_query(query)
            self.log.debug("END: TABLE CREATION")

        except Exception as e:
            self.log.showError("UNABLE TO CREATE TABLE", e)
            exit()


    def insert(self, values):
        try:
            self.log.debug("START: INSERTING DATA")
            query = "INSERT INTO employee (FirstName, LastName, FullName, Email, Salary, created_by) VALUES ( %s, %s , %s, %s, %s, %s)"
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