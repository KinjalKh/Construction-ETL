import scripts
import common_lib
import random
import time
import os

class Ingestion:
    def __init__(self, args, config_location):

        # creating logger
        self.log = common_lib.Logger("Ingestion")
        self.log.debug("INGESTION FILE INITIATED")

        # loading default configrations and argument it.
        self.load_arguments(args)
        self.config_location = config_location
        self.load_default_config(config_location)

    
    def ingect(self):

        # TABLES INSTANCES
        self.cites_table = scripts.CitesTable(self.db)
        self.project_table = scripts.ProjectTable(self.db)
        self.employee_table = scripts.EmployeeTable(self.db)
        
        # CREATING TABLES
        self.cites_table.create()
        self.project_table.create()
        self.employee_table.create()

        # INSERTING RECORDS
        self.handle_cities_table()
        self.handle_employee_table()
        
        ## removing old data
        try:
            self.project_table.truncate()
            os.remove(self.config_locat_output_folder + "/project_inserted_data.json")
        except FileNotFoundError:
            pass            # ignoring file not found
        except Exception as e:
            self.log.showError("", e)
            exit()  # terminating the program
        finally:
            self.log.info("OUTPUT FOLDER HAS BEEN CLEARED")

        # creating requried folder
        common_lib.FileHandling.create_folder(self.config_locat_output_folder)
        
        try: # infinite loop
            while True:
                self.handle_project_table()
                self.log.showInfo(f"INSERTING DATA TO PROJECT. LAST RECORD INSERTED ID = {self.project_table.last_id()}.")
                self.log.showInfo(f"SLEEPING FOR {self.argu_sleep_time} SECONDS.")
                time.sleep(int(self.argu_sleep_time))
        except KeyboardInterrupt:
            print("------------------------------------------------")
            self.log.showInfo("PROGRAM TERMINETED BY USER")
            self.log.showInfo(f"{self.project_table.last_id()} RECORDS ARE INSERTED.")
            print("------------------------------------------------")
            return 0

    def load_default_config(self, config_location):
        # loading default configrations and extracting it.
        try:
            default_config = common_lib.FileHandling.read_json(config_location)
            self.config_locat_mysql_config      = default_config["locations"]["mysql_config"]
            self.config_locat_sample_csv        = default_config["locations"]["sample_csv"]
            self.config_locat_output_folder     = default_config["locations"]["output_folder"]
            self.config_locat_city_json_url     = default_config["locations"]["city_json_url"]
            self.config_last_fetched_url          = default_config["program"]["last_fetched_url"]
            self.config_user_name               = default_config["user"]["name"]

        except Exception as e:
            self.log.showError("UNABLE TO SETUP CONFIGRATION", e)

    def load_arguments(self, args):
        try:
            self.argu_env = args.env
            self.argu_start_year = args.start_year
            self.argu_end_year = args.end_year
            self.argu_sleep_time = args.sleep_time
        except Exception as e:
            self.log.showError("FAILD TO LOAD ARGUMENTS",e)
            

    def connect_db(self):
        """
            CREATE CONNECTION WITH MYSQL DATABASE USING CONFIG FILE
        """
        db_config = common_lib.FileHandling.read_json(self.config_locat_mysql_config)[self.argu_env]

        # creating database instance
        self.db = common_lib.MysqlHandling(
            host=db_config['mysql_config']['host'],
            port=db_config['mysql_config']['port'],
            user=db_config['mysql_config']['user'],
            password=db_config['mysql_config']['password'],
            database=db_config['mysql_config']['database'],
            log_enable=db_config['logger_config']['is_ddl_print'],
        )
    
    def close_db(self):
        self.db.close()

    def handle_cities_table(self):
        """
            INSERTING CITIES RECORDS FROM URL
        """
        self.log.debug("handle_cities_table: START")

        ## INSERTING DATA
        # if last url not match to current url it will insert record
        # if table hase no rows it wil start inserting data
        if (self.config_locat_city_json_url != self.config_last_fetched_url) or (self.cites_table.last_id() == None): 
            # removing old data
            self.cites_table.truncate()

            # fetching url
            self.log.showInfo(f"FETCHING JSON TO MEMORY. [URL: {self.config_locat_city_json_url}]")
            try:
                cities_data = common_lib.FileHandling.read_json_url(self.config_locat_city_json_url)
                self.log.showInfo(f"FETCHING JSON SUCCESS.")
            except Exception as e:
                self.log.showError("FETCHING JSON FAIELD")
                exit()
            

            ## converting into list of tuple formate according insertion
            ## cleaning data
            ## name, state_id, state_code, state_name, country_name, created_by
            clean_data = []

            for dict in cities_data:
                try:
                    clean_data.append((dict['name'].encode('latin-1').decode('utf-8'), dict['state_id'], dict['state_code'], dict['state_name'].encode('latin-1').decode('utf-8'), dict['country_name'], self.config_user_name))
                except:
                    continue

            # # # Inserting data to table
            self.cites_table.insert(clean_data)
            self.log.showInfo(f"{self.cites_table.last_id()} ROWS INERTED IN CITES TABLE.")

            # # UPDATING JSON FILE TO LAST USED URL
            json_data = common_lib.FileHandling.read_json(self.config_location)
            json_data['program']['last_fetched_url'] = self.config_locat_city_json_url
            common_lib.FileHandling.store_to_json(json_data, self.config_location)
            self.log.info("URL UPDATED ON CONFIG FILE")

        else:
            self.log.showInfo(f"{self.cites_table.last_id()} ROWS FOUND ON CITY TABLE.")
            self.log.showInfo("PROVIDED URL IS PRIVIOUSLY USED FOR INSERTING DATA.")
            self.log.showInfo("SKIPPING DATA ISNERTING ON CITY.")


    def handle_employee_table(self):
        ## truncating table
        self.employee_table.truncate()
        
        ## reading csv file
        try: 
            csv_data = common_lib.FileHandling.read_csv(self.config_locat_sample_csv)
            self.log.debug("CSV FILE LOADED")
        except Exception as e:
            self.log.showError("UNABLE TO LOAD CSV FILE, E:", e)

        ## updating data
        domains = ["gmail","yahoo","aol","hotmail"]

        values = []
        for data in csv_data:
            # FirstName, LastName, FullName, Email, Salary, created_by
            values.append((data[1], data[2], (data[1] + " " + data[2]),(data[1]+ "_" + data[2] + "@" + random.choice(domains)), random.randint(50000,5000000), self.config_user_name))

        # inserting data
        self.employee_table.insert(values)
        self.log.showInfo(f"{self.employee_table.last_id()} ROWS INSERTED IN EMPLOYEE TABLE.")


    def handle_project_table(self):
        ## variables
        project_name = ["home", "building", "SmallBuilding", "Big Building", "Infra 2", "Infra 1"]

        # random location
        location = self.cites_table.fetch_data(random.randint(0, self.cites_table.last_id()))[0]
        location = location[1] + ", " + location[4] + ", " + location[5] + "-" + str(location[2])


        ## dictionary to store json and also for inserting into table
        data = {"id": ""} # pre defined id
        data["project_name"] = random.choice(project_name)

        # handling start_year, end_year

        if self.argu_end_year - self.argu_start_year == 0:
            self.log.showError(f"START YEAR AND END YEAR NEVER BE SAME. ENTERD YEAR IS {self.argu_start_year}")
            self.log.showInfo(f"PLEASE ENTER VALID INPUTS")
            exit()
        elif self.argu_end_year - self.argu_start_year < 2:
            data["start_year"] = self.argu_start_year
            data["end_year"] = self.argu_end_year
        elif self.argu_end_year - self.argu_start_year < 3:
            data["start_year"] = random.randint(self.argu_start_year, self.argu_end_year-1)
            data["end_year"] = random.randint(data["start_year"]+1, self.argu_end_year)
        else:
            data["start_year"] = random.randint(self.argu_start_year, self.argu_end_year-2)
            data["end_year"] = random.randint(data["start_year"]+1, self.argu_end_year)
            
  
        data["start_date"] = time.strftime("%d-%m-", time.localtime()) + str(data["start_year"])
        data["end_date"] = time.strftime("%d-%m-", time.localtime()) + str(data["end_year"])
        data["duration"] = data["end_year"] - data["start_year"]
        
        data["budget"] = random.randint(1250000,1550000)
        data["location"] = location

        # ## inserting data to table
        self.project_table.insert(
            project_name=data["project_name"],
            location=data['location'],
            budget=data["budget"],
            start_date=data["start_date"],
            end_date=data["end_date"],
            duration=data["duration"],
            user = self.config_user_name
        )

        ## finding inserted id
        data['id'] = self.project_table.last_id()

        ## appending to json
        common_lib.FileHandling.append_json([data], (self.config_locat_output_folder + "/project_inserted_data.json"))



