import json
import requests
import csv
import os
import common_lib

class FileHandling:
    def __init__(self):
        self.file_handling_log = common_lib.Logger('FILE HANDLING')
        pass

    def file_exist(self, file):
        self.file_handling_log.debug(f"START: FILE CHECKUP")
        self.file_handling_log.debug(f"RECIVED: {file}")

        if os.path.isfile(file):
            self.file_handling_log.info(f"FILE FOUND {file}")
            self.file_handling_log.debug(f"END: FILE CHECKUP")
            return True
        else:
            self.file_handling_log.warning(f"FILE NOT FOUND: {file}")
            self.file_handling_log.debug(f"END: FILE CHECKUP")
            return False
        
    def read_json(self, file):
        if self.file_exist(file):
            try:
                with open(file, 'r') as json_file:
                    data = json.load(json_file)
                return data
            except Exception as e:
                raise Exception(f"Failed to load data")
        else:
            raise FileNotFoundError(f"Given File {file} is not found")

    def read_json_url(self, url):
        """read json file using url and return into dictionary"""
        response = requests.get(url)
        return response.json()
    
    def store_to_json(self, data, location):
        """STORING DICT TO JSON FILE"""
        with open(location, 'w') as json_file:
            json.dump(data, json_file, indent=4)

    def append_json(self, data, file):
        """appending json"""
        try:
            with open(file, 'r') as json_file:
                ed = json.load(json_file)
            ed.extend(data)
            self.store_to_json(ed, file)
        except:
            self.store_to_json(data, file)

    def read_csv(self, file):
        with open(file, newline='') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            rows = list(reader)
            
        return rows
    
    def write_csv(self, file_path, data, header=None):
        try:
            self.file_handling_log.debug("START: WRITING CSV FILE")

            with open(file_path, mode='w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                
                if header:
                    self.file_handling_log.debug(f"recived header {header}")
                    writer.writerow(header)
                
                writer.writerows(data)
            
            self.file_handling_log.debug("END: WRITING CSV FILE")
        except Exception as e:
            self.file_handling_log.showError("FACING ISSUE TO WRITE CSV FILE", e)
            exit()
        
    def list_csv_files(self, directory):
        csv_files = []

        # Iterate over all files in the directory
        for filename in os.listdir(directory):
            # Check if the file ends with ".csv" extension
            if filename.endswith(".csv"):
                csv_files.append(filename)

        return csv_files
    
    def create_folder(self, name):
        self.file_handling_log.debug("CREATE FOLDER: START")
        self.file_handling_log.debug(f"RECIVED {name}")

        try:
            os.makedirs(name)
            self.file_handling_log.info(f"{name} DIRECTRORY CREATED")
        except Exception as e:
            self.file_handling_log.warning("FACING ISSUE TO CREATE DIR.")
            self.file_handling_log.error(e)
        finally:
            self.file_handling_log.debug("CREATE FOLDER: END")

    def remove_folder(self, name):
        self.file_handling_log.debug("REMOVE FOLDER: START")
        self.file_handling_log.debug(f"RECIVED {name}")

        try:
            os.removedirs(name)
            self.file_handling_log.info(f"{name} DIRECTRORY REMOVED")
        except Exception as e:
            self.file_handling_log.warning("FACING ISSUE TO REMOVE DIR.")
            self.file_handling_log.error(e)
        finally:
            self.file_handling_log.debug("REMOVE FOLDER: END")
