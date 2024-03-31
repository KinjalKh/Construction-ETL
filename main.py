import argparse
import common_lib
import scripts

class Main:
    def __init__(self):

        # setup location
        config_file_location = "config/config.json"

        # setup log
        self.log = common_lib.Logger("MAIN")

        # get argument parser
        args = self.get_args()

        if args.env == "qa":
            args.env = "QA"
        elif args.env == "prod":
            args.env == "PROD"
            
        self.validate_arguments(args) # validation

        # ingestion
        try:
            self.ingestion = scripts.Ingestion(args, config_file_location)
        except:
            self.log.showError("INTERNAL ERROR", e)

        try:
            # connect database
            self.ingestion.connect_db()

            # performig etl
            self.ingestion.ingect()

            # closing databaase connection
            self.ingestion.close_db()
        except Exception as e:  
            self.log.showError("INTERNAL ERROR", e)
            exit()


    def get_args(self):
        """
            Setup Argument Parser
        """
        parser = argparse.ArgumentParser()
        parser.add_argument('-env', choices=["QA", "PROD", "qa", "prod"], default="QA",help="Give any from QA or PROD")
        parser.add_argument('-start_year',type=int, default=2000, help="Select Start Year for Random year selection.")
        parser.add_argument('-end_year', type=int, default=3000, help="Select Start Year for Random year selection.")
        parser.add_argument('-sleep_time', type=int,default=2, help="Sleept time(seconds) between record insertation i")
        return parser.parse_args()
    
    def validate_arguments(self, args):
        """Validating argument parser"""
        self.log.info("START: VALIDATION OF USER INPUT")

        # fetching arguments into variables
        try:
            start_year = args.start_year
            end_year = args.end_year
            sleep_time = args.sleep_time
            self.log.debug("SUCCESS: FETCHING ARGUMENTS VALUES")

        except Exception as e:
            self.log.showError("ARGUMENTS ARE NOT VALID", e)
            exit()

        # LOGGING RECIVED VALUES
        self.log.debug(f"PROVIDED VALUES ARE: START_YEAR = {start_year}, END_YEAR = {end_year}, SLEEP_TIME = {sleep_time}")

        # VALIDATING YEAR
        if end_year < start_year:
            self.log.showError(f"End Year[{end_year}] is bigger then Start Year[{start_year}]")
            self.log.showInfo("PLEASE ENTER VALID YEAR.")
            exit()
        elif end_year - start_year == 0:
            self.log.showError(f"given years {start_year} are Same.")
            self.log.showInfo("PLEASE ENTER VALID YEAR.")
            exit()
            
        self.log.info("END: VALIDATION OF USER INPUT")



if __name__ == "__main__":
    Main()