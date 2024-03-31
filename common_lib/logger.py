import logging
import os

# os.environ['TZ'] = 'UTC'

class Logger: 
    def __init__(self, logger_name = "root", log_enable = True):
        self.log_enable = log_enable

        logging.basicConfig(
            filename="logs.log",
            filemode="w",
            level=logging.DEBUG,
            format="%(asctime)s - %(name)-15s - %(levelname)-8s  - %(message)s",
            datefmt="%d-%m-%Y %I:%M:%S %p",
            # datefmt="%d-%m-%Y %I:%M:%S %p %Z", # UTC TIMEZONE
            force=True
        )

        self.logger = logging.getLogger(logger_name)

    def debug(self, msg):
        if self.log_enable:
            self.logger.debug(msg)

    def info(self, msg):
        if self.log_enable:
            self.logger.info(msg)

    def warning(self, msg):
        if self.log_enable:
            self.logger.warning(msg)

    def error(self, msg):
        if self.log_enable:
            self.logger.error(msg)

    def critical(self, msg):
        if self.log_enable:
            self.logger.critical(msg)
    
    def showError(self, msg, exception_msg = None):
        if self.log_enable:
            print("ERROR: ", msg)
            self.critical(msg)
            
            if exception_msg != None:
                self.error(exception_msg)
    
    def showWarning(self, msg, exception_msg = None):
        if self.log_enable:
            print("WARNNING: ", msg)
            self.warning(msg)

            if exception_msg != None:
                self.error(exception_msg)
        
    def showInfo(self, msg):
        if self.log_enable:
            print(f"INFO: {msg}")
            self.info(msg)
