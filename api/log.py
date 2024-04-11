import logging
import os
from datetime import datetime, timedelta

def setup_logger():
    current_dir = os.path.dirname(__file__)
    log_file_path = os.path.join(current_dir, "app.log")

    # Create a logger and set the logging level to INFO
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create a file handler to write log messages to the specified log file
    file_handler = logging.FileHandler(log_file_path)

    # Define the log format, including a timestamp
    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(log_format)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    # Clear old log files
    # clear_old_logs(current_dir)

    return logger

def clear_old_logs(directory, days_old=5):
    current_time = datetime.now()
    for filename in os.listdir(directory):
        if filename.endswith(".log"):
            file_path = os.path.join(directory, filename)
            # Get the creation time of the log file
            creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
            # Calculate the age of the log file
            age = current_time - creation_time
            # If the log file is older than the specified number of days, delete it
            if age.days >= days_old:
                os.remove(file_path)

if __name__ == "__main__":
    setup_logger()
