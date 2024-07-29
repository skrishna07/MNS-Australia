import os
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from dotenv import load_dotenv,find_dotenv
load_dotenv(find_dotenv())


def setup_logging():
    log_directory = os.environ.get('logging_path')
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_filename = os.path.join(log_directory, datetime.datetime.now().strftime("%d-%m-%Y.log"))

    # Configure logging to both console and file
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.StreamHandler(),  # Log to console
                            TimedRotatingFileHandler(log_filename, when="midnight", backupCount=7)  # Log to file
                        ])

    # Add this line to get the root logger and set its level
    logging.getLogger().setLevel(logging.INFO)
