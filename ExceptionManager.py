import logging
from PythonLogging import setup_logging
from SendEmail import send_email
from DatabaseQueries import get_retry_count
from DatabaseQueries import update_retry_count
from DatabaseQueries import update_locked_by_empty
from DatabaseQueries import update_process_status
import os


def exception_handler(e, registration_no, config_dict, receipt_no, company_name, database_id, db_config):
    setup_logging()
    system_name = os.environ.get('SystemName')
    logging.error(f"Exception occurred {e}")
    retry_count = get_retry_count(db_config, registration_no, database_id)
    if retry_count is not None:
        if retry_count == '':
            retry_count = 0
    else:
        retry_count = 0
    try:
        retry_count = int(retry_count)
        retry_count += 1
    except Exception as error:
        logging.error(f"Exception while fetching retry count {error}")
    update_retry_count(db_config, registration_no, retry_count, database_id)
    update_locked_by_empty(db_config, database_id)
    if retry_count > 4:
        update_process_status(db_config, database_id, 'Exception')

    exception_subject = str(config_dict['Exception_subject']).format(registration_no, receipt_no)
    exception_message = str(config_dict['Exception_message']).format(registration_no, receipt_no, company_name, e, system_name)
    exception_mails = str(config_dict['Exception_mails']).split(',')
    send_email(config_dict, exception_subject, exception_message, exception_mails, None)


def exception_handler_main(e, config_dict):
    system_name = os.environ.get('SystemName')
    exception_subject = 'Australia Master Process failed'
    exception_message = """<!DOCTYPE html>
                            <html>
                            <head>
                              <title></title>
                            </head>
                            <body>
                              <p>Hello Team,</p>
                            
                              <p>This is to notify you that our BOT Failed at main run level</p>
                            
                            
                              <p>Exception Message: {}</p>
                            
                              <p>Thanks & Regards,</p>
                              <p>{}</p>
                            </body>
                            </html>""".format(e, system_name)
    exception_mails = ['ayush.bhattad@bradsol.com', 'srikrishna.mekala@bradsol.com']
    send_email(config_dict, exception_subject, exception_message, exception_mails, None)
