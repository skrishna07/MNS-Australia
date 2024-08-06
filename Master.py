from PythonLogging import setup_logging
from ReadExcelConfig import create_main_config_dictionary
import logging
from DatabaseQueries import get_db_credentials
from DatabaseQueries import fetch_orders_to_extract_data
from DatabaseQueries import update_locked_by
import os
from MasterFunctions import data_extraction_and_insertion
from MasterFunctions import json_loader_and_tables
from DatabaseQueries import update_locked_by_empty
from ExceptionManager import exception_handler
from DatabaseQueries import update_workflow_status
from DatabaseQueries import update_process_status
from DatabaseQueries import update_bot_comments_empty
from DatabaseQueries import update_end_time
from TransactionalLog import generate_transactional_log
from DatabaseQueries import update_completed_status_api
from SendEmail import send_email


def main():
    excel_file = 'Australia_Main_Config.xlsx'
    run_environment = os.environ.get('RunEnvironment')
    system_name = os.environ.get('SystemName')
    if 'dev' in run_environment.lower():
        sheet_name = "DEV"
    else:
        sheet_name = "PROD"
    try:
        setup_logging()
        config_dict, config_status = create_main_config_dictionary(excel_file, sheet_name)
        if config_status == "Pass":
            logging.info("Config Read successfully")
            db_config = get_db_credentials(config_dict)
            while True:
                registration_no = None
                receipt_no = None
                company_name = None
                database_id = None
                pending_orders_data = fetch_orders_to_extract_data(db_config)
                if len(pending_orders_data) == 0:
                    logging.info(f"No more orders to extract")
                    break
                for pending_order in pending_orders_data:
                    try:
                        attachments = []
                        database_id = pending_order[3]
                        update_locked_by(db_config, database_id)
                        receipt_no = pending_order[0]
                        registration_no = pending_order[1]
                        company_name = pending_order[2]
                        workflow_status = pending_order[4]
                        if str(workflow_status).lower() == 'extraction_pending':
                            data_extraction = data_extraction_and_insertion(db_config, registration_no, config_dict)
                            if data_extraction:
                                logging.info(f"Successfully extracted data for reg no - {registration_no}")
                                update_workflow_status(db_config, database_id, 'Loader_pending')
                                update_locked_by_empty(db_config, database_id)
                        if str(workflow_status).lower() == 'loader_pending':
                            loader_status, final_email_table, json_file_path, financial_table = json_loader_and_tables(db_config, excel_file, registration_no, receipt_no, config_dict, database_id)
                            if loader_status:
                                logging.info(f"Successfully extracted JSON Loader for reg no - {registration_no}")
                                update_workflow_status(db_config, database_id, 'Loader_generated')
                                update_process_status(db_config, database_id, 'Completed')
                                update_bot_comments_empty(db_config, registration_no, database_id)
                                update_end_time(db_config, registration_no, database_id)
                                transactional_log_file_path = generate_transactional_log(db_config, config_dict)
                                completed_subject = str(config_dict['cin_Completed_subject']).format(registration_no,
                                                                                                     receipt_no)
                                completed_body = str(config_dict['cin_Completed_body']).format(registration_no,
                                                                                               receipt_no, company_name,
                                                                                               final_email_table, financial_table, system_name)
                                business_mails = str(config_dict['business_mail']).split(',')
                                attachments.append(json_file_path)
                                attachments.append(transactional_log_file_path)
                                # api_update_status = update_completed_status_api(receipt_no, config_dict)
                                # if api_update_status:
                                #     logging.info(f"Successfully updated in API for Receipt No - {receipt_no}")
                                try:
                                    send_email(config_dict, completed_subject, completed_body, business_mails,
                                               attachments)
                                except Exception as e:
                                    logging.error(f"Error sending mail {e}")
                            update_locked_by_empty(db_config, database_id)
                    except Exception as e:
                        logging.error(f"Exception occurred for Reg no - {registration_no} \n {e}")
                        exception_handler(e, registration_no, config_dict, receipt_no, company_name, database_id, db_config)
    except Exception as e:
        logging.error(f"Exception {e} occurred in master script")


if __name__ == "__main__":
    main()
