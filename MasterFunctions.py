from PythonLogging import setup_logging
from DatabaseQueries import get_documents_to_extract
import logging
import traceback
from DatabaseQueries import extraction_pending_files
from RegistryDocumentExtraction import registry_document_main
from DatabaseQueries import update_extraction_status
import os
from DatabaseQueries import get_financial_status
from Financial_Document_Extraction import finance_main
from DatabaseQueries import update_finance_status
from DatabaseQueries import update_pnl_status
from JSONLoaderGeneration import json_loader
from ReadExcelConfig import create_main_config_dictionary
from OrderJson import order_json
from FinalEmailTable import final_table
from Holding_Entities import get_holding_entities
from AddressSplit import split_address
from DatabaseQueries import get_extraction_status
from DatabaseQueries import update_extraction_status_directors
from DatabaseQueries import update_extraction_status_other_than_directors
from FinalEmailTable import financials_table


def data_extraction_and_insertion(db_config, registration_no, config_dict):
    setup_logging()
    error_count = 0
    errors = []
    try:
        documents_to_extract = get_documents_to_extract(db_config, registration_no)
        document_name = None
        document_download_path = None
        for document in documents_to_extract:
            try:
                document_id = document[0]
                document_name = document[2]
                document_download_path = document[6]
                category = document[3]
                output_path = str(document_download_path).replace('.pdf', '.xlsx')
                if 'registry' in str(category).lower():
                    extraction_status_directors, extraction_status_other_directors = get_extraction_status(db_config, registration_no, document_id)
                    if str(extraction_status_directors).lower() != 'y':
                        input_extraction = 'directors'
                        registry_document_extraction_directors = registry_document_main(db_config, config_dict, document_download_path, output_path, registration_no, input_extraction)
                        if registry_document_extraction_directors:
                            logging.info(f"Successfully extracted directors for {document_name}")
                            update_extraction_status_directors(db_config, registration_no, document_id)
                    if str(extraction_status_other_directors).lower() != 'y':
                        input_extraction = 'other_than_directors'
                        registry_document_extraction_other_than_directors = registry_document_main(db_config, config_dict, document_download_path, output_path, registration_no, input_extraction)
                        if registry_document_extraction_other_than_directors:
                            logging.info(f"Successfully extracted other than directors for {document_name}")
                            update_extraction_status_other_than_directors(db_config, registration_no, document_id)
                    updated_extraction_status_directors, updated_extraction_status_other_directors = get_extraction_status(db_config, registration_no, document_id)
                    if str(updated_extraction_status_directors).lower() == 'y' and str(updated_extraction_status_other_directors).lower() == 'y':
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
                elif 'financial' in str(category).lower():
                    temp_pdf_directory = os.path.dirname(document_download_path)
                    pdf_document_name = os.path.basename(document_download_path)
                    pdf_document_name = str(pdf_document_name).replace('.pdf', '')
                    temp_pdf_name_finance = 'temp_finance_' + pdf_document_name
                    if '.pdf' not in temp_pdf_name_finance:
                        temp_pdf_name_finance = temp_pdf_name_finance + '.pdf'
                    temp_pdf_path_finance = os.path.join(temp_pdf_directory, temp_pdf_name_finance)
                    finance_output_file_name = 'finance_' + pdf_document_name
                    if '.xlsx' not in finance_output_file_name:
                        finance_output_file_name = finance_output_file_name + '.xlsx'
                    finance_output_file_path = os.path.join(temp_pdf_directory, finance_output_file_name)
                    finance_status, profit_and_loss_status = get_financial_status(db_config, registration_no,
                                                                                  document_id)
                    finance_input = config_dict['financial_input']
                    if str(finance_status).lower() != 'y':
                        main_finance_extraction = finance_main(db_config, config_dict, document_download_path, registration_no, finance_output_file_path, finance_input, temp_pdf_path_finance)
                        if main_finance_extraction:
                            logging.info(f"Successfully extracted for assets and liabilities")
                            update_finance_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted for assets and liabilities")
                    temp_pdf_name_pnl = 'temp_pnl_' + pdf_document_name
                    if '.pdf' not in temp_pdf_name_pnl:
                        temp_pdf_name_pnl = temp_pdf_name_pnl + '.pdf'
                    temp_pdf_path_pnl = os.path.join(temp_pdf_directory, temp_pdf_name_pnl)
                    pnl_output_file_name = 'pnl_' + pdf_document_name
                    if '.xlsx' not in pnl_output_file_name:
                        pnl_output_file_name = pnl_output_file_name + '.xlsx'
                    pnl_output_path = os.path.join(temp_pdf_directory, pnl_output_file_name)
                    pnl_input = config_dict['pnl_input']
                    if str(profit_and_loss_status).lower() != 'y':
                        pnl_extraction = finance_main(db_config, config_dict, document_download_path, registration_no, pnl_output_path, pnl_input, temp_pdf_path_pnl)
                        if pnl_extraction:
                            logging.info(f"Successfully extracted Profit and Loss")
                            update_pnl_status(db_config, registration_no, document_id)
                    else:
                        logging.info(f"Already extracted Profit and Loss")
                    updated_finance_status, updated_pnl_status = get_financial_status(db_config, registration_no,
                                                                                      document_id)
                    if str(updated_finance_status).lower() == 'y' and str(updated_pnl_status).lower() == 'y':
                        logging.info(f"Successfully extracted for {document_name}")
                        update_extraction_status(db_config, document_id, registration_no)
            except Exception as e:
                logging.error(f"Error {e} occurred while extracting for file - {document_name} at path - {document_download_path}")
                error_count += 1
                tb = traceback.extract_tb(e.__traceback__)
                for frame in tb:
                    if frame.filename == __file__:
                        errors.append(f"File - {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        try:
            get_holding_entities(db_config, registration_no, config_dict)
        except Exception as e:
            logging.error(f"Error in fetching holding entities {e}")
        try:
            split_address(registration_no, config_dict, db_config)
        except Exception as e:
            logging.error(f"Error in splitting address {e}")
    except Exception as e:
        logging.error(f"Error occurred while extracting for Reg no - {registration_no}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        pending_files = extraction_pending_files(db_config, registration_no)
        if len(pending_files) == 0:
            return True
        else:
            raise Exception(f"Multiple exceptions occurred while extracting:\n\n" + "\n".join(errors))


def json_loader_and_tables(db_config, config_excel_path, registration_no, receipt_no, config_dict, database_id):
    errors = []
    try:
        config_json_file_path = config_dict['config_json_file_path']
        root_path = config_dict['Root path']
        sheet_name = 'JSON_Loader_SQL_Queries'
        final_email_table = None
        json_loader_status, json_file_path, json_nodes = json_loader(db_config, config_json_file_path, registration_no, root_path, config_excel_path, sheet_name, receipt_no)
        if json_loader_status:
            order_sheet_name = "JSON Non-LLP Order"
            config_dict_order, status = create_main_config_dictionary(config_excel_path, order_sheet_name)
            for json_node in json_nodes:
                try:
                    json_order_status = order_json(config_dict_order, json_node, json_file_path)
                    if json_order_status:
                        logging.info(f"Successfully ordered json for {json_node}")
                except Exception as e:
                    logging.error(f"Error occurred while ordering for {json_node} {e}")
            final_email_table = final_table(db_config, registration_no, database_id)
            financial_table = financials_table(db_config, registration_no)
    except Exception as e:
        logging.error(f"Exception occurred while generating json loader {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        return True, final_email_table, json_file_path, financial_table
