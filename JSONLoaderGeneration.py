import json
import mysql.connector
from ReadExcelConfig import create_main_config_dictionary
import os
import shutil
import logging
from PythonLogging import setup_logging
import datetime
from datetime import date
import traceback


def get_json_node_names(data, parent_name=''):
    node_names = []
    if isinstance(data, dict):
        for key, value in data.items():
            node_name = f"{parent_name}.{key}" if parent_name else key
            get_json_node_names(value, node_name)
            node_names.append(node_name)
    elif isinstance(data, list):
        for index, item in enumerate(data):
            node_name = f"{parent_name}[{index}]"
            get_json_node_names(item, node_name)
            node_names.append(node_name)
    return node_names
# Read JSON data from a file


def decode_json(json_obj):
    if isinstance(json_obj, str):
        # Decode the string using unicode_escape codec
        return bytes(json_obj, "utf-8").decode("unicode_escape")
    elif isinstance(json_obj, dict):
        # Recursively decode string values in dictionary
        return {key: decode_json(value) for key, value in json_obj.items()}
    elif isinstance(json_obj, list):
        # If it's a list, decode string values recursively
        return [decode_json(item) if isinstance(item, str) else decode_json_dict(item) for item in json_obj]
    else:
        # Return other types as is
        return json_obj


def decode_json_dict(json_obj):
    if isinstance(json_obj, str):
        # Decode the string using unicode_escape codec
        return bytes(json_obj, "utf-8").decode("unicode_escape")
    elif isinstance(json_obj, dict):
        # Recursively decode string values in dictionary
        return {key: decode_json_dict(value) for key, value in json_obj.items()}
    else:
        # Return other types as is
        return json_obj


def remove_backslashes(data):
    if isinstance(data, dict):
        return {key: remove_backslashes(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [remove_backslashes(element) for element in data]
    elif isinstance(data, str):
        return data.replace('\\', '')
    else:
        return data


def json_loader(db_config, config_json_file_path, registration_no, root_path, excel_path, sheet_name, receiptno):
    error_count = 0
    errors = []
    try:
        setup_logging()
        if not os.path.exists(config_json_file_path):
            raise Exception("Config file not exists")
        json_folder_name = 'JSons'
        json_folder_path = os.path.join(root_path, json_folder_name)
        if not os.path.exists(json_folder_path):
            os.makedirs(json_folder_path)
        current_date = datetime.date.today()
        file_name = receiptno
        json_file_path = os.path.join(json_folder_path, file_name)
        json_file_path = json_file_path + '.json'
        if not os.path.exists(json_file_path):
            shutil.copy(config_json_file_path, json_file_path)
        with open(config_json_file_path, encoding='utf-8') as f:
            json_data = json.load(f)
        try:
            json_data["metatag"]["last_updated"] = date.today().strftime("%Y-%m-%d")
            json_data["metatag"]["MNS_receiptno"] = receiptno
        except Exception as e:
            logging.info(f"Exception occurred while updating receipt no and date {e}")
        # Call the function with your JSON data, starting directly with the 'data' child nodes
        json_nodes = get_json_node_names(json_data.get('data', {}), parent_name='')
        config_dict_loader, status = create_main_config_dictionary(excel_path, sheet_name)
        for json_node in json_nodes:
            try:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                try:
                    company_query = config_dict_loader[json_node]
                except Exception as e:
                    continue
                if json_node == 'financials':
                    query = company_query.format(registration_no, registration_no, registration_no)
                else:
                    query = company_query.format(registration_no)
                logging.info(query)
                cursor.execute(query)
                result_company = cursor.fetchall()
                json_string = ', '.join(result_company[0])
                # Convert the JSON string to a Python dictionary
                company_data = json.loads(json_string)
                json_data["data"][json_node] = company_data
                cursor.close()
                connection.close()
            except Exception as e:
                logging.error(f"Exception occurred or no value for {json_node}{e}")
        # with open('Output_json.txt', 'w', encoding='utf-8') as text_file:
        #     text_file.write(json.dumps(json_data, ensure_ascii=False, indent=2))
        #
        # with open('Output_json.txt', 'r', encoding='utf-8') as file:
        #     text_data = file.read()
        # try:
        #     parsed_data = json.loads(text_data)
        # except json.JSONDecodeError as e:
        #     print(f"Error parsing JSON: {e}")
        #     # Handle the error, such as providing a default value or exiting the program
        #     exit()
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=2)
        with open('Output_json.txt', 'w', encoding='utf-8'):
            pass
    except Exception as e:
        logging.error(f"Exception occurred while preparing JSON Loader {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"File {frame.filename},Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        return True, json_file_path, json_nodes
