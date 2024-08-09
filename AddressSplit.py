import json
import mysql.connector
import logging
from PythonLogging import setup_logging
from OpenAI import split_openai


def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text


def split_address(registration_no,config_dict,db_config):
    setup_logging()
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    address_query = f"select address,id from authorized_signatories where registration_no = '{registration_no}'"
    logging.info(address_query)
    cursor.execute(address_query)
    address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    prompt = config_dict['Prompt']
    for address in address_list:
        try:
            address_to_split = address[0]
            database_id = address[1]
            address_to_split = address_to_split.replace("'", "").replace('"', "")
            logging.info(address_to_split)
            if str(address_to_split).lower() != 'null' and address_to_split is not None:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                connection.autocommit = True
                splitted_address = split_openai(address_to_split,prompt)
                splitted_address = remove_text_before_marker(splitted_address, "```json")
                splitted_address = remove_string(splitted_address, "```")
                try:
                    splitted_address = json.loads(splitted_address)
                except Exception as e:
                    splitted_address = eval(splitted_address)
                splitted_address['address_line2'] = splitted_address['address_line1']
                splitted_address['address_line1'] = address_to_split
                try:
                    splitted_address = str(splitted_address).replace("'",'"')
                except:
                    pass
                update_query = f"update authorized_signatories set splitted_address = '{splitted_address}' where registration_no = '{registration_no}' and id = {database_id}"
                logging.info(update_query)
                cursor.execute(update_query)
                cursor.close()
                connection.close()
        except Exception as e:
            logging.error(f"Error in splitting address for  {e}")

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    previous_address_query = f"select previous_address,id from previous_address where registration_no = '{registration_no}'"
    logging.info(previous_address_query)
    cursor.execute(previous_address_query)
    previous_address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for previous_address in previous_address_list:
        try:
            previous_address_to_split = previous_address[0]
            database_id = previous_address[1]
            previous_address_to_split = previous_address_to_split.replace("'", "").replace('"', "")
            if str(previous_address_to_split).lower() != 'null' and previous_address_to_split is not None:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                connection.autocommit = True
                previous_splitted_address = split_openai(previous_address_to_split, prompt)
                previous_splitted_address = remove_text_before_marker(previous_splitted_address, "```json")
                previous_splitted_address = remove_string(previous_splitted_address, "```")
                try:
                    previous_splitted_address = json.loads(previous_splitted_address)
                except Exception as e:
                    previous_splitted_address = eval(previous_splitted_address)
                city = previous_splitted_address['city']
                state = previous_splitted_address['state']
                pincode = previous_splitted_address['pincode']
                try:
                    previous_splitted_address = str(previous_splitted_address).replace("'",'"')
                except:
                    pass
                update_query = f"update previous_address set previous_splitted_address	 = '{previous_splitted_address}',city = '{city}',state = '{state}', pincode = '{pincode}' where registration_no = '{registration_no}' and id = {database_id}"
                logging.info(update_query)
                cursor.execute(update_query)
                cursor.close()
                connection.close()
        except Exception as e:
            logging.error(f"Error in splitting previous address for  {e}")
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    registered_address_query = f"select registered_full_address,id from Company where registration_no = '{registration_no}'"
    logging.info(registered_address_query)
    cursor.execute(registered_address_query)
    registered_address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for registered_address in registered_address_list:
        try:
            registered_address_to_split = registered_address[0]
            database_id = registered_address[1]
            registered_address_to_split = registered_address_to_split.replace("'", "").replace('"', "")
            if str(registered_address_to_split).lower() != 'null' and registered_address_to_split is not None:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                connection.autocommit = True
                registered_splitted_address = split_openai(registered_address_to_split, prompt)
                registered_splitted_address = remove_text_before_marker(registered_splitted_address, "```json")
                registered_splitted_address = remove_string(registered_splitted_address, "```")
                try:
                    registered_splitted_address = json.loads(registered_splitted_address)
                except Exception as e:
                    registered_splitted_address = eval(registered_splitted_address)
                registered_splitted_address['address_line2'] = registered_splitted_address['address_line1']
                registered_splitted_address['address_line1'] = registered_address_to_split
                address_line1 = registered_splitted_address['address_line1']
                address_line2 = registered_splitted_address['address_line2']
                city = registered_splitted_address['city']
                state = registered_splitted_address['state']
                pincode = registered_splitted_address['pincode']
                try:
                    registered_splitted_address = str(registered_splitted_address).replace("'", '"')
                except:
                    pass
                update_query = f"update Company set registered_splitted_address	 = '{registered_splitted_address}',registered_city = '{city}',registered_state = '{state}',registered_pincode = '{pincode}',registered_address_line1 = '{address_line1}',registered_address_line2 = '{address_line2}' where registration_no = '{registration_no}' and id = {database_id}"
                logging.info(update_query)
                cursor.execute(update_query)
                cursor.close()
                connection.close()
        except Exception as e:
            logging.error(f"Error in splitting registered address {e}")

    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    connection.autocommit = True
    business_address_query = f"select ba_full_address,id from Company where registration_no = '{registration_no}'"
    logging.info(business_address_query)
    cursor.execute(business_address_query)
    business_address_list = cursor.fetchall()
    cursor.close()
    connection.close()
    for business_address in business_address_list:
        try:
            business_address_to_split = business_address[0]
            database_id = business_address[1]
            business_address_to_split = business_address_to_split.replace("'", "").replace('"', "")
            if str(business_address_to_split).lower() != 'null' and business_address_to_split is not None:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                connection.autocommit = True
                business_splitted_address = split_openai(business_address_to_split, prompt)
                business_splitted_address = remove_text_before_marker(business_splitted_address, "```json")
                business_splitted_address = remove_string(business_splitted_address, "```")
                try:
                    business_splitted_address = json.loads(business_splitted_address)
                except Exception as e:
                    business_splitted_address = eval(business_splitted_address)
                business_splitted_address['address_line2'] = business_splitted_address['address_line1']
                business_splitted_address['address_line1'] = business_address_to_split
                address_line1 = business_splitted_address['address_line1']
                address_line2 = business_splitted_address['address_line2']
                city = business_splitted_address['city']
                state = business_splitted_address['state']
                pincode = business_splitted_address['pincode']
                try:
                    business_splitted_address = str(business_splitted_address).replace("'", '"')
                except:
                    pass
                update_query = f"update Company set ba_city = '{city}',ba_state = '{state}',ba_pincode = '{pincode}',ba_address_line1 = '{address_line1}',ba_address_line2 = '{address_line2}' where registration_no = '{registration_no}' and id = {database_id}"
                logging.info(update_query)
                cursor.execute(update_query)
                cursor.close()
                connection.close()
        except Exception as e:
            logging.error(f"Error in splitting registered address {e}")
