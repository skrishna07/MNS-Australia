import mysql.connector
from PythonLogging import setup_logging
import logging
import unicodedata


def check_string(s, config_dict):
    s = s.lower()
    endings = str(config_dict['holding_entities_ending_keys']).split(',')
    for ending in endings:
        if s.endswith(ending):
            logging.info(f"{s} is matched as per rule for Holding entities")
            return True
    return False


def get_holding_entities(db_config, registration_no, config_dict):
    try:
        setup_logging()
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        shareholdings_details_query = f"select * from current_shareholdings where registration_no = '{registration_no}'"
        logging.info(shareholdings_details_query)
        cursor.execute(shareholdings_details_query)
        shareholders_result = cursor.fetchall()
        cursor.close()
        connection.close()
        for shareholders in shareholders_result:
            try:
                full_name = shareholders[2]
                full_name = unicodedata.normalize('NFKD', full_name).encode('ASCII', 'ignore').decode('utf-8')
                percentage_holding = shareholders[4]
                try:
                    percentage_holding = float(percentage_holding)
                except:
                    pass
                cin = shareholders[5]
                check_name = check_string(full_name, config_dict)
                if check_name and percentage_holding > 50:
                    connection = mysql.connector.connect(**db_config)
                    cursor = connection.cursor()
                    connection.autocommit = True
                    select_holding_entities = f"select * from holding_entities_companies where registration_no = '{registration_no}' and cin = '{cin}'"
                    logging.info(select_holding_entities)
                    cursor.execute(select_holding_entities)
                    holding_entities_result = cursor.fetchall()
                    if len(holding_entities_result) == 0:
                        insert_query = f"INSERT INTO holding_entities_companies(registration_no,cin,legal_name,share_holding_percentage) VALUES('{registration_no}','{cin}','{full_name}','{percentage_holding}')"
                        logging.info(insert_query)
                        cursor.execute(insert_query)
                    else:
                        update_query = f"Update holding_entities_companies set legal_name = '{full_name}', share_holding_percentage = '{percentage_holding}' where registration_no = '{registration_no}' and cin = '{cin}'"
                        logging.info(update_query)
                        cursor.execute(update_query)
                    cursor.close()
                    connection.close()
                else:
                    logging.info(f"No holding entities information matched for {full_name}")
            except Exception as e:
                logging.error(f"Error occurred while inserting holding entities {e}")
    except Exception as e:
        logging.error(f"Error in getting holding entities {e}")
