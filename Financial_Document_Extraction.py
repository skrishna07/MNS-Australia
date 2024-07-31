from PythonLogging import setup_logging
import logging
import os
import pandas as pd
import traceback
from OpenAI import split_openai
from AmazonOCRAllPages import extract_text_from_pdf
from AmazonOCR import extract_text_from_pdf_with_keyword
import re
from ReadExcelConfig import create_main_config_dictionary
import json
from DatabaseQueries import update_database_single_value_financial
from DatabaseQueries import get_db_credentials
import PyPDF2


def remove_text_before_marker(text, marker):
    index = text.find(marker)
    if index != -1:
        return text[index + len(marker):]
    return text


def remove_string(text, string_to_remove):
    if string_to_remove in text:
        text = text.replace(string_to_remove, "")
    return text


def find_header_and_next_pages(pdf_path, fields):
    # Open the PDF file
    pdf_file = open(pdf_path, 'rb')
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    num_pages = len(pdf_reader.pages)
    # Search for the header in the PDF
    for page_num in range(0, num_pages):
        # Read the current page and the next page
        page1 = pdf_reader.pages[page_num]
        text1 = page1.extract_text()
        # Combine text from both pages
        combined_text = text1
        combined_text = combined_text.replace(',','')
        numbers = re.findall(r'\b\d{5,}\b', combined_text)
        # Check if all fields are in the combined text
        if combined_text and all(field in combined_text.lower() for field in fields):
            next_page = page_num + 2 if page_num + 2 < num_pages else None
            return page_num, next_page
    return None, None


def split_pdf(file_path, start_page, end_page, output_path):
    setup_logging()
    try:
        pdf_reader = PyPDF2.PdfReader(open(file_path, 'rb'))
        pdf_writer = PyPDF2.PdfWriter()

        for page_num in range(start_page - 1, end_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])

        with open(output_path, 'wb') as output_pdf:
            pdf_writer.write(output_pdf)
    except Exception as e:
        logging.error(f"Error in splitting pdf {e}")
        raise Exception(e)


def finance_main(db_config, config_dict, pdf_path, registration_no, output_file_path, financial_type, temp_pdf_path):
    setup_logging()
    error_count = 0
    errors = []
    try:
        if financial_type == 'finance':
            keywords = str(config_dict['financial_headers']).split(',')
        elif financial_type == 'pnl':
            keywords = str(config_dict['profit_and_loss_headers']).split(',')
        else:
            raise Exception("No Input financial type provided")
        start_page, end_page = find_header_and_next_pages(pdf_path, keywords)
        if start_page is not None and end_page is not None:
            split_pdf(pdf_path,start_page,end_page,temp_pdf_path)
            extracted_text = extract_text_from_pdf(temp_pdf_path)
            extracted_text = extracted_text.replace(',', '')
            logging.info(extracted_text)
        else:
            extracted_text = extract_text_from_pdf_with_keyword(pdf_path,keywords)
            extracted_text = extracted_text.replace(',', '')
        if "$'000" in extracted_text or "$ 000" in extracted_text or "$'000'" in extracted_text:
            currency = 'AUD 000'
        else:
            currency = 'AUD'
        config_file_path = config_dict['financial_config']
        map_file_sheet_name = config_dict['config_sheet']
        if not os.path.exists(config_file_path):
            raise Exception("Main Mapping File not found")
        try:
            df_map = pd.read_excel(config_file_path, engine='openpyxl', sheet_name=map_file_sheet_name)
        except Exception as e:
            raise Exception(f"Below exception occurred while reading mapping file {e}")
        df_map['Value'] = None
        open_ai_df_list = []
        registration_no_column_name = config_dict['registration_no_Column_name']
        if financial_type == 'finance':
            financial_df = df_map[(df_map['Type_of_financial'] == config_dict['financial_keyword']) | (df_map['Type_of_financial'] == config_dict['common_keyword'])]
        elif financial_type == 'pnl':
            financial_df = df_map[(df_map['Type_of_financial'] == config_dict['profit_and_loss_keyword']) | (
                        df_map['Type_of_financial'] == config_dict['common_keyword'])]
        else:
            raise Exception("No input financial type provided")
        straight_df = financial_df[(financial_df[financial_df.columns[1]] == config_dict['financial_straight_keyword']) &
                            (financial_df['Node'].notna()) &
                            (financial_df['Node'] != '') &
                            (financial_df['Node'] != 'null')]
        main_field_nodes = straight_df['main_dict_node'].unique()
        open_ai_dict = {}
        for field_node in main_field_nodes:
            straight_nodes_list = straight_df[straight_df['main_dict_node'] == field_node]['Node'].unique()
            open_ai_dict[field_node] = {field_name: '' for field_name in straight_nodes_list}
        straight_field_nodes = straight_df[(straight_df['main_dict_node'] == '') | (straight_df['main_dict_node'].isna())]['Node'].unique()
        exclude_fields = ['year', 'financial_year', 'nature', 'filing_type', 'filing_standard','Currency']
        master_dict = {"Group": [{'YYYY': ""}], "Company": [{'YYYY': ""}]}
        open_ai_dict_straight = {field_name: '' for field_name in straight_field_nodes if field_name not in exclude_fields}
        open_ai_dict.update(open_ai_dict_straight)
        master_dict["Group"][0]["YYYY"] = str(open_ai_dict)
        master_dict["Company"][0]["YYYY"] = str(open_ai_dict)
        logging.info(master_dict)
        prompt = config_dict['financial_prompt'] + '\n' + str(master_dict) + '\n' + '\n' + str(config_dict['financial_example_prompt'])
        output = split_openai(extracted_text, prompt)
        try:
            output = re.sub(r'(?<=: ")(\d+(,\d+)*)(?=")', lambda x: x.group(1).replace(",", ""), output)
        except:
            pass
        output = remove_text_before_marker(output, "```json")
        output = remove_string(output, "```")
        logging.info(output)
        try:
            output = eval(output)
        except:
            output = json.loads(output)
        try:
            if len(output["Group"]) != 0:
                group_output = output["Group"][0]
            else:
                group_output = {}
        except:
            group_output = {}
        try:
            if len(output["Company"]) != 0:
                company_output = output["Company"][0]
            else:
                company_output = {}
        except:
            company_output = {}
        main_group_df = financial_df.copy()
        main_company_df = financial_df.copy()
        df_list = []
        for key, value in company_output.items():
            company_year_df = main_company_df.copy()
            nature = 'Standalone'
            for index,row in company_year_df.iterrows():
                field_name = str(row.iloc[0]).strip()
                main_node = row['main_dict_node']
                value_type = str(row.iloc[1]).strip()
                if value_type.lower() == 'straight':
                    node = row['Node']
                    if field_name == 'year':
                        financial_value = key
                    elif field_name == 'nature':
                        financial_value = nature
                    elif field_name == 'Currency':
                        financial_value = currency
                    elif field_name == 'filing_type':
                        financial_value = 'Annual return'
                    else:
                        if pd.notna(main_node) and main_node != '' and main_node != 'nan':
                            financial_value = value[main_node][node]
                        else:
                            financial_value = value[node]
                    try:
                        if field_name != 'year':
                            financial_value = float(financial_value)
                    except:
                        pass
                    company_year_df.at[index, 'Value'] = financial_value
            df_list.append(company_year_df)
        for key, value in group_output.items():
            group_year_df = main_group_df.copy()
            nature = 'Consolidated'
            for index,row in group_year_df.iterrows():
                field_name = str(row.iloc[0]).strip()
                main_node = row['main_dict_node']
                value_type = str(row.iloc[1]).strip()
                if value_type.lower() == 'straight':
                    node = row['Node']
                    if field_name == 'year':
                        financial_value = key
                    elif field_name == 'nature':
                        financial_value = nature
                    elif field_name == 'Currency':
                        financial_value = currency
                    elif field_name == 'filing_type':
                        financial_value = 'Annual return'
                    else:
                        if pd.notna(main_node) and main_node != '' and main_node != 'nan':
                            financial_value = value[main_node][node]
                        else:
                            financial_value = value[node]
                    try:
                        if field_name != 'year':
                            financial_value = float(financial_value)
                    except:
                        pass
                    group_year_df.at[index, 'Value'] = financial_value
            df_list.append(group_year_df)
        for i, df in enumerate(df_list):
            formula_df = df[df[df.columns[1]] == config_dict['Formula_Keyword']]
            for _, row in formula_df.iterrows():
                company_formula = row['Node']
                company_formula_field_name = row['Field_Name']
                subtype = row['main_dict_node']
                for field_name in df['Field_Name']:
                    try:
                        field_name = str(field_name)
                        pattern = r'\b' + re.escape(field_name) + r'\b'
                        # current_formula = current_formula.replace(field_name, str(current_year_df[current_year_df['Field_Name'] == field_name]['Value'].values[0]))
                        if pd.notna(subtype) and subtype != '' and subtype != 'nan':
                            replacement_value = str(
                                df[(df['Field_Name'] == field_name) & (df['main_dict_node'] == subtype)]['Value'].values[0])
                        else:
                            replacement_value = str(
                                df[df['Field_Name'] == field_name]['Value'].values[0])
                        replacement_value = str(replacement_value) if replacement_value != '' else '0'
                        company_formula = re.sub(pattern, replacement_value,company_formula)
                    except Exception as e:
                        continue
                logging.info(company_formula_field_name + ":" + company_formula)
                try:
                    # Calculate the value using the provided formula and insert it
                    if 'None' in company_formula:
                        company_formula = company_formula.replace('None', '0')
                    if pd.notna(subtype) and subtype != '' and subtype != 'nan':
                        df.at[
                            df[(df['Field_Name'] == company_formula_field_name) & (df['main_dict_node'] == subtype)].index[
                                0], 'Value'] = round(eval(company_formula), 2)
                    else:
                        df.at[
                            df[df['Field_Name'] == company_formula_field_name].index[
                                0], 'Value'] = round(eval(company_formula), 2)
                except (NameError, SyntaxError):
                    # Handle the case where the formula is invalid or contains a missing field name
                    logging.info(f"Invalid formula for {company_formula_field_name}: {company_formula}")
            df_list[i] = df
        logging.info(df_list)
        for df_to_insert in df_list:
            sql_tables_list = df_to_insert[df_to_insert.columns[3]].unique()
            logging.info(sql_tables_list)
            year_value = df_to_insert[df_to_insert['Field_Name'] == 'year']['Value'].values[0]
            nature_value = df_to_insert[df_to_insert['Field_Name'] == 'nature']['Value'].values[0]
            logging.info(year_value)
            for table_name in sql_tables_list:
                table_df = df_to_insert[df_to_insert[df_to_insert.columns[3]] == table_name]
                columns_list = table_df[table_df.columns[4]].unique()
                logging.info(columns_list)
                for column_name in columns_list:
                    logging.info(column_name)
                    # filter table df with only column value
                    column_df = table_df[table_df[table_df.columns[4]] == column_name]
                    logging.info(column_df)
                    # create json dict with keys of field name and values for the same column name entries
                    json_dict = column_df.set_index(table_df.columns[0])['Value'].to_dict()
                    # Convert the dictionary to a JSON string
                    json_string = json.dumps(json_dict)
                    logging.info(json_string)
                    try:
                        update_database_single_value_financial(db_config, table_name, registration_no_column_name,
                                                               registration_no, column_name, json_string, year_value,
                                                               nature_value)
                    except Exception as e:
                        logging.info(f"Exception {e} occurred while updating data in dataframe for {table_name} "
                                     f"with data {json_string}")
        with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
            row_index = 0
            for dataframe in df_list:
                # logging.info(dataframe)
                dataframe.to_excel(writer, sheet_name='Sheet1', index=False, startrow=row_index)
                row_index += len(dataframe.index) + 2
    except Exception as e:
        logging.error(f"Error in extracting financial data for reg no - {registration_no}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception(errors)
    else:
        if error_count == 0:
            logging.info(f"Successfully extracted for Form 6")
            return True
        else:
            raise Exception(f"Multiple exceptions occurred:\n\n" + "\n".join(errors))
