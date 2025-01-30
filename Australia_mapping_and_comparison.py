import pandas as pd
import re
import json  # Import the json module\
def australia_mapping_and_comp(output, excel_file_path, config_file_path, json_file_path, is_pnl):
    """
    config_file_path : Italian Financial Config
    excel_file_path  : PDF to Excel converted file.
    """
    print("italian mapping an comp called")
    print("is_pnl",is_pnl)
    european_number_regex = r'^\d{1,3}(,\d{3})*$'
    date_regex = r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}"
    parentheses_number_regex = r"\(\d{1,3}(,\d{3})*\)"
    # Load the financial config
    config_df = pd.read_excel(config_file_path)
    excel_nodes_list = []

    # Load the PDF to Excel converted file (to get all sheets)
    excel_sheets = pd.ExcelFile(excel_file_path)
    def safe_clean_field(field):
        # Check if the field is None or NaN (using float('nan'))
        if field is None or (isinstance(field, float) and field != field):  # field != field checks for NaN
            return ""
        return str(field).strip().lower()
    def preprocess_field(field):
        if not isinstance(field, str):
            if re.match(european_number_regex, str(field)):
                return f'{field}'  # Add quotes if it matches the regex
            else:
                return ""  # Return an empty string if it doesn't match
        cleaned_field = field.strip()
        cleaned_field = re.sub(r"^-+\s*|\n:unselected:|:unselected:\n",'', cleaned_field).strip()

        cleaned_field = cleaned_field.strip('-').strip()
        return cleaned_field
    # Define the headers for each category
    non_current_assets_headers = ["Non-current assets", "Non current activities", "FIXED ASSETS"]
    current_assets_headers = ["Current assets", "Current activities"]
    equity_related_headers = ["EQUITY"]
    non_current_liabilities_headers = ["Non-current liabilities", "Non current liabilities"]
    current_liabilities_headers = ["Current liabilities"]
    total_assets_headers = ["TOTAL ACTIVITIES","Total assets", "Activity"]
    total_liabilities_headers = ["Total Net Equity and Liabilities","Total LiabiLities and sharehoLders' Equity","Total shareholders' equity and liabilities", "Shareholders' equity and liabilities", "Total liabilities"]

    fields_to_ignore = ["non-current assets", "non current activities", "fixed assets", "current assets", "current activities",
        "equity", "non-current liabilities", "current liabilities", "total activities", "total assets", "activity",
        "total net equity and liabilities", "total liabilities and shareholders' equity",
        "total shareholders' equity and liabilities", "shareholders' equity and liabilities", "total liabilities",
        "liabilities and shareholders' equity", "total non-current assets", "total current assets",
        "total non-current liabilities", "total current liabilities", "total equity", "net assets", "Non current liabilities", "LIABILITIES"
    ]
    fields_to_ignore = [field.lower().strip() for field in fields_to_ignore]

    def update_both_entities_data(entity, node_with_values, is_standalone_first, main_dict_node):
        print("entitytt", entity)

        # Determine which values to use based on `is_standalone_first`
        for key, value in node_with_values.items():
            if is_standalone_first:
                if entity == 'Company':
                    current_year_value, previous_year_value = value[0], value[1]
                elif entity == 'Group':
                    current_year_value, previous_year_value = value[2], value[3]
            else:
                if entity == 'Group':
                    current_year_value, previous_year_value = value[0], value[1]
                elif entity == 'Company':
                    current_year_value, previous_year_value = value[2], value[3]

            print(f"curr {current_year_value} and prev {previous_year_value}")

            # Retrieve existing values from the `output` dictionary
            openai_current_value = (
                output.get(entity, [{}])[0]
                .get(current_year, {})
                .get(main_dict_node, {})
                .get(key, None)
            )
            openai_previous_value = (
                output.get(entity, [{}])[0]
                .get(previous_year, {})
                .get(main_dict_node, {})
                .get(key, None)
                if previous_year is not None
                else None
            )

            # Update `output` if values are different
            if openai_current_value != current_year_value:
                if main_dict_node is None:
                    output.setdefault(entity, [{}])[0].setdefault(current_year, {})[
                        key
                    ] = current_year_value
                else:
                    output.setdefault(entity, [{}])[0].setdefault(current_year, {}).setdefault(
                        main_dict_node, {}
                    )[key] = current_year_value

            if previous_year and openai_previous_value != previous_year_value:
                if main_dict_node is None:
                    output.setdefault(entity, [{}])[0].setdefault(previous_year, {})[
                        key
                    ] = previous_year_value
                else:
                    output.setdefault(entity, [{}])[0].setdefault(previous_year, {}).setdefault(
                        main_dict_node, {}
                    )[key] = previous_year_value
        return output

    def compare_openai_and_Excel_data(node_list, fields_data, main_dict_node, both_entities_found, is_standalone_first):
        # print("fields_data", fields_data)
        print("both_entities_found",both_entities_found)
        node_with_values = {}
        for node in node_list:
            # print("node",node)
            node_found = False
            node_values = []  # Reset node values for each node
            nodes_not_to_keep = []
            for idx, field in enumerate(fields_data):
                # Check if the node matches the field with a similarity threshold
                if str(node).lower() == preprocess_field(field).lower():
                    print("Nodeeeeeeee", node)
                    print(f"Node '{node}' found at index {idx} in the fields_data.")

                    # Current year value is the next value after the node
                    if idx + 1 < len(fields_data):
                        current_year_value = fields_data[idx + 1]
                        if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(current_year_value)):
                            current_year_value = ""
                        print(f"Current Year Value: {current_year_value}")

                        if pd.isna(current_year_value) or current_year_value == '-' or current_year_value == 'nan':
                            current_year_value = ''
                            node_values.append(current_year_value)

                        # Ensure current_year_value is always a string before further processing
                        current_year_value = str(current_year_value)

                        # Check and process values with parentheses
                        if current_year_value.startswith('(') and current_year_value.endswith(')'):
                            current_year_value = current_year_value[1:-1]  # Remove brackets

                        # Match the value against the regex
                        match = re.search(european_number_regex, current_year_value)
                        if match or current_year_value == '':
                            if current_year_value != '':
                                # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                node_values.append(current_year_value)
                            # Previous year value can be extracted similarly, if available
                    if idx + 2 < len(fields_data):
                        previous_year_value = fields_data[idx + 2]
                        print(f"Previous Year Value: {previous_year_value}")
                        if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value)):
                            previous_year_value = ""
                        if pd.isna(previous_year_value) or previous_year_value == '-' or previous_year_value == 'nan':
                            previous_year_value = ''
                            node_values.append(previous_year_value)
                        # Ensure current_year_value is always a string before further processing
                        previous_year_value = str(previous_year_value)
                        # if isinstance(previous_year_value, float):
                        #     value = str(previous_year_value)  # Convert float to string for consistent processing
                        if previous_year_value.startswith('(') and previous_year_value.endswith(')'):
                            previous_year_value = previous_year_value[1:-1]
                        match = re.search(european_number_regex, str(previous_year_value))
                        if match or previous_year_value == '':
                            if previous_year_value != '':
                                # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                node_values.append(previous_year_value)
                    print("type",both_entities_found)
                    if both_entities_found:
                        if idx + 3 < len(fields_data):
                            current_year_value_1 = fields_data[idx + 3]
                            if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(current_year_value_1)):
                                current_year_value_1 = ""
                            print(f"Current Year Value: {current_year_value_1}")

                            if pd.isna(current_year_value_1) or current_year_value_1 == '-' or current_year_value_1 == 'nan':
                                current_year_value_1 = ''
                                node_values.append(current_year_value_1)

                            # Ensure current_year_value is always a string before further processing
                            current_year_value_1 = str(current_year_value_1)

                            # Check and process values with parentheses
                            if current_year_value_1.startswith('(') and current_year_value_1.endswith(')'):
                                current_year_value_1 = current_year_value_1[1:-1]  # Remove brackets

                            # Match the value against the regex
                            match = re.search(european_number_regex, current_year_value_1)
                            if match or current_year_value_1 == '':
                                if current_year_value_1 != '':
                                    # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                    node_values.append(current_year_value_1)
                        if idx + 4 < len(fields_data):
                            previous_year_value_1 = fields_data[idx + 4]
                            print(f"Previous Year Value: {previous_year_value_1}")
                            if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value_1)):
                                previous_year_value_1 = ""
                            if pd.isna(previous_year_value_1) or previous_year_value_1 == '-' or previous_year_value_1 == 'nan':
                                print("yeesss")
                                previous_year_value_1 = ''
                                node_values.append(previous_year_value_1)
                            # Ensure current_year_value is always a string before further processing
                            previous_year_value_1 = str(previous_year_value_1)
                            # if isinstance(previous_year_value, float):
                            #     value = str(previous_year_value)  # Convert float to string for consistent processing
                            if previous_year_value_1.startswith('(') and previous_year_value_1.endswith(')'):
                                previous_year_value_1 = previous_year_value_1[1:-1]
                            match = re.search(european_number_regex, str(previous_year_value_1))
                            if match or previous_year_value_1 == '':
                                if previous_year_value_1 != '':
                                    # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                    node_values.append(previous_year_value_1)

                    node_found = True
                    node_with_values[node] = node_values
                    print("node_with_values", node_with_values)
                    break  # Break the inner loop as we found the node
            if node_found and not both_entities_found:
                excel_nodes_list.append(node)
                print(f"Extracted values for '{node}': {node_values}")

                if len(node_values) >= 1:
                    current_year_value = node_values[0]
                    previous_year_value = node_values[1] if len(node_values) > 1 else None

                    # Check if the node is in the total_assets_list
                    if any(total_asset_keyword.lower() == node.lower() for total_asset_keyword in
                           total_assets_list):
                        print(f"Node '{node}' is in total_assets_list. Updating the values.")
                        output[entity][0][current_year][node] = current_year_value
                        if previous_year:
                            output[entity][0][previous_year][node] = previous_year_value
                        continue  # Skip the rest of the logic for this node

                    # Handle OpenAI current and previous values
                    openai_current_value = output.get(entity, [{}])[0].get(current_year, {}).get(main_dict_node,
                                                                                                 {}).get(node, None)
                    openai_previous_value = None

                    if previous_year is not None:
                        openai_previous_value = output.get(entity, [{}])[0].get(previous_year, {}).get(
                            main_dict_node,
                            {}).get(node,
                                    None)

                    if openai_current_value != current_year_value or openai_previous_value != previous_year_value:
                        if openai_current_value != current_year_value:
                            if main_dict_node is None:
                                output[entity][0][current_year][node] = current_year_value
                            else:
                                output[entity][0][current_year].setdefault(main_dict_node, {})[
                                    node] = current_year_value

                        if previous_year and openai_previous_value != previous_year_value:
                            if main_dict_node is None:
                                output[entity][0][previous_year][node] = previous_year_value
                            else:
                                output[entity][0][previous_year].setdefault(main_dict_node, {})[
                                    node] = previous_year_value
                    else:
                        if current_year_value:
                            if main_dict_node is None:
                                output[entity][0][current_year][node] = current_year_value
                            else:
                                output[entity][0][current_year].setdefault(main_dict_node, {})[
                                    node] = current_year_value

                        if previous_year_value and previous_year is not None:
                            if main_dict_node is None:
                                output[entity][0][previous_year][node] = previous_year_value
                            else:
                                output[entity][0][previous_year].setdefault(main_dict_node, {})[
                                    node] = previous_year_value

                print("excel_nodes_list", excel_nodes_list)
        else:
            if both_entities_found:
                if entity == 'Group':
                    update_both_entities_data(entity, node_with_values, is_standalone_first, main_dict_node)
                elif entity == 'Company':
                    update_both_entities_data(entity, node_with_values, is_standalone_first, main_dict_node)
        return output, excel_nodes_list

    def remove_duplicate_nodes_from_final_output(output, config_df, fields_data, is_pnl):
        segregated_nodes = {
            'Non-current assets': {},
            'Current assets': {},
            'EQUITY': {},
            'Non-current liabilities': {},
            'Current liabilities': {},
            'Profit and Loss': {}
        }

        special_nodes = [
            "Total assets",
            'TOTAL ACTIVITIES',
            "Activity",
            "Total shareholders' equity and liabilities",
            "Total LiabiLities and sharehoLders' Equity",
            'Total Net Equity and Liabilities',
            "Shareholders' equity and liabilities",
            "Total liabilities"
        ]
        # Iterate through each row in config_df
        for _, row in config_df.iterrows():
            main_dict_node = row['main_dict_node']
            node_type = row['Type']
            node = row['Node']
            column_name = row['Column_Name']
            group = row['Group']

            # Skip invalid groups
            if pd.isna(group) or group == 'nan':
                continue

            # Special handling for special nodes
            if node in special_nodes:
                if "" not in segregated_nodes:
                    segregated_nodes[""] = {}
                if group not in segregated_nodes[""]:
                    segregated_nodes[""][group] = []
                segregated_nodes[""][group].append(node)
            elif is_pnl and column_name == 'financials_pnl_lineitems':
                if node_type == 'Straight' and node != "Null":
                    segregated_nodes.setdefault("Profit and Loss", {})
                    segregated_nodes["Profit and Loss"].setdefault(group, [])
                    segregated_nodes["Profit and Loss"][group].append(node)
            elif not is_pnl and node_type == 'Straight' and node != "Null":
                if main_dict_node in segregated_nodes:
                    segregated_nodes[main_dict_node].setdefault(group, [])
                    segregated_nodes[main_dict_node][group].append(node)
        # Processing segregated_nodes for exact and partial matches in Excel
        for main_node, groups in segregated_nodes.items():
            # print(f"Main Node: {main_node}")
            for group, nodes in groups.items():
                # print("Group", groups)
                # print("nodes",nodes)
                node_values = []
                nodes_to_keep = []
                for node in nodes:
                    node_found = False
                    node_index = None  # Reset for each node
                    for idx, field in enumerate(fields_data):
                        if str(node).lower() == preprocess_field(field).lower():
                            node_found = True
                            node_index = idx
                            break  # Exit the loop once a match is found
                    # print("node_index", node_index)

                    # Only proceed if a match is found
                    if node_index is not None:
                        value_indices = [node_index + 1, node_index + 2]

                        # Loop through the value indices
                        for i, value_idx in enumerate(value_indices):
                            if value_idx < len(fields_data):
                                value = fields_data[value_idx]
                                if pd.isna(value):  # Check if value is NaN
                                    value = ""  # Assign an empty string
                                if value.startswith('(') and value.endswith(')'):
                                    value = value[1:-1]
                                    # Check for date format and assign an empty string if matched
                                if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(value)):
                                    print("yesssssssssss")
                                    value = ""
                                    node_values.append(value)
                                    print("Node_values", node_values)
                                print(f"Value {i + 1}: {value}")
                                if re.match(european_number_regex, value):  # Validate with regex
                                    node_values.append(value)
                        if node_values!= [] and node_values[0] == '' and node_values[1] == '':
                            print("Node",node)
                            node_values = []

                    if node_found and len(node_values)>0:
                        print("Node_found", node)
                        node_found_in_excel = True
                        nodes_to_keep.append(node)  # Add this node to the nodes to keep
                        excel_nodes_list.append(node)
                        print("nodes_to_keep", nodes_to_keep)
                        # Now, update the output and remove any nodes not found in Excel
                        for group, nodes in groups.items():
                            # For each group, remove nodes that are not found in Excel
                            # print(f"group {group} and nodes {nodes}")
                            for node in nodes:
                                # print("for node in nodes", node)
                                if node in nodes_to_keep:
                                    print(
                                        f"    Node: {node} found in Excel, removing other nodes from the group {group}")
                                    # Loop over both current and previous year sections and remove other nodes
                                    year_data = {}
                                    for year in [current_year, previous_year]:
                                        if main_node == "Profit and Loss" or main_node == '':
                                            # Directly retrieve year data for Profit and Loss
                                            year_data = output.get(entity, [{}])[0].get(year, {})
                                        else:
                                            year_data = output.get(entity, [{}])[0].get(year, {}).get(main_node, {})

                                        print("year_Data", year_data)
                                        if node in year_data:
                                            # Remove all other nodes in this group from the current year's section
                                            for other_node in nodes:
                                                # print("other_node")
                                                if other_node != node and other_node in year_data:
                                                    print(
                                                        f"    Removing {other_node} from {main_node} in {year}")
                                                    del year_data[other_node]  # Remove the other node
                                                # After checking and modifying, only keep the nodes that were found in Excel
                        for node in nodes_to_keep:
                            print(f"    Keeping node: {node}")

    def check_nodes_in_excel(headers_list, node_list, ignore_headers, main_dict_node, is_pnl=False, all_tags_data=None):
        global row_start, row_end, excel_nodes_list
        matched_headers = set()
        if all_tags_data is None:
            all_tags_data = []  # Initialize if not provided
        # print(f"main_dict_node {main_dict_node} and nodes_list {node_list}")
        row_end = None
        row_start = None
        new_tags_list = []
        new_tags_dict = {}
        standalone_synonyms = ["Economic Entity"]
        consolidated_synonyms = ["Parent Entity"]
        is_standalone_first = False
        # Extract values from the specified range
        fields_data = excel_data_frame.iloc[row_start:row_end].values.flatten()

        # Initialize flags
        standalone_found = any(synonym in fields_data for synonym in standalone_synonyms)
        consolidated_found = any(synonym in fields_data for synonym in consolidated_synonyms)

        # Find indexes of standalone_synonyms in fields_data
        # Find the first index of standalone_synonyms in fields_data
        standalone_index = next(
            (idx for idx, value in enumerate(fields_data) if value in standalone_synonyms),
            None
        )

        # Find the first index of consolidated_synonyms in fields_data
        consolidated_index = next(
            (idx for idx, value in enumerate(fields_data) if value in consolidated_synonyms),
            None
        )

        # Check if both entities are found
        if standalone_found and consolidated_found:
            both_entities_found = True
            if standalone_index < consolidated_index:
                is_standalone_first = True
            else:
                is_standalone_first = False
            print("Both standalone and consolidated entities found.")
            print(f"Indexes of standalone synonyms: {standalone_index}")
            print(f"Indexes of consolidated synonyms: {consolidated_index}")
        else:
            both_entities_found = False
            print("Either standalone or consolidated entity is missing.")

        if is_pnl:
            # Assuming excel_data_frame is your dataframe
            fields_data = excel_data_frame.iloc[row_start:row_end].values.flatten()
            row_start = 0
            row_end = len(fields_data)
            # Step 1: Find the indices of the fields in `fields_data`
            fields_data = [str(field).lower().strip() if isinstance(field, str) else str(field) for field in fields_data]
            indices = [i for i, field in enumerate(fields_data) if field in fields_to_ignore]
            if indices:
                # Step 2: Update row_start and row_end based on the first and last indices
                row_start = indices[0]
                row_end = indices[-1] + 1  # +1 to include the last index in the range
                print("row_start:", row_start, "First field in fields_data:", fields_data[row_start])
                print("row_end:", row_end, "Last field in fields_data:", fields_data[indices[-1]])
                print("data",fields_data[:row_start])
                # Step 3: Delete data within row_start and row_end and store the remaining data
                fields_data_new = list(fields_data[:row_start]) + list(fields_data[row_end:])
                print("Updated fields_data:", fields_data_new)

            # Initialize the final dictionary before the loop
            final_tags_dict = {'PnL': []}
            fields_to_ignore_1 = ['euro', '$', 'economic entity', 'parent entity', 'nan']
            # Loop through the fields_data
            for idx, field in enumerate(fields_data_new):

                if (
                        preprocess_field(field) and  # Ensure the field is not empty or None
                        all(node.lower() != preprocess_field(field).lower() for node in
                            node_list) and  # Not in node_list
                        preprocess_field(field).lower() not in (ignore.lower() for ignore in
                                                                fields_to_ignore) and  # Not in ignore list
                        not re.search(date_regex, preprocess_field(field)) and  # Field doesn't match date format
                        not re.search(european_number_regex,
                                      preprocess_field(field)) and  # Field doesn't match European number format
                        not re.match(parentheses_number_regex,
                                     preprocess_field(field)) and  # Field shouldn't be a number in parentheses
                        not pd.isna(preprocess_field(field)) and  # Field shouldn't be NaN
                        "unnamed" not in preprocess_field(field).lower() and  # Field shouldn't contain 'unnamed'
                        preprocess_field(field).lower() not in (ignore.lower() for ignore in
                                                                fields_to_ignore_1) and
                        not re.match(r"^\d{4}$", preprocess_field(field).strip())
                # Field shouldn't be a year (e.g., 2023)
                ):
                    # Add preprocessed field to the list
                    values_to_add = [preprocess_field(field)]

                    # Check bounds to avoid IndexError for current_year_value
                    if idx + 1 < len(fields_data_new):
                        current_year_value = fields_data_new[idx + 1]
                        print("current_year_value", current_year_value)
                        if pd.isna(current_year_value) or current_year_value == '-' or current_year_value == 'nan':
                            current_year_value = ''
                            values_to_add.append(current_year_value)

                        current_year_value = str(current_year_value)

                        # Check and process values with parentheses
                        if current_year_value.startswith('(') and current_year_value.endswith(')'):
                            current_year_value = current_year_value[1:-1]  # Remove brackets

                        # Match the value against the regex
                        match = re.search(european_number_regex, current_year_value)
                        if match or current_year_value == '':
                            if current_year_value != '':
                                # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                values_to_add.append(current_year_value)

                    if idx + 2 < len(fields_data_new):
                        previous_year_value = fields_data_new[idx + 2]
                        print("previous_year_value", previous_year_value)
                        if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value)):
                            previous_year_value = ""
                            print(f"Previous Year Value: {previous_year_value}")
                        if pd.isna(previous_year_value) or previous_year_value == '-' or previous_year_value == 'nan':
                            previous_year_value = ''
                            values_to_add.append(previous_year_value)
                        previous_year_value = str(previous_year_value)

                        if previous_year_value.startswith('(') and previous_year_value.endswith(')'):
                            previous_year_value = previous_year_value[1:-1]

                        match = re.search(european_number_regex, str(previous_year_value))
                        if match or previous_year_value == '':
                            if previous_year_value != '':
                                # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                values_to_add.append(previous_year_value)
                    if both_entities_found:
                        if idx + 3 < len(fields_data):
                            current_year_value_1 = fields_data[idx + 3]
                            if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(current_year_value_1)):
                                current_year_value_1 = ""
                            print(f"Current Year Value: {current_year_value_1}")

                            if pd.isna(
                                    current_year_value_1) or current_year_value_1 == '-' or current_year_value_1 == 'nan':
                                current_year_value_1 = ''
                                values_to_add.append(current_year_value_1)

                            # Ensure current_year_value is always a string before further processing
                            current_year_value_1 = str(current_year_value_1)

                            # Check and process values with parentheses
                            if current_year_value_1.startswith('(') and current_year_value_1.endswith(')'):
                                current_year_value_1 = current_year_value_1[1:-1]  # Remove brackets

                            # Match the value against the regex
                            match = re.search(european_number_regex, current_year_value_1)
                            if match or current_year_value_1 == '':
                                if current_year_value_1 != '':
                                    # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                    values_to_add.append(current_year_value_1)
                            print("yayyyy", current_year_value_1)
                        if idx + 4 < len(fields_data):
                            previous_year_value_1 = fields_data[idx + 4]
                            if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value_1)):
                                previous_year_value_1 = ""
                                print(f"Previous Year Value: {previous_year_value_1}")
                            if pd.isna(
                                    previous_year_value_1) or previous_year_value_1 == '-' or previous_year_value_1 == 'nan':
                                previous_year_value_1 = ''
                                values_to_add.append(previous_year_value_1)
                            # Ensure current_year_value is always a string before further processing
                            previous_year_value_1 = str(previous_year_value_1)
                            # if isinstance(previous_year_value, float):
                            #     value = str(previous_year_value)  # Convert float to string for consistent processing
                            if previous_year_value_1.startswith('(') and previous_year_value_1.endswith(')'):
                                previous_year_value_1 = previous_year_value_1[1:-1]
                            match = re.search(european_number_regex, str(previous_year_value_1))
                            if match or previous_year_value_1 == '':
                                if previous_year_value_1 != '':
                                    # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                    values_to_add.append(previous_year_value_1)

                    # Append the values to the final dictionary's 'PnL' list
                    final_tags_dict['PnL'].extend(values_to_add)

            # After the loop ends, `final_tags_dict` will contain all the values under the 'PnL' key
            print("Final tags data:", final_tags_dict)

            # Append the final dictionary to all_tags_data
            all_tags_data.append(final_tags_dict)

            main_dict_node = None
            output, excel_nodes_list = compare_openai_and_Excel_data(node_list, fields_data_new, main_dict_node, both_entities_found, is_standalone_first)
            remove_duplicate_nodes_from_final_output(output, config_df, fields_data_new, is_pnl)
            print("All tags data:", all_tags_data)
        else:
            print("Mainnn_dict_node", main_dict_node)
            row_start = 0
            row_end = len(excel_data_frame)
            fields_data = excel_data_frame.iloc[row_start:row_end].values.flatten()
            print("DataFrame shape:123", fields_data.shape)
            fields_to_ignore_1 = ['euro', '$', 'economic entity', 'parent entity', 'nan']
            header_row_index = None
            for header in headers_list:
                for idx, field in enumerate(fields_data):
                    # print("field", field)
                    # print("preprocess_field", preprocess_field(field).lower())
                    # print("header", header)

                    if str(header).lower() == preprocess_field(field).lower():
                        print(f"Header '{header}' found at index {idx} in fields_data.")
                        header_row_index = idx
                        break
                else:
                    # No matching header found, continue to the next field
                    print(f"No matching row index found for header")
                    continue

                # Fetch the next two values in the same row after the header
                financial_values = []  # To store the financial values found
                financial_values_found = False

                if header_row_index is not None:
                    value_indices = [header_row_index + 1, header_row_index + 2]
                    # Loop through the value indices
                    for i, value_idx in enumerate(value_indices):
                        if value_idx < len(fields_data):
                            value = fields_data[value_idx]
                            if pd.isna(value):  # Check if value is NaN
                                value = ""  # Assign an empty string
                            if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(value)):
                                # print("yesssssssssss")
                                value = ""
                            print(f"Value {i + 1}: {value}")
                            if re.match(european_number_regex, value):  # Validate with regex
                                financial_values.append(value)
                                financial_values_found = True
                    print("financial_values",financial_values)
                if header in total_assets_headers or header in total_liabilities_headers:
                    financial_values_found = False

                    # Print "Yes" if financial values are found, otherwise "No"
                if financial_values_found:
                    print(f"Yes, financial values found for '{header}' in the two-year columns.")
                    print(f"Financial values for '{header}': {financial_values}")

                    potential_row_end_1 = []
                    row_end = header_row_index  # This marks the end of the current section.

                    # Find potential row_end_1 values based on ignore_headers
                    for other_header in ignore_headers:
                        print("Checking for other_header in ignore_headers:", other_header)
                        for idx, field in enumerate(fields_data):
                            if str(other_header).lower() == preprocess_field(field).lower():
                                print(f"Potential row_end_2 found at row {idx} for header '{other_header}'")
                                potential_row_end_1.append(idx)
                                break
                    print("Potential row_end_1 indices:", potential_row_end_1)

                    # Determine the row_start (just before header_row_index)
                    row_start = 0  # Default to 0 if no valid start index is found
                    for idx_1 in sorted(potential_row_end_1):  # Sort in ascending order
                        if idx_1 < header_row_index:  # Find the largest index smaller than header_row_index
                            row_start = idx_1 + 1
                        else:
                            break  # Stop once we pass header_row_index

                    print("Row start determined as:", row_start)
                    fields_data_new = fields_data[row_start:row_end]

                    for idx, field in enumerate(fields_data_new):
                        if (
                                preprocess_field(field) and
                                all(node.lower() != preprocess_field(field).lower() for node in node_list) and
                                preprocess_field(field).lower() not in (ignore.lower() for ignore in
                                                                        fields_to_ignore) and  # Not in ignore list
                                not re.search(date_regex,
                                              preprocess_field(field)) and  # Field doesn't match date format
                                not re.search(european_number_regex,
                                              preprocess_field(field)) and  # Field doesn't match European number format
                                not re.match(parentheses_number_regex, preprocess_field(field)) and
                                not pd.isna(preprocess_field(field)) and  # Field shouldn't be NaN
                                "unnamed" not in preprocess_field(field).lower() and  # Field shouldn't
                                preprocess_field(field).lower() not in (ignore.lower() for ignore in fields_to_ignore_1) and
                                not re.match(r"^\d{4}$",preprocess_field(field).strip())
                        ):
                            values_to_add = [preprocess_field(field)]
                            print("preprocess_field",values_to_add )
                            # Check bounds to avoid IndexError
                            if idx + 1 < len(fields_data_new):
                                current_year_value = fields_data_new[idx + 1]
                                if pd.isna(current_year_value) or current_year_value == '-' or current_year_value == 'nan':
                                    current_year_value = ''
                                    values_to_add.append(current_year_value)

                                # Ensure current_year_value is always a string before further processing
                                current_year_value = str(current_year_value)

                                # Check and process values with parentheses
                                if current_year_value.startswith('(') and current_year_value.endswith(')'):
                                    current_year_value = current_year_value[1:-1]  # Remove brackets

                                # Match the value against the regex
                                match = re.search(european_number_regex, current_year_value)
                                if match or current_year_value == '':
                                    if current_year_value != '':
                                        # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace( '#', ',')
                                        values_to_add.append(current_year_value)

                            if idx + 2 < len(fields_data_new):
                                previous_year_value = fields_data_new[idx + 2]
                                if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value)):
                                    previous_year_value = ""
                                    print(f"Previous Year Value: {previous_year_value}")
                                if pd.isna(previous_year_value) or previous_year_value == '-' or previous_year_value == 'nan':
                                    previous_year_value = ''
                                    values_to_add.append(previous_year_value)

                                # Ensure current_year_value is always a string before further processing
                                previous_year_value = str(previous_year_value)

                                if previous_year_value.startswith('(') and previous_year_value.endswith(')'):
                                    previous_year_value = previous_year_value[1:-1]
                                match = re.search(european_number_regex, str(previous_year_value))
                                if match or previous_year_value == '':
                                    if previous_year_value != '':
                                        # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                        values_to_add.append(previous_year_value)
                            if both_entities_found:
                                if idx + 3 < len(fields_data):
                                    current_year_value_1 = fields_data[idx + 3]
                                    if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(current_year_value_1)):
                                        current_year_value_1 = ""
                                    print(f"Current Year Value: {current_year_value_1}")

                                    if pd.isna(current_year_value_1) or current_year_value_1 == '-' or current_year_value_1 == 'nan':
                                        current_year_value_1 = ''
                                        values_to_add.append(current_year_value_1)

                                    # Ensure current_year_value is always a string before further processing
                                    current_year_value_1 = str(current_year_value_1)

                                    # Check and process values with parentheses
                                    if current_year_value_1.startswith('(') and current_year_value_1.endswith(')'):
                                        current_year_value_1 = current_year_value_1[1:-1]  # Remove brackets

                                    # Match the value against the regex
                                    match = re.search(european_number_regex, current_year_value_1)
                                    if match or current_year_value_1 == '':
                                        if current_year_value_1 != '':
                                            # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                            values_to_add.append(current_year_value_1)
                                    print("yayyyy", current_year_value_1)
                                if idx + 4 < len(fields_data):
                                    previous_year_value_1 = fields_data[idx + 4]
                                    if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value_1)):
                                        previous_year_value_1 = ""
                                        print(f"Previous Year Value: {previous_year_value_1}")
                                    if pd.isna(previous_year_value_1) or previous_year_value_1 == '-' or previous_year_value_1 == 'nan':
                                        previous_year_value_1 = ''
                                        values_to_add.append(previous_year_value_1)
                                    # Ensure current_year_value is always a string before further processing
                                    previous_year_value_1 = str(previous_year_value_1)
                                    # if isinstance(previous_year_value, float):
                                    #     value = str(previous_year_value)  # Convert float to string for consistent processing
                                    if previous_year_value_1.startswith('(') and previous_year_value_1.endswith(')'):
                                        previous_year_value_1 = previous_year_value_1[1:-1]
                                    match = re.search(european_number_regex, str(previous_year_value_1))
                                    if match or previous_year_value_1 == '':
                                        if previous_year_value_1 != '':
                                            # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                            values_to_add.append(previous_year_value_1)

                            new_tags_list.extend(values_to_add)

                    # Update the main dictionary and append it to the list
                    if main_dict_node is not None:
                        new_tags_dict[main_dict_node] = new_tags_list
                        all_tags_data.append(new_tags_dict.copy())  # Use `.copy()` to avoid overwriting
                    print("new_tags_dict", new_tags_dict)

                    # Perform additional operations if needed
                    output, excel_nodes_list = compare_openai_and_Excel_data(node_list, fields_data_new, main_dict_node, both_entities_found, is_standalone_first)
                    remove_duplicate_nodes_from_final_output(output, config_df, fields_data_new, is_pnl)

                    # Print final list of all tags data
                    print("All tags data:", all_tags_data)
                    # remove_nodes_from_openai_data(output, main_dict_node, excel_nodes_list)
                else:
                    print(f"No, financial values found for '{header}' in the two-year columns.")
                    row_start = header_row_index
                    row_end_1 = len(fields_data)
                    print("row_start_1", row_start)
                    print("row_end_1", row_end_1)

                    # Initialize row_end_2 to a default value
                    row_end_2 = len(fields_data)
                    potential_row_end_2 = []  # List to hold possible row_end_2 values greater than row_start

                    # Find row_end_2 values based on ignore_headers
                    for other_header in ignore_headers:
                        print("other_header in ignore_headers", other_header)
                        for idx, field in enumerate(fields_data):
                            if str(other_header).lower() == preprocess_field(field).lower():
                                print(f"Potential row_end_2 found at row {idx} for header '{other_header}'")
                                potential_row_end_2.append(idx)
                                break

                    # Filter potential_row_end_2 to get the smallest value greater than row_start
                    potential_row_end_2 = [val for val in potential_row_end_2 if val > row_start]
                    if potential_row_end_2:
                        row_end_2 = min(potential_row_end_2)  # Smallest row_end_2 > row_start
                    else:
                        row_end_2 = len(fields_data)  # Default to the last row if no valid row_end_2 is found

                    print("row_start_2", row_start)
                    print("row_end_2", row_end_2)

                    # Determine the final row_end value
                    if row_end_2 > row_start:
                        row_end = row_end_2
                    else:
                        row_end = row_end_1
                    print("final row_end", row_end)

                    fields_data_new = fields_data[row_start:row_end]
                    print("fields_data_new", fields_data_new)

                    for idx, field in enumerate(fields_data_new):
                        if (
                                preprocess_field(field) and
                                all(node.lower() != preprocess_field(field).lower() for node in node_list) and
                                preprocess_field(field).lower() not in (ignore.lower() for ignore in
                                                                        fields_to_ignore) and  # Not in ignore list
                                not re.search(date_regex,
                                              preprocess_field(field)) and  # Field doesn't match date format
                                not re.search(european_number_regex,
                                              preprocess_field(field)) and  # Field doesn't match European number format
                                not re.match(parentheses_number_regex, preprocess_field(field)) and
                                not pd.isna(preprocess_field(field)) and  # Field shouldn't be NaN
                                "unnamed" not in preprocess_field(field).lower() and  # Field shouldn't
                                preprocess_field(field).lower() not in (ignore.lower() for ignore in
                                                                        fields_to_ignore_1) and
                                not re.match(r"^\d{4}$", preprocess_field(field).strip())
                        ):
                            values_to_add = [preprocess_field(field)]
                            print("Values_to_add", values_to_add)

                            # Check bounds to avoid IndexError
                            if idx + 1 < len(fields_data_new):
                                current_year_value = fields_data_new[idx + 1]
                                print("current_year_value", current_year_value)
                                if pd.isna(current_year_value) or current_year_value == '-' or current_year_value == 'nan':
                                    current_year_value = ''
                                    values_to_add.append(current_year_value)

                                # Ensure current_year_value is a string
                                current_year_value = str(current_year_value)

                                # Process parentheses
                                if current_year_value.startswith('(') and current_year_value.endswith(')'):
                                    current_year_value = current_year_value[1:-1]

                                # Match regex
                                match = re.search(european_number_regex, current_year_value)
                                if match or current_year_value == '':
                                    if current_year_value != '':
                                        # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                        values_to_add.append(current_year_value)

                            if idx + 2 < len(fields_data_new):
                                previous_year_value = fields_data_new[idx + 2]
                                print("previous_year_value", previous_year_value)
                                if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value)):
                                    previous_year_value = ""
                                    print(f"Previous Year Value: {previous_year_value}")
                                if pd.isna(previous_year_value) or previous_year_value == '-' or previous_year_value == 'nan':
                                    previous_year_value = ''
                                    values_to_add.append(previous_year_value)

                                # Ensure previous_year_value is a string
                                previous_year_value = str(previous_year_value)

                                # Process parentheses
                                if previous_year_value.startswith('(') and previous_year_value.endswith(')'):
                                    previous_year_value = previous_year_value[1:-1]
                                match = re.search(european_number_regex, previous_year_value)
                                if match or previous_year_value == '':
                                    if previous_year_value != '':
                                        # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace(  '#', ',')
                                        values_to_add.append(previous_year_value)
                            if both_entities_found:
                                if idx + 3 < len(fields_data):
                                    current_year_value_1 = fields_data[idx + 3]
                                    if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(current_year_value_1)):
                                        current_year_value_1 = ""
                                    print(f"Current Year Value: {current_year_value_1}")

                                    if pd.isna(current_year_value_1) or current_year_value_1 == '-' or current_year_value_1 == 'nan':
                                        current_year_value_1 = ''
                                        values_to_add.append(current_year_value_1)

                                    # Ensure current_year_value is always a string before further processing
                                    current_year_value_1 = str(current_year_value_1)

                                    # Check and process values with parentheses
                                    if current_year_value_1.startswith('(') and current_year_value_1.endswith(')'):
                                        current_year_value_1 = current_year_value_1[1:-1]  # Remove brackets

                                    # Match the value against the regex
                                    match = re.search(european_number_regex, current_year_value_1)
                                    if match or current_year_value_1 == '':
                                        if current_year_value_1 != '':
                                            # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                            values_to_add.append(current_year_value_1)
                                    print("yayyyy", current_year_value_1)
                                if idx + 4 < len(fields_data):
                                    previous_year_value_1 = fields_data[idx + 4]
                                    if re.search(r'\d{1,2}[./-]\d{1,2}[./-]\d{2,4}', str(previous_year_value_1)):
                                        previous_year_value_1 = ""
                                        print(f"Previous Year Value: {previous_year_value_1}")
                                    if pd.isna(previous_year_value_1) or previous_year_value_1 == '-' or previous_year_value_1 == 'nan':
                                        previous_year_value_1 = ''
                                        values_to_add.append(previous_year_value_1)
                                    # Ensure current_year_value is always a string before further processing
                                    previous_year_value_1 = str(previous_year_value_1)
                                    # if isinstance(previous_year_value, float):
                                    #     value = str(previous_year_value)  # Convert float to string for consistent processing
                                    if previous_year_value_1.startswith('(') and previous_year_value_1.endswith(')'):
                                        previous_year_value_1 = previous_year_value_1[1:-1]
                                    match = re.search(european_number_regex, str(previous_year_value_1))
                                    if match or previous_year_value_1 == '':
                                        if previous_year_value_1 != '':
                                            # formatted_value = match.group(0).replace('.', '#').replace(',', '.').replace('#', ',')
                                            values_to_add.append(previous_year_value_1)


                            new_tags_list.extend(values_to_add)

                    # Update and append the dictionary
                    if main_dict_node is not None:
                        new_tags_dict[main_dict_node] = new_tags_list
                        all_tags_data.append(new_tags_dict.copy())  # Append a copy of the current dict
                    print("new_tags_dict", new_tags_dict)

                    # Additional operations
                    output, excel_nodes_list = compare_openai_and_Excel_data(node_list, fields_data_new, main_dict_node, both_entities_found, is_standalone_first)
                    remove_duplicate_nodes_from_final_output(output, config_df, fields_data_new, is_pnl)

                    # Print the final list of all tags data
                    print("All tags data:", all_tags_data)
                # remove_nodes_from_openai_data(output, main_dict_node, excel_nodes_list)
        return all_tags_data
    for entity in output.keys():
        # Check if the entity has data
        if len(output[entity]) > 0:
            print(f"Processing data for {entity}...")

            # Get the years from the data (assuming they are in the format '31.12.22' and '31.12.21')
            years = list(output[entity][0].keys())
            # print("years",years)
            # Assume the first year in the list is the current year, and the second is the previous year
            current_year = years[0]
            # print("current_year",current_year)
            previous_year = None
            if len(years)>1:
                previous_year = years[1]
                # print("previous_year", previous_year)
            # Define the lists for each main_dict_node
            non_current_assets_list = []
            current_assets_list = []
            equity_list = []
            non_current_liabilities_list = []
            current_liabilities_list = []
            total_assets_list = None
            total_liabilities_list = None

            # Iterate through the financial config DataFrame and segregate nodes
            for _, row in config_df.iterrows():
                node = row['Node']
                # print("node",node)
                main_dict_node = row['main_dict_node']
                node_type = row['Type']  # Check the Type column
                column_name = row['Column_Name']

                # Append nodes only if Type is "Straight" and Node is not "Null"
                if node_type == 'Straight' and node != "Null":
                    if main_dict_node == 'Non-current assets':
                        non_current_assets_list.append(node)
                    elif main_dict_node == 'Current assets':
                        current_assets_list.append(node)
                    elif main_dict_node == 'EQUITY':
                        equity_list.append(node)
                    elif main_dict_node == 'Non-current liabilities':
                        non_current_liabilities_list.append(node)
                    elif main_dict_node == 'Current liabilities':
                        current_liabilities_list.append(node)
                total_assets_list = ["TOTAL ACTIVITIES", "Total assets", "Activity"]
                total_liabilities_list = ["Total Net Equity and Liabilities", "Total LiabiLities and sharehoLders' Equity", "Total shareholders' equity and liabilities", "Shareholders' equity and liabilities", "Total liabilities"]

            # Check nodes in all lists, with the appropriate headers to ignore for each
            non_current_assets_ignore_headers = current_assets_headers + equity_related_headers + non_current_liabilities_headers + current_liabilities_headers
            current_assets_ignore_headers = non_current_assets_headers + equity_related_headers + non_current_liabilities_headers + current_liabilities_headers
            equity_ignore_headers = non_current_assets_headers + current_assets_headers + non_current_liabilities_headers + current_liabilities_headers + total_assets_headers + total_liabilities_headers
            non_current_liabilities_ignore_headers = non_current_assets_headers + current_assets_headers + equity_related_headers + current_liabilities_headers
            current_liabilities_ignore_headers = non_current_assets_headers + current_assets_headers + equity_related_headers + non_current_liabilities_headers
            total_assets_ignore_headers = non_current_assets_headers + current_assets_headers + equity_related_headers + non_current_liabilities_headers
            total_liabilities_ignore_headers = non_current_assets_headers + current_assets_headers + equity_related_headers + non_current_liabilities_headers

            main_sections = [
                {
                    "headers": non_current_assets_headers,
                    "node_list": non_current_assets_list,
                    "ignore_headers": non_current_assets_ignore_headers,
                    "main_dict_node": "Non-current assets",
                    "is_pnl": False
                },
                {
                    "headers": current_assets_headers,
                    "node_list": current_assets_list,
                    "ignore_headers": current_assets_ignore_headers,
                    "main_dict_node": "Current assets",
                    "is_pnl": False
                },
                {
                    "headers": equity_related_headers,
                    "node_list": equity_list,
                    "ignore_headers": equity_ignore_headers,
                    "main_dict_node": "EQUITY",
                    "is_pnl": False
                },
                {
                    "headers": non_current_liabilities_headers,
                    "node_list": non_current_liabilities_list,
                    "ignore_headers": non_current_liabilities_ignore_headers,
                    "main_dict_node": "Non-current liabilities",
                    "is_pnl": False
                },
                {
                    "headers": current_liabilities_headers,
                    "node_list": current_liabilities_list,
                    "ignore_headers": current_liabilities_ignore_headers,
                    "main_dict_node": "Current liabilities",
                    "is_pnl": False
                },
                {
                    "headers": total_assets_headers,
                    "node_list": total_assets_list,
                    "ignore_headers": total_assets_ignore_headers,
                    "main_dict_node": None,
                    "is_pnl": False
                },
                {
                    "headers": total_liabilities_headers,
                    "node_list": total_liabilities_list,
                    "ignore_headers": total_liabilities_ignore_headers,
                    "main_dict_node": None,
                    "is_pnl": False
                }
            ]
            # Create an empty list to store DataFrames from all sheets
            sheets_to_concat = []
            keywords_to_skip = ["Adjustments for:", "cash flows", "TOTAL EQUITY - OPENING", "TOTAL EQUITY - CLOSING", "Balance at"]

            # Function to determine if a column name represents a year
            def is_year_column(column_name):
                # Assuming year columns are in the format "DD/MM/YYYY" or similar
                return re.match(r"\d{2}/\d{2}/\d{4}", str(column_name))

            # Process each sheet in the Excel file
            for sheet_name in excel_sheets.sheet_names:
                # print(f"Processing sheet: {sheet_name}")

                # Read the sheet into a DataFrame
                excel_df = excel_sheets.parse(sheet_name, header=0)

                # Check if any of the keywords are present in the sheet
                if excel_df.astype(str).apply(
                        lambda col: col.str.contains('|'.join(keywords_to_skip), case=False, na=False)).any().any():
                    print(f"Skipping sheet: {sheet_name} due to presence of keywords")
                    continue

                # Remove the "Nota" column (case-insensitive) from the DataFrame
                excel_df = excel_df.loc[:, ~excel_df.columns.str.lower().str.contains('note', na=False)]
                # Drop columns where any cell contains '[Subtotal]' (case-insensitive)
                excel_df = excel_df.loc[:, ~excel_df.apply(
                    lambda col: col.astype(str).str.contains(r'\[subtotal\]', case=False, na=False) |
                                col.astype(str).str.contains("of which due to related parties", case=False, na=False) |
                                col.astype(str).str.contains("NOTE", case=False, na=False)
                ).any()]

                # Get all columns that represent years
                year_columns = [col for col in excel_df.columns if is_year_column(col)]

                # Check if there are exactly 3 year columns
                if len(year_columns) == 3:
                    # Sort the year columns by date
                    year_columns_sorted = sorted(year_columns, key=lambda x: pd.to_datetime(x, format="%d/%m/%Y"))

                    # Keep only the latest 2 years
                    years_to_keep = year_columns_sorted[-2:]  # Last 2 columns
                    years_to_remove = year_columns_sorted[:-2]  # Columns to remove

                    # Drop the oldest year column from the DataFrame
                    excel_df = excel_df.drop(columns=years_to_remove)

                    print(f"Removed column(s): {years_to_remove}")
                else:
                    pass
                    # print("The DataFrame does not contain exactly 3 year columns.")

                # Include the headers as the first row in the DataFrame
                header_row = pd.DataFrame([excel_df.columns.tolist()], columns=excel_df.columns)
                excel_df.columns = range(len(excel_df.columns))  # Temporarily reset column names to prevent conflicts
                excel_df = pd.concat([header_row, excel_df], ignore_index=True)

                # Append the DataFrame to the list of DataFrames
                sheets_to_concat.append(excel_df)

            # Concatenate all sheets into a single DataFrame
            appended_data = pd.concat(sheets_to_concat, ignore_index=True)

            # Create a DataFrame from the appended data
            excel_data_frame = pd.DataFrame(appended_data)

            # Print the final DataFrame
            print("excel_data_frame", excel_data_frame)
            all_tags_data = []  # Initialize once before the loop
            if not is_pnl:
                for section in main_sections:
                    all_tags_data = check_nodes_in_excel(
                        section["headers"],
                        section["node_list"],
                        section["ignore_headers"],
                        section["main_dict_node"],
                        section["is_pnl"],
                        all_tags_data
                    )
            else:
                # Extract the list of PnL nodes
                pnl_fields_df = config_df[
                    config_df['Column_Name'].str.contains('financials_pnl_lineitems', case=False, na=False)
                ]
                pnl_fields_list = [
                    row['Node'] for _, row in pnl_fields_df.iterrows()
                    if row['Type'] == 'Straight' and row['Node'] != "Null"
                ]

                # Define the headers and ignore headers for PnL
                pnl_headers = ["profit and loss statement", "income statement", "profit and loss"]
                pnl_ignore_headers = (
                        non_current_assets_headers +
                        current_assets_headers +
                        equity_related_headers +
                        non_current_liabilities_headers +
                        current_liabilities_headers
                )

                # Call the function for PnL fields
                all_tags_data = check_nodes_in_excel(
                    pnl_headers, pnl_fields_list,
                    pnl_ignore_headers, None, is_pnl=True, all_tags_data = None)

    # Save updated output to JSON after processing all sheets
    with open(json_file_path, 'w') as json_file:
        json.dump(output, json_file, indent=4)

    print(f"Updated data saved to {json_file_path}")
    return output, all_tags_data
