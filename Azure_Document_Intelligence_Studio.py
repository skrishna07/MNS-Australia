import pandas as pd
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

def azure_pdf_to_excel_conversion(local_file_path,excel_file_path):
    # Initialize the DocumentAnalysisClient
    endpoint = os.environ.get('azure_form_recognizer_endpoint')
    key = os.environ.get('azure_form_recognier_key')
    document_analysis_client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    # Open the PDF file and analyze it directly
    with open(local_file_path, "rb") as file:
        poller = document_analysis_client.begin_analyze_document("prebuilt-layout", file)
        result = poller.result()

    # Prepare to collect DataFrames for each table
    table_dataframes = []

    for table_idx, table in enumerate(result.tables):
        # Create a list to hold the rows of the table
        table_data = []

        # Fill in the table data row by row
        for cell in table.cells:
            # Ensure we have a list for each row
            while len(table_data) <= cell.row_index:
                table_data.append([None] * table.column_count)  # Fill with None initially

            # Assign the cell content to the correct position in the row
            table_data[cell.row_index][cell.column_index] = cell.content

        # Create a DataFrame from the table data
        df = pd.DataFrame(table_data)

        # Optionally set the first row as the header (if your table has headers)
        df.columns = df.iloc[0]  # Set the first row as the header
        df = df[1:]  # Remove the header row from the data
        df.reset_index(drop=True, inplace=True)  # Reset index

        # Add the DataFrame to the list
        table_dataframes.append(df)

    # Write the DataFrames to an Excel file
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        for i, df in enumerate(table_dataframes):
            df.to_excel(writer, index=False, sheet_name=f'Table_{i + 1}')  # Write each DataFrame to a separate sheet

    # Return the list of DataFrames
    return table_dataframes


# local_file_path = r"C:\Users\Bradsol\Downloads\Druckversion-17-19.pdf"
# excel_file_path = r"C:\Users\Bradsol\Downloads\Druckversion-17-19.xlsx"
# azure_pdf_to_excel_conversion(local_file_path,excel_file_path)
