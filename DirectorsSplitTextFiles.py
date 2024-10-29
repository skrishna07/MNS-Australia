import os
import PyPDF2
from datetime import datetime
from PythonLogging import setup_logging
import logging
import traceback

# Get today's date in yyyy-mm-dd format
today_date = datetime.today().strftime('%Y-%m-%d')

# Function to extract text from the PDF
def extract_pdf_text(pdf_path):
    with open(pdf_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        pdf_text = ""
        for page in pdf_reader.pages:
            pdf_text += page.extract_text()
    return pdf_text


# Function to extract directors and their details properly
def extract_directors(text, search_keyword):
    directors = []
    current_director = []
    in_director_section = False

    for line in text.splitlines():
        # Detect start of a new director
        if search_keyword in line:
            # If a director is already being processed, store the current one before starting a new one
            if current_director:
                directors.append("\n".join(current_director))
                current_director = []
            in_director_section = True  # Start collecting for a new director

        # Continue collecting details for the current director
        if in_director_section:
            current_director.append(line)

    # Add the last director after loop ends
    if current_director:
        directors.append("\n".join(current_director))

    return directors


# Function to create text files dynamically with spacing between directors
def create_text_files(directors, folder_path, group_size=10, delimiter="\n\n---\n\n"):
    os.makedirs(folder_path, exist_ok=True)
    file_count = len(directors) // group_size
    remainder = len(directors) % group_size
    file_number = 1
    index = 0

    # Create files with groups of directors
    for _ in range(file_count):
        with open(os.path.join(folder_path, f'File_{file_number}.txt'), 'w') as file:
            file.write(delimiter.join(directors[index:index + group_size]))
        file_number += 1
        index += group_size

    # If there are remaining directors, put them in another file
    if remainder > 0:
        with open(os.path.join(folder_path, f'File_{file_number}.txt'), 'w') as file:
            file.write(delimiter.join(directors[index:index + remainder]))


# Define the main function
def pdf_to_text_files(pdf_path, input_type):
    errors = []
    setup_logging()
    try:
        output_company_folder = os.path.dirname(pdf_path)
        if input_type == 'directors':
            director_text_files_folder = os.path.join(output_company_folder, f"director_text_files_{today_date}")
            search_keyword = 'Name:'
        else:
            raise Exception("Invalid Input Type")
        if not os.path.exists(director_text_files_folder):
            os.makedirs(director_text_files_folder)
        # Extract text from the PDF
        pdf_text = extract_pdf_text(pdf_path)

        # Extract directors, ensuring that all details including multiple official positions are grouped properly
        directors = extract_directors(pdf_text, search_keyword)

        # Create text files with groups of directors, with spacing between entries
        create_text_files(directors, director_text_files_folder)

        logging.info(f"Text files have been created in the '{director_text_files_folder}' folder with proper grouping of positions under each director.")
    except Exception as e:
        logging.error(f"Exception occurred while creating text files {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception('\n'.join(errors))
    else:
        return True, director_text_files_folder