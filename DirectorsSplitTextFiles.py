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


# Function to dynamically detect designations and separate directors based on keywords
def extract_directors_dynamically(text, name_keyword="Name:"):
    # Define main designations with their "Previous" variations
    base_designations = [
        "Director", "Alternate Director", "Secretary"
    ]
    # Generate both current and previous variations for each designation
    designations = [f"Previous {role}" for role in base_designations] + base_designations

    designation_dict = {}
    current_designation = None
    current_entry = []

    for line in text.splitlines():
        # Detect possible designation keywords dynamically, checking "Previous" variants first
        matched_designation = None
        for designation in designations:
            if designation in line:
                matched_designation = designation
                break

        # If a designation is detected, save the current entry and update the designation
        if matched_designation:
            if current_entry and current_designation:
                # Initialize the designation list if it doesn't exist
                if current_designation not in designation_dict:
                    designation_dict[current_designation] = []
                designation_dict[current_designation].append("\n".join(current_entry))
                current_entry = []

            # Set the current designation and ensure it is initialized in the dictionary
            current_designation = matched_designation
            if current_designation not in designation_dict:
                designation_dict[current_designation] = []

        # Detect start of a new director entry within the same designation
        elif name_keyword in line and current_designation:
            # Save the current director entry if it exists
            if current_entry:
                designation_dict[current_designation].append("\n".join(current_entry))
                current_entry = []

        # Continue adding lines to the current entry
        if current_designation:
            current_entry.append(line)

    # Add the last director entry after the loop ends
    if current_entry and current_designation:
        designation_dict[current_designation].append("\n".join(current_entry))

    return designation_dict


# Function to create text files dynamically with designation label at the start of each file
def create_designation_files(designation_dict, folder_path, group_size=10, delimiter="\n\n---\n\n"):
    os.makedirs(folder_path, exist_ok=True)
    for designation, entries in designation_dict.items():
        file_count = len(entries) // group_size
        remainder = len(entries) % group_size
        file_number = 1
        index = 0

        # Create files with groups of entries for each designation
        for _ in range(file_count):
            file_path = os.path.join(folder_path, f'{designation.replace(" ", "_")}_File_{file_number}.txt')
            with open(file_path, 'w') as file:
                file.write(f"{designation}\n\n")  # Add designation label at the top
                file.write(delimiter.join(entries[index:index + group_size]))
            file_number += 1
            index += group_size

        # If there are remaining entries, put them in another file
        if remainder > 0:
            file_path = os.path.join(folder_path, f'{designation.replace(" ", "_")}_File_{file_number}.txt')
            with open(file_path, 'w') as file:
                file.write(f"{designation}\n\n")  # Add designation label at the top
                file.write(delimiter.join(entries[index:index + remainder]))


# Main function
def pdf_to_text_files_dynamically(pdf_path):
    setup_logging()
    errors = []
    try:
        output_company_folder = os.path.dirname(pdf_path)
        designation_text_files_folder = os.path.join(output_company_folder, f"designation_text_files_{today_date}")

        # Extract text from the PDF
        pdf_text = extract_pdf_text(pdf_path)

        # Extract directors and dynamically categorize by designation
        designation_dict = extract_directors_dynamically(pdf_text)

        # Create text files with grouped entries for each designation
        create_designation_files(designation_dict, designation_text_files_folder)

        logging.info(
            f"Text files have been created in the '{designation_text_files_folder}' folder, categorized by designation.")
    except Exception as e:
        logging.error(f"Exception occurred while creating text files {e}")
        tb = traceback.extract_tb(e.__traceback__)
        for frame in tb:
            if frame.filename == __file__:
                errors.append(f"Line {frame.lineno}: {frame.line} - {str(e)}")
        raise Exception('\n'.join(errors))
    else:
        return True, designation_text_files_folder
