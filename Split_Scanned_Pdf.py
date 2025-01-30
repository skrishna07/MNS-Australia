import pytesseract
from PyPDF2 import PdfReader, PdfWriter
from pdf2image import convert_from_path
import os
import shutil
import sys

# Set path to Tesseract OCR executable
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

def extract_text_with_ocr(pdf_path, page_number, temp_dir):
    """Converts a PDF page to an image and extracts text using OCR."""
    images = convert_from_path(pdf_path, first_page=page_number + 1, last_page=page_number + 1, dpi=300)
    image_path = os.path.join(temp_dir, f"page_{page_number + 1}.png")
    images[0].save(image_path, "PNG")
    text = pytesseract.image_to_string(image_path)

    # Ensure file exists before attempting to delete
    if os.path.exists(image_path):
        os.remove(image_path)  # Clean up the temporary image
    return text

def count_fields_in_text(text, fields):
    """Counts occurrences of fields in the text."""
    return sum(field.lower() in text.lower() for field in fields)

def is_contents_page(text, contents_keywords):
    """Checks if the page is a contents page based on specific keywords."""
    return any(keyword.lower() in text.lower() for keyword in contents_keywords)

def split_pdf_based_on_headers_and_fields(pdf_path, output_pdf_path, headers, fields, contents_keywords,
                                          pages_to_extract=4):
    # Read the PDF
    reader = PdfReader(pdf_path)
    total_pages = len(reader.pages)

    # Directory to temporarily store images
    temp_dir = "temp_images"
    os.makedirs(temp_dir, exist_ok=True)

    best_page = None
    max_fields_count = 0

    # Loop through each page in the PDF
    for page_number in range(total_pages):
        print("Page_number", page_number + 1)
        # Extract text using OCR
        text = extract_text_with_ocr(pdf_path, page_number, temp_dir)

        # Skip contents pages
        if is_contents_page(text, contents_keywords):
            continue

        # Check if the page contains any header and count fields
        if any(header.lower() in text.lower() for header in headers):
            fields_count = count_fields_in_text(text, fields)
            print("fields_Count : ", fields_count)
            if fields_count > max_fields_count:
                best_page = page_number
                max_fields_count = fields_count

    if best_page is not None:
        # Extract the best page and the next `pages_to_extract - 1` pages
        writer = PdfWriter()
        for i in range(best_page, min(best_page + pages_to_extract, total_pages)):
            writer.add_page(reader.pages[i])

        # Write to the output PDF
        with open(output_pdf_path, "wb") as output_file:
            writer.write(output_file)

        print(
            f"Header and fields found on page {best_page + 1}. Extracted pages {best_page + 1} to {min(best_page + pages_to_extract, total_pages)}.")
        split_success = True
    else:
        print("No relevant pages found.")
        split_success = False

    # Cleanup temporary directory: delete files and remove the directory
    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)  # Recursively remove the directory and its contents
        except Exception as e:
            print(f"Error removing temporary directory: {e}")
    return split_success
