import mysql.connector
from bs4 import BeautifulSoup
from PythonLogging import setup_logging
import logging
import json
from DatabaseQueries import get_db_credentials
from ReadExcelConfig import create_main_config_dictionary


def process_tags(finance_new_tags, pnl_new_tags):
    logging.info("Processing tags...")

    # Dynamically extract asset and liability tags by looking for categories
    asset_liability_tags = ["Non-current assets", "Current assets", "Non-current liabilities", "Current liabilities"]

    # Create empty lists to store asset and liability tags
    assets_tags = []
    liabilities_tags = []

    # Loop through finance_new_tags to identify asset and liability tags dynamically
    for tag_dict in finance_new_tags:
        for tag_name in asset_liability_tags:
            if tag_name in tag_dict:
                # Classify as asset or liability
                if "assets" in tag_name.lower():
                    assets_tags.append(tag_dict)
                elif "liabilities" in tag_name.lower():
                    liabilities_tags.append(tag_dict)

    logging.debug(f"Assets tags: {assets_tags}")
    logging.debug(f"Liabilities tags: {liabilities_tags}")

    # Extract equity_tags for tags that contain 'EQUITY'
    equity_tags = [tag for tag in finance_new_tags if 'EQUITY' in tag]
    logging.debug(f"Equity tags: {equity_tags}")

    # Process pnl_new_tags to handle each value individually (no grouping)
    processed_pnl_new_tags = []
    for pnl_tag in pnl_new_tags:
        pnl_data = pnl_tag.get("PnL", [])
        processed_data = []

        # Add each value individually without grouping
        for value in pnl_data:
            processed_data.append(value)

        processed_pnl_new_tags.append({"PnL": processed_data})

    logging.debug(f"Processed PnL tags: {processed_pnl_new_tags}")

    return assets_tags, liabilities_tags, equity_tags, processed_pnl_new_tags


def new_tags_table(db_config, registration_no, database_id):
    setup_logging()
    logging.info("Starting final_table function...")

    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        logging.info("Connected to the database.")

        # Fetch finance_new_tags and pnl_new_tags from the database
        tags_query = """
            SELECT finance_new_tags, pnl_new_tags
            FROM documents
            WHERE registration_no = %s AND id = %s
        """
        cursor.execute(tags_query, (registration_no, database_id))
        tags_result = cursor.fetchone()

        if tags_result:
            logging.info("Tags fetched from the database successfully.")

            finance_new_tags = json.loads(tags_result[0])
            pnl_new_tags = json.loads(tags_result[1])

            # Process tags dynamically
            assets_tags, liabilities_tags, equity_tags, processed_pnl_new_tags = process_tags(finance_new_tags,
                                                                                              pnl_new_tags)

            # Log the results in the desired compact format
            logging.info(f"Assets Tags: {assets_tags}")
            logging.info(f"Liabilities Tags: {liabilities_tags}")
            logging.info(f"Equity Tags: {equity_tags}")
            logging.info(f"Processed PnL Tags: {processed_pnl_new_tags}")

            # Generate formatted HTML table
            full_html = f"""
            <html>
            <body>
                <h3>Assets</h3>
                <h5>Non-current Assets</h5>
                <pre>{json.dumps([tag for tag in assets_tags if 'Non-current assets' in tag])}</pre>
                <h5>Current Assets</h5>
                <pre>{json.dumps([tag for tag in assets_tags if 'Current assets' in tag])}</pre>

                <h3>Liabilities</h3>
                <h5>Non-current Liabilities</h5>
                <pre>{json.dumps([tag for tag in liabilities_tags if 'Non-current liabilities' in tag])}</pre>
                <h5>Current Liabilities</h5>
                <pre>{json.dumps([tag for tag in liabilities_tags if 'Current liabilities' in tag])}</pre>

                <h5>Equity</h5>
                <pre>{json.dumps(equity_tags)}</pre>
                <h3>PnL</h3>
                <pre>{json.dumps(processed_pnl_new_tags)}</pre>
            </body>
            </html>
            """

            # Format the HTML using BeautifulSoup
            soup = BeautifulSoup(full_html, "html.parser")
            formatted_table = soup.prettify()

            # Log or return the final HTML
            logging.info("Final HTML table generated successfully.")
            return formatted_table
        else:
            logging.error("No tags found for the provided registration number and database ID.")
            return "No new tags found"

    except Exception as e:
        # Log any errors that occur during execution
        logging.error(f"Error in fetching final email table: {e}")
        return None