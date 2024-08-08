import mysql.connector
from bs4 import BeautifulSoup
from PythonLogging import setup_logging
import logging
from datetime import datetime
import json


def final_table(db_config, registration_no, database_id):
    setup_logging()
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Define the CIN number

        # Execute query to fetch data
        query = f"""
            SELECT 
                lei_status, lei_comments,
                legal_cases_status, legal_cases_comments,
                stock_exchange_status,stock_excahnge_comments,
                credit_rating_status, credit_rating_comments,
                trade_mark_status,trade_mark_comments
            FROM orders
            WHERE registration_no = '{registration_no}' and id = {database_id}
        """

        cursor.execute(query)
        result = cursor.fetchone()

        # Close the database connection
        conn.close()

        # Format the data into a table
        html_table = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        table {{
            border-collapse: collapse;
            width: 60%;
        }}
        th, td {{
            border: 1px solid black;
            padding: 8px;
            text-align: left;
            width: 20%;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr.red {{
            background-color: red;
            color: black;
        }}
    </style>
</head>
<body>
    <table>
        <tr>
            <th>Category</th>
            <th>Status</th>
            <th>Comments</th>
        </tr>
        <tr class="{ 'red' if result[0] in ['N', 'P'] else '' }">
            <td>LEI</td>
            <td>{result[0]}</td>
            <td>{result[1] if result[1] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[2] in ['N', 'P'] else '' }">
            <td>Legal cases</td>
            <td>{result[2]}</td>
            <td>{result[3] if result[3] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[4] in ['N', 'P'] else '' }">
            <td>Stock Exchange</td>
            <td>{result[4]}</td>
            <td>{result[5] if result[5] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[6] in ['N', 'P'] else '' }">
            <td>Credit Ratings</td>
            <td>{result[6]}</td>
            <td>{result[7] if result[7] is not None else ''}</td>
        </tr>
        <tr class="{ 'red' if result[8] in ['N', 'P'] else '' }">
            <td>Trade Mark</td>
            <td>{result[8]}</td>
            <td>{result[9] if result[9] is not None else ''}</td>
        </tr>
    </table>
</body>
</html>
"""

        # Use BeautifulSoup to format the HTML table
        soup = BeautifulSoup(html_table, 'html.parser')
        formatted_table = soup.prettify()

        # Print or use the formatted table
        logging.info(formatted_table)
        # Print or store the table
        return formatted_table
    except Exception as e:
        logging.info(f"Error in fetching final email table {e}")
        return None


def financials_table(db_config, registration_no):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        query = f"select * from financials where registration_no = '{registration_no}' order by year DESC"
        cursor.execute(query)
        results = cursor.fetchall()

        # Create a new BeautifulSoup object
        soup = BeautifulSoup(features="html.parser")

        # Create the table element
        table = soup.new_tag('table', style='width: 60%; border-collapse: collapse;')

        # Create table headers
        headers = ['Year', 'Taxonomy', 'Nature', 'Difference value of Assets', 'Difference value of Liabilities',
                   'PNL Difference']
        header_row = soup.new_tag('tr')
        for header in headers:
            th = soup.new_tag('th', style='border: 1px solid black; padding: 8px;')
            th.string = header
            header_row.append(th)
        table.append(header_row)

        # Populate table with data
        for result in results:
            year = result[6]
            try:
                date_obj = datetime.strptime(year, '%d/%m/%Y')
                year = date_obj.strftime('%Y')
            except:
                pass

            try:
                date_obj = datetime.strptime(year, '%Y-%m-%d')
                year = date_obj.strftime('%Y')
            except:
                pass
            taxonomy = result[10]
            nature = result[8]
            subtotals = result[14]
            pnl_items = result[15]
            subtotals_dict = json.loads(subtotals)
            pnl_dict = json.loads(pnl_items)
            try:
                assets_difference = subtotals_dict['diffrence_value_of_assets']
            except:
                assets_difference = None
            try:
                liabilities_difference = subtotals_dict['diffrence_value_of_liabilities']
            except:
                liabilities_difference = None
            try:
                pnl_difference = pnl_dict['difference_value']
            except:
                pnl_difference = None
            # Create a new row
            row = soup.new_tag('tr')

            # Add data to the row
            data = [year, taxonomy, nature, assets_difference, liabilities_difference, pnl_difference]
            for idx, item in enumerate(data):
                td = soup.new_tag('td', style='border: 1px solid black; padding: 8px;')
                if idx >= 3 and item != 0:
                    td['style'] += 'background-color: red; color: black;'
                else:
                    td['style'] += 'color: black;'
                td.string = str(item)
                row.append(td)

            # Add row to table
            table.append(row)

        # Append table to soup
        soup.append(table)

        # Return the HTML table as a string
        return str(soup)
    except Exception as e:
        print(f"Exception in generating Fianancials Table {e}")
        return None
    