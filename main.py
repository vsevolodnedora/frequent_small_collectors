import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import urllib.request
import json

class UpdateDutchTTFfromINGMARKETS:
    """ https://www.ingmarkets.de/markte/NLICE0000006 """
    @staticmethod
    def extract_props_data(html_content):
        # Parse the HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all elements that have a 'props' attribute
        elements_with_props = soup.find_all(attrs={"props": True})

        # List to hold all props dictionaries
        all_props = []

        # Iterate through found elements and extract 'props' data
        for element in elements_with_props:
            props_content = element['props']
            # Convert HTML entities to normal characters and parse JSON
            try:
                props_dict = json.loads(props_content.replace('&quot;', '"'))
                all_props.append(props_dict)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

        return all_props
    @staticmethod
    def update_csv_with_new_date(file_path, new_date, new_price):
        # Load the CSV file into a pandas DataFrame
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"Creating original dataframe on {new_date}")
            # If the file does not exist, create a new DataFrame
            df = pd.DataFrame(columns=['date', 'price'])

        # Ensure that the 'date' column is treated as datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Check if the new date is later than the last date in the DataFrame
        if not df.empty and pd.to_datetime(new_date) <= df['date'].max():
            print(f"The new date is not later than the last date in the DataFrame: {new_date}")
            return

        # Append the new date and price
        # Create a new DataFrame for the new row
        new_row = pd.DataFrame({'date': [pd.to_datetime(new_date)], 'price': [new_price]})

        # Concatenate the new row with the existing DataFrame
        df = pd.concat([df, new_row], ignore_index=True)

        # Save the updated DataFrame back to the CSV file
        df.to_csv(file_path, index=False)
        print(f"New row added: {new_row}")

    @staticmethod
    def update():

        url = "https://www.ingmarkets.de/markte/NLICE0000006"
        html_content = urllib.request.urlopen(url).read()
        extracted_props = UpdateDutchTTFfromINGMARKETS.extract_props_data(html_content)
        found_datetime = extracted_props[1]['value'][1]
        curr_price = extracted_props[3]['value'][1] # euros

        UpdateDutchTTFfromINGMARKETS.update_csv_with_new_date(
            './data/ingmarkets_dutch_ttf_prices.csv', found_datetime, curr_price
        )

if __name__ == '__main__':
    # update Dutch TTF (gas price) using data from www.ingmarkets.de
    UpdateDutchTTFfromINGMARKETS.update()
