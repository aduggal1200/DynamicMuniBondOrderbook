import pandas as pd
import numpy as np
from tqdm import tqdm

def load_data(file_name: str) -> pd.DataFrame:
    """Load data from a CSV file and process the timestamp column.

    Parameters:
    - file_name: The path to the CSV file.

    Returns:
    - A DataFrame with processed 'time_stamp'.
    """
    df = pd.read_csv(file_name)
    df['time_stamp'] = df['time_stamp'].str.split().str[1]
    return df

def initialize_new_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Initialize new columns for storing top offers in the DataFrame.

    Parameters:
    - df: Original DataFrame.

    Returns:
    - DataFrame with new columns initialized.
    """
    new_columns = [
        f"offerprice{i+1}" for i in range(10)
    ] + [
        f"offersize{i+1}" for i in range(10)
    ] + [
        f"offerdealer{i+1}" for i in range(10)
    ] + [
        f"offertime{i+1}" for i in range(10)
    ]

    for column in new_columns:
        df[column] = np.nan

    return df

def process_grouped_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process the grouped data to populate top offers in the main DataFrame.

    Parameters:
    - df: DataFrame with initialized columns.

    Returns:
    - Updated DataFrame with top offers.
    """
    grouped = df.groupby('cusip')
    prev_cusip = None

    # Iterate through each grouped CUSIP to process its rows
    for cusip, group in tqdm(grouped):
        all_offers = []

        # Iterate through each row in the grouped data
        for index, row in group.iterrows():

            # Process rows of type 'OFFERING'
            if row['order_type'] == 'OFFERING':
                new_offer = {
                    'price': row['price'],
                    'size': row['amount'],
                    'dealer': row['company_id'],
                    'identifier': row['identifier'],
                    'index': index,
                    'time_stamp': row['time_stamp']
                }

                # Check and remove existing offers with the same identifier
                identifiers_all = [offer['identifier'] for offer in all_offers]
                if row['identifier'] in identifiers_all:
                    position = identifiers_all.index(row['identifier'])
                    del all_offers[position]

                # If action is 'DELETE', update DataFrame with top offers and skip adding this offer
                if row['action'] == 'DELETE':
                    top_offers = sorted(all_offers, key=lambda k: (k['price'], k['index']))[:10]

                    for i in range(1, 11):
                        df.loc[index, f"offerprice{i}"] = np.nan
                        df.loc[index, f"offersize{i}"] = np.nan
                        df.loc[index, f"offerdealer{i}"] = np.nan
                        df.loc[index, f"offertime{i}"] = np.nan

                    # Update with current top offers
                    for i, offer in enumerate(top_offers):
                        df.loc[index, f"offerprice{i+1}"] = offer['price']
                        df.loc[index, f"offersize{i+1}"] = offer['size']
                        df.loc[index, f"offerdealer{i+1}"] = offer['dealer']
                        df.loc[index, f"offertime{i+1}"] = offer['time_stamp']
                    continue

                # Add the new offer to the list
                all_offers.append(new_offer)

                # Update the row with the new top offers
                top_offers = sorted(all_offers, key=lambda k: (k['price'], k['index']))[:10]

                for i, offer in enumerate(top_offers):
                    df.loc[index, f"offerprice{i+1}"] = offer['price']
                    df.loc[index, f"offersize{i+1}"] = offer['size']
                    df.loc[index, f"offerdealer{i+1}"] = offer['dealer']
                    df.loc[index, f"offertime{i+1}"] = offer['time_stamp']

            # Process other types of orders
            elif row['order_type'] in ['BID WANTED', 'BW BID', 'SITUATION BID', 'DEALER', 'PURCHASE', 'SALE']:

                if cusip != prev_cusip and row['order_type'] == 'PURCHASE':
                    for i in range(1, 11):
                        df.loc[index, f"offerprice{i}"] = np.nan
                        df.loc[index, f"offersize{i}"] = np.nan
                        df.loc[index, f"offerdealer{i}"] = np.nan
                        df.loc[index, f"offertime{i}"] = np.nan
                else:
                    # Copy offers from the previous row
                    if index > 0:
                        for i in range(1, 11):
                            df.loc[index, f"offerprice{i}"] = df.loc[index-1, f"offerprice{i}"]
                            df.loc[index, f"offersize{i}"] = df.loc[index-1, f"offersize{i}"]
                            df.loc[index, f"offerdealer{i}"] = df.loc[index-1, f"offerdealer{i}"]
                            df.loc[index, f"offertime{i}"] = df.loc[index-1, f"offertime{i}"]

            prev_cusip = cusip

    return df

def filter_and_save(df: pd.DataFrame, output_file: str):
    """Filter the DataFrame for a specific company and save to a CSV file.

    Parameters:
    - df: The DataFrame to filter and save.
    - output_file: The path to the output CSV file.
    """
    df = df[df['company_id'] == 99]
    columns_to_drop = [
        'action', 'firm_time', 'sharp_time', 'identifier',
        'link_identifier', 'settlement_date', 'status', 'company_id'
    ]
    df = df.drop(columns=columns_to_drop)
    df.to_csv(output_file, index=False)

def main():
    """Main execution function."""
    data_file = 'INSERT YOUR DATA FILE.csv'
    output_file = 'processed_data.csv'

    df = load_data(data_file)
    df = initialize_new_columns(df)
    df = process_grouped_data(df)
    filter_and_save(df, output_file)

if __name__ == "__main__":
    main()
