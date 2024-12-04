import pandas as pd
from pymongo import MongoClient
from etherscan_api import main

# MongoDB connection setup
def get_mongodb_client(uri="mongodb://localhost:27017/"):
    try:
        client = MongoClient(uri)
        print("Connected to MongoDB successfully!")
        return client
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return None

# Function to store data in MongoDB
def store_in_mongodb(data, database_name="smartcontract_db", collection_name="transaction_tb", uri="mongodb://localhost:27017/"):
    client = get_mongodb_client(uri)
    if not client:
        return

    try:
        db = client[database_name]  # Access the database
        collection = db[collection_name]  # Access the collection
        
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to list of dictionaries
            data = data.to_dict("records")

        # Insert data into the collection
        if isinstance(data, list):
            collection.insert_many(data)
            print(f"Inserted {len(data)} records into MongoDB.")
        else:
            collection.insert_one(data)
            print("Inserted one record into MongoDB.")
    except Exception as e:
        print(f"Failed to insert data into MongoDB: {e}")
    finally:
        client.close()

# Function to fetch and process transactions for addresses in a CSV file
def process_csv_and_store(csv_file, api_key):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Ensure the 'Address' column exists
    if "Address" not in df.columns:
        print("Error: The CSV file must contain an 'Address' column.")
        return
    print(df)
    for index, row in df.iterrows():
        address = row["Address"]
        print(f"Fetching transactions for: {address}")
        # transactions = get_transactions(address, api_key, limit=100)
        # if transactions:
        #     processed_data = process_transactions(transactions)
        #     all_data.extend(processed_data)
        df_data = main(address)
    

        # If there is data, store it in MongoDB
        if len(df_data)>0:
            print("Sample Data:")
            print(df_data.head())  # Optional: Print to verify data
            store_in_mongodb(df_data)

# Main function
def mongo_main():
    # Replace with your Etherscan API Key
    api_key = "KC21HXQJGBYF8PGTZPYBX92SF6P2R4AX4Q"

    # Path to your VerifiedContract.csv file
    csv_file = "./Data/VerifiedContract.csv"
    
    # Process CSV and store data in MongoDB
    process_csv_and_store(csv_file, api_key)

if __name__ == "__main__":
    mongo_main()
    # get_mongodb_client()
