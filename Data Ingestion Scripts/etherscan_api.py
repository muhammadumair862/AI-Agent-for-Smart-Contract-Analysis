import requests
import pandas as pd
from datetime import datetime
from web3 import Web3
import json

# Function to fetch transactions from Etherscan API
def get_transactions(address, api_key, limit=100):
    url = f"https://api.etherscan.io/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": address,
        "startblock": 0,
        "endblock": 99999999,
        "page": 1,
        "offset": limit,  # Number of transactions to fetch
        "sort": "desc",  # Latest transactions first
        "apikey": api_key
    }
    
    response = requests.get(url, params=params)
    # print(response.content)
    if response.status_code != 200:
        print(f"Failed to fetch data: {response.text}")
        return []
    data = response.json()
    if data['status'] != "1":
        print(f"Error: {data['message']}")
        return []
    return data['result']


# Function to process and format the data
def process_transactions(transactions, abi=None):
    processed_data = []
    
    # Create a Web3 instance (dummy provider for ABI decoding)
    w3 = Web3(Web3.HTTPProvider("https://mainnet.infura.io/v3/YOUR_INFURA_PROJECT_ID"))  # Replace with your Infura project URL

    # Parse ABI if provided
    contract = w3.eth.contract(abi=json.loads(abi)) if abi else None

    for tx in transactions:
        # Decode method name and parameters if ABI is available
        method_name = "Unknown"
        decoded_params = "Unknown"

        if abi and tx["input"] != "0x":
            try:
                decoded_function = contract.decode_function_input(tx["input"])
                method_name = decoded_function[0].fn_name
                decoded_params = decoded_function[1]
            except Exception as e:
                print(f"Error decoding input for tx {tx['hash']}: {e}")
        
        processed_data.append({
            "Transaction Hash": tx["hash"],
            "Status": "Success" if tx["isError"] == "0" else "Failed",
            "Method": method_name,
            "Block Number": tx["blockNumber"],
            "Block Hash": tx["blockHash"],
            "Transaction Index": tx["transactionIndex"],
            "DateTime (UTC)": datetime.utcfromtimestamp(int(tx["timeStamp"])).strftime('%Y-%m-%d %H:%M:%S'),
            "From": tx["from"],
            "To": tx["to"],
            "Amount (ETH)": int(tx["value"]) / 10**18,
            "Gas": tx["gas"],
            "Gas Price (ETH)": int(tx["gasPrice"]) / 10**18,
            "Gas Used": tx["gasUsed"],
            "Gas Fee (ETH)": (int(tx["gasPrice"]) * int(tx["gasUsed"])) / 10**18,
            "Cumulative Gas Used": tx["cumulativeGasUsed"],
            "Nonce": tx["nonce"],
            "Contract Address": tx["contractAddress"] if tx["contractAddress"] else "None",
            "Confirmations": tx["confirmations"],
            "Method ID": tx["methodId"],
            "Function Name": tx["functionName"],
        })
    return processed_data


# Main function
def main(address):
    # Replace with your Etherscan API Key
    api_key = "KC21HXQJGBYF8PGTZPYBX92SF6P2R4AX4Q"

    # # Replace with your list of Ethereum addresses
    # addresses = ["0xBB9bc244D798123fDe783fCc1C72d3Bb8C189413"]

    all_data = []
    # for address in addresses:
    print(f"Fetching transactions for: {address}")
    transactions = get_transactions(address, api_key, limit=100)
    if transactions:
        processed_data = process_transactions(transactions)
        all_data.extend(processed_data)

    # Save to CSV or print as DataFrame
    df = pd.DataFrame(all_data)
    # # print(df)
    # df.to_csv("transactions.csv", index=False)
    return df

# if __name__ == "__main__":
#     main()
