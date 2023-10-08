import json
import csv
import requests
import pandas as pd

#Read the CSV files into pandas DataFrames
df_a = pd.read_csv('File_A.csv')
df_b = pd.read_csv('File_B.csv')

#Merge the DataFrames using the 'user_id' column as the key
merged_df = pd.merge(df_a, df_b, on='user_id', how='inner')

#Define a function that converts the JSON response to a user matrix
def json_to_user_matrix(api_response_json):
    try:

        # Verify if the key 'users' is present in the JSON file
        if 'users' in data:
            users = data['users']

            # Create a list of tuples with the fields 'email', 'first_name' and 'last_name'
            user_list = [(user['email'], user.get('first_name', ''), user.get('last_name', ''), user.get('uid','')) for user in users]

            return user_list
        else:
            # Return an empty list if the key 'users' is not in the JSON response
            return []

    except json.JSONDecodeError as e:
        print(f"Error decoding the JSON response: {str(e)}")
        return []

#API data
domain = 'sandbox.piano.io/api/v3'
aid = 'o1sRRZSLlw'
api_token = 'xeYjNEhmutkgkqCZyhBn6DErVntAKDx30FqFOS6D'
endpoint = 'publisher/user/list'

# Assemble the API URL
url = f'https://{domain}/{endpoint}?aid={aid}&api_token={api_token}'

try:
    #Make a POST request to the API
    response = requests.post(url)

    #Verify if the request was successful (status code 200)
    if response.status_code == 200:
        # Process the API response
        data = response.json()
        print("API response:")
        print(data)
    else:
        print(f"Request error. Status code: {response.status_code}")

except requests.exceptions.RequestException as e:
    print(f"Error connecting to the API: {str(e)}")

#Call the json_to_user_matrix function with the API response 
user_matrix = json_to_user_matrix(data)

#Print the user matrix returned by the API
print("User Matrix:")
for user in user_matrix:
    print(user)

def update_uids_in_merged_df(merged_df, user_matrix):

    #Iterate through the rows of the merged_df dataframe and update the wrong user ids
    for index, row in merged_df.iterrows():
      for user_row in user_matrix:
        if user_row[0]==row['email'] and user_row[3]!=row['user_id']:
          merged_df.at[index, 'user_id'] = user_row[3]

#Print the merged matrix
print("Merged Matrix:")
print(merged_df)

update_uids_in_merged_df(merged_df, user_matrix)

#Print the updated merged matrix
print("Updated User Matrix:")
print(merged_df)

#Save the merged and updated DataFrame to a new CSV file
merged_df.to_csv('merged_file.csv', index=False)

