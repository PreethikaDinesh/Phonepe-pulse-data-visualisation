import pandas as pd
import os
import json
import mysql.connector
import requests

#path1
path_1 = "G:/phonepe/pulse/data/aggregated/transaction/country/india/state/"
Agg_trans_state_list = os.listdir(path_1)

Agg_trans = {'State': [], 'Year': [], 'Quarter': [], 'Trans_type': [], 'Trans_count': [], 'Trans_amount': []}

for i in Agg_trans_state_list:
    p_i = path_1 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D1 = json.load(Data)

            for z in D1['data']['transactionData']:
                Name = z['name']
                count = z['paymentInstruments'][0]['count']
                amount = z['paymentInstruments'][0]['amount']
                Agg_trans['Trans_type'].append(Name)
                Agg_trans['Trans_count'].append(count)
                Agg_trans['Trans_amount'].append(amount)
                Agg_trans['State'].append(i)
                Agg_trans['Year'].append(j)
                Agg_trans['Quarter'].append(int(k.strip('.json')))
               
df_aggregated_transaction = pd.DataFrame(Agg_trans)


# path2-DATA-AGGREGATED -USER 

path_2 = "G:/phonepe/pulse/data/aggregated/user/country/india/state/"
Agg_user_state_list = os.listdir(path_2)

Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

for i in Agg_user_state_list:
    p_i = path_2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D2 = json.load(Data)

            try:
                for z in D2["data"]["usersByDevice"]:
                    brand_name = z["brand"]
                    count_ = z["count"]
                    percentage = z["percentage"]
                    Agg_user["Brands"].append(brand_name)
                    Agg_user["User_Count"].append(count_)
                    Agg_user["User_Percentage"].append(percentage*100)
                    Agg_user["State"].append(i)
                    Agg_user["Year"].append(j)
                    Agg_user["Quarter"].append(int(k.strip('.json')))
                  
            except:
                pass

df_aggregated_user = pd.DataFrame(Agg_user)

#==============================        =========================================#
#path3- DATA-MAP-TRANSACTION 

path_3 = "G:/phonepe/pulse/data/map/transaction/hover/country/india/state/"
map_trans_state_list = os.listdir(path_3)

map_trans = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Trans_Count': [], 'Trans_Amount': []}

for i in map_trans_state_list:
    p_i = path_3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D3= json.load(Data)

            for z in D3["data"]["hoverDataList"]:
                District = z["name"]
                count = z["metric"][0]["count"]
                amount = z["metric"][0]["amount"]
                map_trans["District"].append(District)
                map_trans["Trans_Count"].append(count)
                map_trans["Trans_Amount"].append(amount)
                map_trans['State'].append(i)
                map_trans['Year'].append(j)
                map_trans['Quarter'].append(int(k.strip('.json')))
                

df_map_transaction = pd.DataFrame(map_trans)


#path4-  DATA-MAP-USER 

path_4 = "G:/phonepe/pulse/data/map/user/hover/country/india/state/"
map_user_state_list = os.listdir(path_4)

map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": [],"Appopens_user":[]}

for i in map_user_state_list:
    p_i = path_4 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D4= json.load(Data)

            for z in D4["data"]["hoverData"].items():
                district = z[0]
                registereduser = z[1]["registeredUsers"]
                AppOpens = z[1]["appOpens"]
                map_user["District"].append(district)
                map_user["Registered_User"].append(registereduser)
                map_user["Appopens_user"].append(AppOpens)
                map_user['State'].append(i)
                map_user['Year'].append(j)
                map_user['Quarter'].append(int(k.strip('.json')))
              
               

df_map_user = pd.DataFrame(map_user)

# path5-DATA-TOP-TRANSACTION 

path_5 = "G:/phonepe/pulse/data/top/transaction/country/india/state/"
top_trans_state_list = os.listdir(path_5)

top_trans = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Trans_count': [], 'Trans_amount': []}

for i in top_trans_state_list:
    p_i = path_5 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D5= json.load(Data)

            for z in D5['data']['pincodes']:
                Name = z['entityName']
                count = z['metric']['count']
                amount = z['metric']['amount']
                top_trans['District_Pincode'].append(Name)
                top_trans['Trans_count'].append(count)
                top_trans['Trans_amount'].append(amount)
                top_trans['State'].append(i)
                top_trans['Year'].append(j)
                top_trans['Quarter'].append(int(k.strip('.json')))
                

df_top_transaction = pd.DataFrame(top_trans)


# path6- DATA -TOP-USER

path_6 = "G:/phonepe/pulse/data/top/user/country/india/state/"
top_user_state_list = os.listdir(path_6)

top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

for i in top_user_state_list:
    p_i = path_6 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D6 = json.load(Data)

            for z in D6['data']['pincodes']:
                Name = z['name']
                registeredUser = z['registeredUsers']
                top_user['District_Pincode'].append(Name)
                top_user['Registered_User'].append(registeredUser)
                top_user['State'].append(i)
                top_user['Year'].append(j)
                top_user['Quarter'].append(int(k.strip('.json')))
               
df_top_user = pd.DataFrame(top_user)



#######################################################################################3
#mysql connection

import mysql.connector

# Define your MySQL connection parameters
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="phonepe"
)
cursor = conn.cursor()
if conn.is_connected():
        cursor = conn.cursor()
# Create Tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS States (
        StateID INT AUTO_INCREMENT PRIMARY KEY,
        StateName VARCHAR(255)
    )
""")
try:
  # Create the 'aggregated_transaction' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS aggregated_transaction (
            id INT AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Trans_type VARCHAR(255),
            Trans_count INT,
            Trans_amount FLOAT
        )
        """
        cursor.execute(create_table_query)
        print("Table 'aggregated_transaction' created successfully.")

        # Read data and insert into the 'aggregated_transaction' table
        path_1 = "G:/phonepe/pulse/data/aggregated/transaction/country/india/state/"
        for state_folder in os.listdir(path_1):
            state_path = os.path.join(path_1, state_folder)
            for year_folder in os.listdir(state_path):
                year_path = os.path.join(state_path, year_folder)
                for quarter_file in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter_file)
                    with open(quarter_path, 'r') as json_file:
                        data = json.load(json_file)
                        for transaction_data in data['data']['transactionData']:
                            trans_type = transaction_data['name']
                            trans_count = transaction_data['paymentInstruments'][0]['count']
                            trans_amount = transaction_data['paymentInstruments'][0]['amount']
                            state = state_folder
                            year = int(year_folder)
                            quarter = int(quarter_file.split('.')[0])
                            insert_query = """
                            INSERT INTO aggregated_transaction (State, Year, Quarter, Trans_type, Trans_count, Trans_amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (state, year, quarter, trans_type, trans_count, trans_amount))
                            conn.commit()
        print("Data inserted into 'aggregated_transaction' table successfully.")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))
try:
      # Create the 'aggregated_user' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS aggregated_user (
            id INT AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            Brands VARCHAR(255),
            User_Count INT,
            User_Percentage FLOAT
        )
        """
        cursor.execute(create_table_query)
        print("Table 'aggregated_user' created successfully.")

        # Read data and insert into the 'aggregated_user' table
        path_2 = "G:/phonepe/pulse/data/aggregated/user/country/india/state/"
        for state_folder in os.listdir(path_2):
            state_path = os.path.join(path_2, state_folder)
            for year_folder in os.listdir(state_path):
                year_path = os.path.join(state_path, year_folder)
                for quarter_file in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter_file)
                    with open(quarter_path, 'r') as json_file:
                        data = json.load(json_file)
                        try:
                            for device_data in data['data']['usersByDevice']:
                                brand_name = device_data['brand']
                                count = device_data['count']
                                percentage = device_data['percentage'] * 100
                                insert_query = """
                                INSERT INTO aggregated_user (State, Year, Quarter, Brands, User_Count, User_Percentage)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """
                                cursor.execute(insert_query, (state_folder, int(year_folder), int(quarter_file.split('.')[0]), brand_name, count, percentage))
                                conn.commit()
                        except:
                            pass
        print("Data inserted into 'aggregated_user' table successfully.")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))
try:
     # Create the 'map_transaction' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS map_transaction (
            id INT AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District VARCHAR(255),
            Trans_Count INT,
            Trans_Amount FLOAT
        )
        """
        cursor.execute(create_table_query)
        print("Table 'map_transaction' created successfully.")

        # Read data and insert into the 'map_transaction' table
        path_3 = "G:/phonepe/pulse/data/map/transaction/hover/country/india/state/"
        for state_folder in os.listdir(path_3):
            state_path = os.path.join(path_3, state_folder)
            for year_folder in os.listdir(state_path):
                year_path = os.path.join(state_path, year_folder)
                for quarter_file in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter_file)
                    with open(quarter_path, 'r') as json_file:
                        data = json.load(json_file)
                        for hover_data in data["data"]["hoverDataList"]:
                            district = hover_data["name"]
                            count = hover_data["metric"][0]["count"]
                            amount = hover_data["metric"][0]["amount"]
                            insert_query = """
                            INSERT INTO map_transaction (State, Year, Quarter, District, Trans_Count, Trans_Amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (state_folder, int(year_folder), int(quarter_file.split('.')[0]), district, count, amount))
                            conn.commit()
        print("Data inserted into 'map_transaction' table successfully.")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))
try:
        # Create the 'map_user' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS map_user (
            id INT AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District VARCHAR(255),
            Registered_User INT,
            Appopens_user INT
        )
        """
        cursor.execute(create_table_query)
        print("Table 'map_user' created successfully.")

        # Read data and insert into the 'map_user' table
        path_4 = "G:/phonepe/pulse/data/map/user/hover/country/india/state/"
        for state_folder in os.listdir(path_4):
            state_path = os.path.join(path_4, state_folder)
            for year_folder in os.listdir(state_path):
                year_path = os.path.join(state_path, year_folder)
                for quarter_file in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter_file)
                    with open(quarter_path, 'r') as json_file:
                        data = json.load(json_file)
                        for district, values in data["data"]["hoverData"].items():
                            registered_users = values["registeredUsers"]
                            app_opens = values["appOpens"]
                            insert_query = """
                            INSERT INTO map_user (State, Year, Quarter, District, Registered_User, Appopens_user)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (state_folder, int(year_folder), int(quarter_file.split('.')[0]), district, registered_users, app_opens))
                            conn.commit()
        print("Data inserted into 'map_user' table successfully.")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))
try:
     # Create the 'top_transaction' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS top_transaction (
            id INT AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District_Pincode VARCHAR(255),
            Trans_count INT,
            Trans_amount FLOAT
        )
        """
        cursor.execute(create_table_query)
        print("Table 'top_transaction' created successfully.")

        # Read data and insert into the 'top_transaction' table
        path_5 = "G:/phonepe/pulse/data/top/transaction/country/india/state/"
        for state_folder in os.listdir(path_5):
            state_path = os.path.join(path_5, state_folder)
            for year_folder in os.listdir(state_path):
                year_path = os.path.join(state_path, year_folder)
                for quarter_file in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter_file)
                    with open(quarter_path, 'r') as json_file:
                        data = json.load(json_file)
                        for pincode_data in data['data']['pincodes']:
                            district_pincode = pincode_data['entityName']
                            count = pincode_data['metric']['count']
                            amount = pincode_data['metric']['amount']
                            insert_query = """
                            INSERT INTO top_transaction (State, Year, Quarter, District_Pincode, Trans_count, Trans_amount)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (state_folder, int(year_folder), int(quarter_file.split('.')[0]), district_pincode, count, amount))
                            conn.commit()
        print("Data inserted into 'top_transaction' table successfully.")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))
try:
    # Create the 'top_user' table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS top_user (
            id INT AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District_Pincode VARCHAR(255),
            Registered_User INT
        )
        """
        cursor.execute(create_table_query)
        print("Table 'top_user' created successfully.")

        # Read data and insert into the 'top_user' table
        path_6 = "G:/phonepe/pulse/data/top/user/country/india/state/"
        for state_folder in os.listdir(path_6):
            state_path = os.path.join(path_6, state_folder)
            for year_folder in os.listdir(state_path):
                year_path = os.path.join(state_path, year_folder)
                for quarter_file in os.listdir(year_path):
                    quarter_path = os.path.join(year_path, quarter_file)
                    with open(quarter_path, 'r') as json_file:
                        data = json.load(json_file)
                        for pincode_data in data['data']['pincodes']:
                            district_pincode = pincode_data['name']
                            registered_users = pincode_data['registeredUsers']
                            insert_query = """
                            INSERT INTO top_user (State, Year, Quarter, District_Pincode, Registered_User)
                            VALUES (%s, %s, %s, %s, %s)
                            """
                            cursor.execute(insert_query, (state_folder, int(year_folder), int(quarter_file.split('.')[0]), district_pincode, registered_users))
                            conn.commit()
        print("Data inserted into 'top_user' table successfully.")

except mysql.connector.Error as error:
    print("Failed to insert record into MySQL table {}".format(error))


finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection is closed.")

