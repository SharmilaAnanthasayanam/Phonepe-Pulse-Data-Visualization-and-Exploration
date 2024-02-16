import os
import json
import mysql.connector
from dotenv import load_dotenv
import os
load_dotenv()

try:
    #Connecting to SQL
    password= os.getenv("password") 
    mydb = mysql.connector.connect( 
            host="localhost",
            user="root",
            password = password,
            database="phonepe"
        )
    cursor = mydb.cursor()

    # Creating required tables
    def create_table(table_name, *argv):
        '''Input: Takes in Table Name and column names with data type
            Output: creates the table'''
        create_agg_table = f"""CREATE TABLE {table_name}(
                            state VARCHAR(255),
                            year VARCHAR(255),
                            quarter int)"""
        cursor.execute(create_agg_table)
        mydb.commit()
        for arg in argv :
            Alter_query = f"""ALTER TABLE {table_name}
                            ADD {arg}"""
            cursor.execute(Alter_query)
            mydb.commit()
        print(f"{table_name} created successfully")

    create_table("agg_transaction", "transaction_type VARCHAR(255)", "transaction_count BIGINT", "transaction_amount BIGINT")
    create_table("map_transaction", "district VARCHAR(255)", "count BIGINT", "amount BIGINT")
    create_table("top_transaction", "district VARCHAR(255)", "count BIGINT", "amount BIGINT")
    create_table("agg_user", "registered_users BIGINT", "app_opens BIGINT")
    create_table("map_user", "district VARCHAR(255)", "registered_users BIGINT", "app_opens BIGINT")
    create_table("top_user", "district VARCHAR(255)", "registered_users BIGINT")
    create_table("agg_user_device", "brand VARCHAR(255)", "count BIGINT", "percentage BIGINT")
    create_table("top_pincodes","pincode VARCHAR(255)", "count BIGINT", "amount BIGINT")
    create_table("top_user_pincodes", "pincode VARCHAR(255)", "registered_users BIGINT")
        
    #Insert Table function
    def insert_into_table(table_name, *args):
        '''Input: Takes in Table Name and dictionary with column name and value as keys and values respectively
            Output: Inserts the dict data into given table'''
        args = args[0]
        arg_str = ", ".join(args.keys())
        val = list(args.values())
        s_list = ["%s" for i in range(len(args))]
        s_str = ", ".join(s_list)
        insert_query = f"""INSERT INTO {table_name} ({arg_str})
                        VALUES ({s_str})"""
        cursor.execute(insert_query,(val))
        mydb.commit()

    # Extracting and Inserting data into agg_transaction table

    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\aggregated\transaction\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        path_state = path + f"\\{state}"
        year_list = os.listdir(path_state)
        for year in year_list:
            path_year = path_state + f"\\{year}"
            agg_quarter_list = os.listdir(path_year)
            for Quarter in agg_quarter_list:
                path_quarter = path_year + f"\\{Quarter}"
                data = open(path_quarter,'r')
                data = json.load(data)
                quarter = int(Quarter.strip(".json"))
                data = data.get("data")
                transaction_data = data.get("transactionData")
                for transaction_datum in transaction_data:
                    transaction_type = transaction_datum.get("name")
                    payment_instruments = transaction_datum.get("paymentInstruments")
                    for payment_details in payment_instruments:
                        transaction_amount = round(payment_details.get("amount"))
                        transaction_count = payment_details.get("count")
                        insert_into_table("agg_transaction", {"state":state,"year":year,"quarter":quarter, "transaction_type":transaction_type,"transaction_count":transaction_count,"transaction_amount":transaction_amount})
    
    # Extracting and Inserting data into agg_user and agg_user_device
                        
    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\aggregated\user\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        path_state = path + f"\\{state}"
        year_list = os.listdir(path_state)
        for year in year_list:
            path_year = path_state + f"\\{year}"
            agg_quarter_list = os.listdir(path_year)
            for Quarter in agg_quarter_list:
                path_quarter = path_year + f"\\{Quarter}"
                quarter = int(Quarter.strip(".json"))
                data = open(path_quarter,'r')
                data = json.load(data)
                data = data.get('data')
                data_agg = data.get("aggregated")
                registered_users = data_agg.get("registeredUsers")
                app_opens = data_agg.get("appOpens")
                users_by_device = data.get("usersByDevice")
                insert_into_table("agg_user",{"state":state,"year":year,"quarter":quarter,"registered_users":registered_users,"app_opens":app_opens})

                if users_by_device:
                    for users in users_by_device:
                        brand = users.get("brand")
                        count = users.get("count")
                        percentage = users.get("percentage")
                        insert_into_table("agg_user_device",{"state":state,"year":year,"quarter":quarter,"brand":brand, "count":count, "percentage":percentage})
        
    # Extracting and Inserting data into map_transaction table
    
    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\map\transaction\hover\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        path_state = path + f"\\{state}"
        year_list = os.listdir(path_state)
        for year in year_list:
            path_year = path_state + f"\\{year}"
            agg_quarter_list = os.listdir(path_year)
            for Quarter in agg_quarter_list:
                path_quarter = path_year + f"\\{Quarter}"
                quarter = int(Quarter.strip(".json"))
                data = open(path_quarter,'r')
                data = json.load(data)
                data = data.get("data")
                hover_data_list = data.get("hoverDataList")
                for info in hover_data_list:
                    district = info.get("name")
                    metric =  info.get("metric")
                    amount = metric[0].get("amount")
                    count = metric[0].get("count")
                    insert_into_table("map_transaction",{"state":state,"year":year,"quarter":quarter, "district":district, "count":count, "amount":amount} )
    
    # Extracting and Inserting data into top_transaction and top_pincodes table
    
    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\top\transaction\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        path_state = path + f"\\{state}"
        year_list = os.listdir(path_state)
        for year in year_list:
            path_year = path_state + f"\\{year}"
            agg_quarter_list = os.listdir(path_year)
            for Quarter in agg_quarter_list:
                path_quarter = path_year + f"\\{Quarter}"
                quarter = int(Quarter.strip(".json"))
                data = open(path_quarter,'r')
                data = json.load(data)
                data = data.get("data")
                districts = data.get("districts")
                for info in districts:
                    district = info.get("entityName")
                    metric =  info.get("metric")
                    amount = metric.get("amount")
                    count = metric.get("count")
                    insert_into_table("top_transaction",{"state":state,"year":year,"quarter":quarter, "district":district, "count":count, "amount":amount} )
                pincodes = data.get("pincodes")
                for info in pincodes:
                    pincode = info.get("entityName")
                    metric =  info.get("metric")
                    amount = metric.get("amount")
                    count = metric.get("count")
                    insert_into_table("top_pincodes",{"state":state,"year":year,"quarter":quarter, "pincode":pincode, "count":count, "amount":amount} )
        
    # Extracting and Inserting data into map_user
                        
    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\map\user\hover\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        path_state = path + f"\\{state}"
        year_list = os.listdir(path_state)
        for year in year_list:
            path_year = path_state + f"\\{year}"
            agg_quarter_list = os.listdir(path_year)
            for Quarter in agg_quarter_list:
                path_quarter = path_year + f"\\{Quarter}"
                quarter = int(Quarter.strip(".json"))
                data = open(path_quarter,'r')
                data = json.load(data)
                data = data.get('data')
                hover_data = data.get("hoverData")
                districts = hover_data.keys()
                for district in districts:
                    district_info = hover_data.get(district)
                    registered_users = district_info.get("registeredUsers")
                    app_opens = district_info.get("appOpens")
                    insert_into_table("map_user",{"state":state,"year":year,"quarter":quarter,"district":district,"registered_users":registered_users,"app_opens":app_opens})
                
    # Extracting and Inserting data into top_user

    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\top\user\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        path_state = path + f"\\{state}"
        year_list = os.listdir(path_state)
        for year in year_list:
            path_year = path_state + f"\\{year}"
            agg_quarter_list = os.listdir(path_year)
            for Quarter in agg_quarter_list:
                path_quarter = path_year + f"\\{Quarter}"
                quarter = int(Quarter.strip(".json"))
                data = open(path_quarter,'r')
                data = json.load(data)
                data = data.get('data')
                districts = data.get("districts")
                for district_info in districts:
                    district = district_info.get("name")
                    registered_users = district_info.get("registeredUsers")
                    insert_into_table("top_user",{"state":state,"year":year,"quarter":quarter,"district":district,"registered_users":registered_users})
                pincodes = data.get("pincodes")
                for pincodes_info in pincodes:
                    pincode = pincodes_info.get("name")
                    registered_users = pincodes_info.get("registeredUsers")
                    print([pincode,registered_users])
                    insert_into_table("top_user_pincodes",{"state":state, "year":year, "quarter":quarter, "pincode":pincode, "registered_users":registered_users})

except Exception as e:
    print(e)