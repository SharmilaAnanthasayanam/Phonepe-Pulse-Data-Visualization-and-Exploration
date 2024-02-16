import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os
import plotly.express as px
load_dotenv()
import streamlit as st
from streamlit_plotly_events import plotly_events 
import matplotlib.pyplot as plt


try:
    #Connecting to SQL
    password= os.getenv("password")
    mydb =  mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = password,
        database = "phonepe"
    )
    cursor = mydb.cursor()

    # Title Configuration
    st.header("Phonepe Pulse Data Visualization and Exploration",divider='rainbow')
    st.text("")
    st.text("")

    # Dropdowns for selecting Transactions/Users, year and quarter
    col1, col2, col3 = st.columns(3)

    with col1:
        option = st.selectbox("Choose the desired option",("Transactions", "Users"))
    with col2:
        year_option = st.selectbox("Choose the desired year",("2018", "2019","2020", "2021", "2022", "2023"))
    with col3:
        quarter_list = ["Q1 (Jan - Mar)", "Q2 (Apr - Jun)", "Q3 (Jul - Sep)", "Q4 (Oct - Dec)" ]
        quarter_option = st.selectbox("Choose the quarter",(quarter_list))
        quarter_option = quarter_list.index(quarter_option) + 1

    #Creating and Fetching data for Transaction dict based on the user selection to plot the map 
    agg_transaction_dict = {'State':[], 'Year':[],'Quarter':[],'Transaction_count':[], 'Transaction_amount':[], 'Avg_Transaction_Value':[]}
    agg_users_dict = {'State':[], 'Year':[], 'Quarter':[], 'Registered Users':[], 'App Opens':[]}
    path = r"C:\Users\Public\Documents\Projects\Phoenpe_Project\pulse\data\aggregated\transaction\country\india\state"
    state_list = os.listdir(path)
    for state in state_list:
        if option == "Transactions":
            SELECT_QUERY = """select state, sum(transaction_count), sum(transaction_amount), round(sum(transaction_amount)/sum(transaction_count))
                            from agg_transaction
                            where state = %s and year = %s and quarter = %s;"""
            cursor.execute(SELECT_QUERY,([state, year_option, quarter_option]))
            val = cursor.fetchall()
            agg_transaction_dict["State"].append(state)
            agg_transaction_dict["Year"].append(year_option)
            agg_transaction_dict["Quarter"].append(quarter_option)
            agg_transaction_dict["Transaction_count"].append(val[0][1])
            agg_transaction_dict["Transaction_amount"].append(val[0][2])
            agg_transaction_dict["Avg_Transaction_Value"].append(val[0][3])
            df = pd.DataFrame(agg_transaction_dict)
        else:
            SELECT_QUERY = """select state, registered_users, app_opens
                            from agg_user
                            where state = %s and year = %s and quarter = %s;"""
            cursor.execute(SELECT_QUERY, ([state, year_option, quarter_option]))
            val = cursor.fetchall()
            agg_users_dict["State"].append(state)
            agg_users_dict["Year"].append(year_option)
            agg_users_dict["Quarter"].append(quarter_option)
            agg_users_dict["Registered Users"].append(val[0][1])
            agg_users_dict["App Opens"].append(val[0][2])
            df = pd.DataFrame(agg_users_dict)

    match_list = ["Andaman & Nicobar","Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chandigarh",
                "Chhattisgarh","Dadra and Nagar Haveli and Daman and Diu","Delhi","Goa","Gujarat",
                "Haryana","Himachal Pradesh","Jammu & Kashmir","Jharkhand","Karnataka","Kerala","Ladakh",
                "Lakshadweep","Madhya Pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland",
                "Odisha","Puducherry","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura",
                "Uttar Pradesh","Uttarakhand","West Bengal"]
    df["State"] = match_list

    def map(color, hover_data):
        '''Takes color(Column value based on which color differentiation occurs) and hover data'''
        fig = px.choropleth(
                df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='State',
                color=color,
                color_continuous_scale='Viridis',
                hover_data = hover_data
                )
        fig.update_geos(fitbounds="locations",  visible=False)
        fig.update_layout(width=700, height=500 )
        selected_points = plotly_events(fig, click_event=True, hover_event=False)
        return selected_points

    #Plotting map
    if option == "Transactions":
        df["Transaction_amount"]  = df["Transaction_amount"].astype("float")
        df["Transaction_count"] = df["Transaction_count"].astype("float")
        df["Avg_Transaction_Value"] = df["Avg_Transaction_Value"].astype("float")
        selected_points = map("Transaction_amount",["State","Transaction_count","Avg_Transaction_Value"])
    else:
        df["Registered Users"] = df["Registered Users"].astype("float")
        df["App Opens"] = df["App Opens"].astype("float")
        selected_points = map("Registered Users",["State","Registered Users","App Opens"])

    if selected_points:
        match_state = match_list[selected_points[0].get("pointNumber")]
        state = state_list[match_list.index(match_state)]

        #Splitting the data to be displayed based on the Transactions/Users option
        if option == "Transactions":
            #Displaying Top 10 districts on Transaction amount in selected state .
            SELECT_QUERY = """select district, count, amount from top_transaction
                                where state = %s and year = %s and quarter = %s;"""
            cursor.execute(SELECT_QUERY,([state, year_option, quarter_option]))
            val = cursor.fetchall()
            df = pd.DataFrame(val)
            df.columns = ["District","Count", "Amount"]
            st.subheader(f''':green[**Top 10 Districts in {match_state}**]''')
            st.dataframe(df)
            
            #Displaying graph for Transaction amount of selected state over years.
            QUERY = """select state, year, sum(transaction_amount)
                    from agg_transaction
                    where state = %s
                    group by year;"""
            cursor.execute(QUERY,([state]))
            val = cursor.fetchall()
            state_df = pd.DataFrame(val)
            state_df.columns = ["state","year","Transaction Amount"]
            state_df["Transaction Amount"] = state_df["Transaction Amount"].astype("float")
            fig = px.bar(state_df, x='year', y='Transaction Amount')
            st.subheader(f''':green[**Transaction Amount of {match_state} over years**]''')
            st.plotly_chart(fig)

            # Displaying District and Area info based on the selection
            col1, col2 = st.columns(2)
            with col1:
                district_list_query = """select distinct(district) from map_transaction 
                                    where state = %s"""
                cursor.execute(district_list_query, ([state]) )
                values = cursor.fetchall()
                district_list = [""]
                for item in values:
                    district_list.append(item[0])

                st.subheader(''':green[District Info]''')
                district = st.selectbox("Choose the desired district",(district_list))

                if district:
                    QUERY = """select count, amount from map_transaction
                            where district = %s and year = %s and quarter = %s;"""
                    cursor.execute(QUERY,([district,year_option,quarter_option]))
                    values = cursor.fetchall()
                    count = values[0][0]
                    amount =  values[0][1]
                    avg_transaction = round(amount/count)
                    st.markdown(f''':blue[**Transactions in {year_option} Quarter {quarter_option}**]   :   **{count}**''')
                    st.markdown(f''':blue[**Transaction value in {year_option} Quarter {quarter_option}**]  :  **{amount}**''')
                    st.markdown(f''':blue[**Avg Transaction value in {year_option} Quarter {quarter_option}**]  :  **{avg_transaction}**''')
            with col2:
                QUERY = """select distinct(pincode) from top_transaction_pincodes
                            where state = %s;"""
                cursor.execute(QUERY,([state]))
                val = cursor.fetchall()
                pincode_list = [""]
                for item in val:
                    pincode_list.append(item[0])
                st.subheader(f''':green[Area Info]''')
                pincode = st.selectbox("Choose the desired pincode",(pincode_list))

                if pincode:
                    QUERY = """select pincode, count, amount from top_transaction_pincodes
                            where pincode = %s and year = %s and quarter = %s;"""
                    cursor.execute(QUERY, ([pincode, year_option, quarter_option]))
                    val = cursor.fetchall()
                    if val:
                        count = val[0][1]
                        amount = val[0][2]
                        avg_transaction = round(amount/count)
                        st.markdown(f''':blue[**Transactions in {year_option} Quarter {quarter_option}**]   :   **{count}**''')
                        st.markdown(f''':blue[**Transaction value in {year_option} Quarter {quarter_option}**]  :  **{amount}**''')
                        st.markdown(f''':blue[**Avg Transaction value in {year_option} Quarter {quarter_option}**]  :  **{avg_transaction}**''')
                    else:
                        st.markdown(f":orange[Value not found for the chosen pincode in the year {year_option} quarter {quarter_option}]")

            #Displaying Transaction amount based on the selected Transaction Types in the dropdown.
            Transaction_types = ["Recharge & bill payments","Peer-to-peer payments","Merchant payments",
                                "Financial Services","Others"]
            st.subheader(f''':green[Transaction Type Filter]''')
            Transaction_type = st.multiselect("Choose the desired Transaction Type",(Transaction_types))
            Transaction_type_text = "',  '".join(Transaction_type)
            Transaction_type_text = "'"+Transaction_type_text+"'"
            QUERY = f"""SELECT transaction_type, transaction_amount, transaction_count
                    FROM agg_transaction
                    WHERE transaction_type IN ({Transaction_type_text}) AND state = "{state}" AND year = {year_option} AND quarter = {quarter_option};
                    """
            cursor.execute(QUERY)
            val = cursor.fetchall()
            for i in val:
                Transaction_type = i[0]
                transaction_amount = i[1]
                transaction_count = i[2]
                st.markdown(f":orange[**{Transaction_type}**]")
                st.markdown(f''':blue[**Transactions in {year_option} Quarter {quarter_option}**]   :   **{transaction_count}**''')
                st.markdown(f''':blue[**Transaction value in {year_option} Quarter {quarter_option}**]  :  **{transaction_amount}**''')
        else:
            # Top 10 districts based on Registered Users
            QUERY = """select district, registered_users from top_user
                        where state = %s and year = %s and quarter = %s;"""
            cursor.execute(QUERY,([state, year_option, quarter_option]))
            val = cursor.fetchall()
            df = pd.DataFrame(val)
            df.columns = ["District", "Registered Users"]
            st.subheader(f":green[Top 10 Districts in {match_state}]")
            st.dataframe(df)

            # Displaying Registered Users count for the selected state over years.
            QUERY = """select state, year, sum(registered_users)
                    from agg_user
                    where state = %s
                    group by year;"""
            cursor.execute(QUERY,([state]))
            val = cursor.fetchall()
            state_df = pd.DataFrame(val)
            state_df.columns = ["State", "Year", "Registered Users"]
            state_df["Registered Users"] = state_df["Registered Users"].astype("float")
            fig = px.bar(state_df, x='Year', y='Registered Users')
            st.subheader(f''':green[**Registered Users of {match_state} over years**]''')
            st.plotly_chart(fig)

            #Displaying dropdown to select a district from the selected state. 
            district_list_query = """select distinct(district) from map_user 
                                            where state = %s"""
            cursor.execute(district_list_query, ([state]) )
            values = cursor.fetchall()
            district_list = [""]
            for item in values:
                district_list.append(item[0])
            st.subheader(f''':green[District Info]''')
            district = st.selectbox("Choose the desired district",(district_list))

            #Fetching and displaying, Registered users and app opens count for the selected district.
            if district:
                QUERY = """select registered_users, app_opens 
                            from map_user
                            where district = %s and year = %s and quarter = %s;"""
                cursor.execute(QUERY,([district, year_option, quarter_option]))
                val = cursor.fetchall()
                registered_users = val[0][0]
                app_opens =  val[0][1]
                st.markdown(f''':blue[**Registered Users in {year_option} Quarter {quarter_option}**]   :   **{registered_users}**''')
                st.markdown(f''':blue[**App Opens in {year_option} Quarter {quarter_option}**]  :  **{app_opens}**''')

            #Displaying dropdown to select a pincode from the selected state.
            QUERY = """select distinct(pincode) from top_user_pincodes
                        where state = %s;"""
            cursor.execute(QUERY,([state]))
            val = cursor.fetchall()
            pincode_list = [""]
            for item in val:
                pincode_list.append(item[0])
            st.subheader(f''':green[Area Info]''')
            pincode = st.selectbox("Choose the desired pincode",(pincode_list))
            #Fetching and displaying, Registered users count for the selected district.
            if pincode:
                QUERY = """select pincode, registered_users from top_user_pincodes
                        where pincode = %s and year = %s and quarter = %s;"""
                cursor.execute(QUERY, ([pincode, year_option, quarter_option]))
                val = cursor.fetchall()
                if val:
                    registered_users = val[0][1]
                    st.markdown(f":blue[**Registered Users in {year_option} Quarter {quarter_option}**]   :   **{registered_users}**")
                else:
                    st.markdown(f":orange[Value Not found for the chosen pincode in year {year_option} quarter {quarter_option}]")

            #Displaying multiselect dropdown to select brands.
            brands_list = ["Xiaomi", "Samsung", "Vivo", "Oppo", "OnePlus", "Realme", "Apple", "Motorola", "Lenovo", "Huawei", "Others",
                        "Tecno", "Gionee", "Infinix", "Asus", "Micromax", "HMD Global", "Lava", "COOLPAD", "Lyf"]
            st.subheader(f''':green[Brand Filter]''')
            brands_type = st.multiselect("Choose the desired Brand",(brands_list))
            if brands_type:
                brands_type_text = "',  '".join(brands_type)
                brands_type_text = "'"+brands_type_text+"'"
                #Fetching and displaying, selected brand's Registered users count for the selected district.
                QUERY = f"""select brand, count from agg_user_device
                            where brand in ({brands_type_text}) and state = "{state}" and year = {year_option} and quarter = {quarter_option};
                        """
                cursor.execute(QUERY)
                val = cursor.fetchall()
                if val:
                    for i in val:
                        brand = i[0]
                        user_count = i[1]
                        st.markdown(f":orange[**{brand}**]")
                        st.markdown(f''':blue[**Users Count in {year_option} Quarter {quarter_option}**]   :   **{user_count}**''')
                else:
                    st.markdown(f":orange[Value not found for the chosen Brands in the year {year_option} quarter {quarter_option}. Data present until 2022 Quarter 1]")

except Exception as e:
    print(f"An error has occured: {e}")