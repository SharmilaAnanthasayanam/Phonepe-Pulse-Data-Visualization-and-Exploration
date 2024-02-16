import mysql.connector
import os
import pandas as pd
import streamlit as st
import plotly.express as px

try:
    # Connecting to SQL
    password= os.getenv("password")
    mydb =  mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = password,
        database = "phonepe"
    )
    cursor = mydb.cursor()

    # Title Configuration
    st.title("Insights Corner!🤠")
    st.text("")

    #Creating container to display insights on the same container
    container = st.container(border=True)
    placeholder = st.empty()

    #Creating functions for insights
    def insight_1():
        query = """SELECT state,sum(transaction_amount) FROM agg_transaction
                    GROUP BY state 
                    ORDER BY sum(transaction_amount) DESC
                    LIMIT 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[**Top 3 states having the Maximum Total Transaction Amount for the period from 2018 to 2023**]")
            for value in val:
                st.write(f"- {value[0].capitalize()} - ₹ {value[1]}")
            st.markdown(":green[Telangana has Hyderabad, Maharashtra has pune and Karnataka has Banglore.\
                        These metropolitan cities might be the reason for highest transaction amount. ]")

    def insight_2():
        query = """SELECT state,sum(transaction_count) FROM agg_transaction
                    GROUP BY state 
                    ORDER BY sum(transaction_count) DESC
                    LIMIT 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[**Top 3 states that has Maximum Total Transaction Count for the period from 2018 to 2023**]")
            for value in val:
                st.write(f"- {value[0].capitalize()} - {value[1]}") 
            st.markdown(":green[As mentioned previously, the devloped metropolitan cities are expected to be the\
                        reason for high transaction count]")

    def insight_3():
        query_amount = """select district,sum(amount) from map_transaction
                            group by district
                            order by sum(amount) desc
                            limit 3;"""
        query_count = """select district, sum(count) from map_transaction
                            group by district
                            order by sum(count) desc
                            limit 3;"""
        cursor.execute(query_amount)
        val_amount = cursor.fetchall()
        # df_amount = pd.DataFrame(val_amount)
        # df_amount.columns=["State", "Year", "Quarter", "District", "Count", "Amount"]

        cursor.execute(query_count)
        val_count = cursor.fetchall()
        # df_count = pd.DataFrame(val_count)
        # df_count.columns=["State", "Year", "Quarter", "District", "Count", "Amount"]

        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[The 3 districts that has the Top Total Transaction Amount and Count in Overall India]")
            st.markdown(":orange[Amount]")
            for values in val_amount:
                st.write(f"- {values[0]} - ₹ {values[1]}")
            # st.markdown(":orange[Top Amount Table]")
            # st.table(df_amount)
            st.markdown(":orange[Count]")
            for values in val_count:
                st.write(f"- {values[0]} - {values[1]}")
            # st.table(df_count)
            st.markdown(":green[As expected, top states and top districts inside the states correlates.]")

    def insight_4():
        query = """select state,sum(transaction_amount) from agg_transaction
                    group by state 
                    order by sum(transaction_amount)
                    limit 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[The lowest 3 states making the Minimum Total Transaction Amount for the period from 2018 to 2023]")
            for value in val:
                st.write(f"- {value[0].capitalize()} - ₹ {value[1]}")
            st.markdown(":green[As Lakshadweep and Andaman & Nicobar are Islands, they have very less human population and covered with large amount of flora and fauna.\
                        \n Mizoram is the second least populated state and possess tribal origin.]")

    def insight_5():
        query = """select state,sum(transaction_count) as total_trans_amount from agg_transaction
                    group by state 
                    order by sum(transaction_count)
                    limit 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[The Lowest 3 states that has Minimum Total Transaction Count for the period from 2018 to 2023]")
            for value in val:
                st.write(f"- {value[0].capitalize()} - {value[1]}")
            st.markdown(":green[Laskshadweep and Mizoram's less transaction count reason might be due to previosuly mentioned reasons.\
                        \n Ladakh's eastern region constitutes region that has dispute with pakistan. Safety disputes might be the reason for less population and transactions in Ladakh.]")
            

    def insight_6():
        query = """SELECT g.district, d.state, SUM(g.amount) AS total_amount
                    FROM map_transaction AS d
                    INNER JOIN (
                    SELECT district, SUM(amount) AS amount
                    FROM map_transaction
                    GROUP BY district
                    ) AS g ON d.district = g.district
                    GROUP BY d.state, g.district
                    order by sum(g.amount)
                    limit 3;"""
        query_count = """SELECT g.district, d.state, SUM(g.count) AS total_count
                        FROM map_transaction AS d
                        INNER JOIN (
                        SELECT district, SUM(count) AS count
                        FROM map_transaction
                        GROUP BY district
                        ) AS g ON d.district = g.district
                        GROUP BY d.state, g.district
                        order by sum(g.count)
                        limit 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        df = pd.DataFrame(val)
        df.columns = ["District", "State", "Total Transaction Amount"]

        cursor.execute(query_count)
        val_count = cursor.fetchall()
        df_count = pd.DataFrame(val_count)
        df_count.columns = ["District", "State", "Total Transaction Count"]

        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[The Lowest 3 districts that has Minimum Total Transaction Amount for the period from 2018 to 2023]")
            st.table(df)
            st.markdown(":blue[The Lowest 3 districts that has Minimum Total Transaction Count for the period from 2018 to 2023]")
            st.table(df_count)
            st.markdown(":green[Least Transaction state does not coincide with districts. \
                        \n 1.Pherzawl district is mainly populated by kuki-zo hill tribal people.\
                        \n 2.Shi Yomi district is home to the people of Adi Tagin, and Memba tribes\
                        \n 3.Dibang Valley district is the least populated district in India.\
                        These are expected to be the reasons for least transaction in these districts]")

    def insight_7():
        query = """select brand, sum(count) from agg_user_device
                    group by brand
                    order by sum(count) desc;"""
        cursor.execute(query)
        val = cursor.fetchall()
        df = pd.DataFrame(val)
        df.columns = ["Brand", "Total Users"]
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[**Brands users count Table**]")
            st.table(df)
            st.markdown(":green[Phonepe might have collaboration with Smart Phone vendors like Xiaomi.]")


    def insight_8():
        query =  """select transaction_type, sum(transaction_count) from agg_transaction
                    group by transaction_type
                    order by sum(transaction_count) desc;"""
        cursor.execute(query)
        val = cursor.fetchall()
        df = pd.DataFrame(val)
        df.columns = ["Transaction Type", "Total Count"]
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[**Transaction Type count**]")
            st.table(df)
            st.markdown(":green[As every shop has QR scanning facilities, Merchant transaction might be at the peak.]")

    def insight_9():
        query = """select state, sum(registered_users) from map_user
                group by state
                order by sum(registered_users) desc
                limit 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        df = pd.DataFrame(val)
        df.columns = ["State", "Total Registered Users"]
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[**State Registered Users count**]")
            st.table(df)
            st.markdown(":green[Maharashtra and Karnataka have hightest Transaction amount and count, so it is no wonder that it has hightest count of users.\
                        \n Uttar Pradesh was in 6th position in total transaction amount table. So this might also be the district with high users count.]")

    def insight_10():
        query = """select  district, sum(count) from map_transaction
                    where state = "tamil-nadu"
                    group by district
                    order by sum(count) desc
                    limit 3;"""
        cursor.execute(query)
        val = cursor.fetchall()
        df = pd.DataFrame(val)
        df.columns = ["District", "Transaction Count"]
        placeholder.empty()
        with placeholder.container():
            st.markdown(":blue[**District Transaction Count**]")
            st.table(df)
            st.markdown(":green[**In Chennai, Shollinganallur is the place where transaction occurs the most. \
                        It is the area that has much IT companies. This might be the reason.**]")

    st.text("")
    #Creating slider to traverse through the insights
    slider = st.slider("Move the slider to go through the information", min_value=1, max_value=10, step=1 )
    #Based on the slider value respective function is being called.
    if slider == 1:
        insight_1()
    elif slider == 2:
        insight_2()
    elif slider == 3:
        insight_3()
    elif slider == 4:
        insight_4()
    elif slider == 5:
        insight_5()
    elif slider == 6:
        insight_6()
    elif slider == 7:
        insight_7()
    elif  slider == 8:
        insight_8()
    elif slider == 9:
        insight_9()
    elif slider == 10:
        insight_10()

except Exception as e:
    print(f"An Error has Occured : {e}")

















 