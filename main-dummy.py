import json
from decimal import Decimal
import pandas as pd
from openai import OpenAI
import os
import databricks.sql as sql
import streamlit as st
import numpy as np

# Database connection parameters
DB_HOSTNAME = "dbc-0e2cf586-b478.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/62a107e9f3119c6c"

#Other Pages
import dummyData

# Function to connect to Databricks and fetch customer data
def get_customers():
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = """
                SELECT DISTINCT
                    CustomerKey,
                    FirstName,
                    LastName,
                    EmailAddress,
                    AddressLine1
                FROM
                    adventure_works.v_fact_internet_sales
                """
                cursor.execute(query)
                result = cursor.fetchall()
                columns = ["CustomerKey", "FirstName", "LastName", "EmailAddress", "AddressLine1"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

def get_customer_details(customer_key):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = f"""
                    WITH json_data AS (
                        SELECT to_json(struct(
                            CustomerKey, FirstName, LastName, TotalOrders, 
                            TotalUnitsOrdered, TotalSalesAmount, POP_AvgTotalOrders, 
                            POP_AvgTotalUnitsOrdered, POP_AvgTotalSalesAmount, 
                            LatestOrderDate, EarliestOrderDate, Gender, HouseOwnerFlag, 
                            NumberCarsOwned, CommuteDistance, YearlyIncome, Lifetime_Spend, 
                            future_spend_prediction_quartile, future_purchase_prob_quartile, 
                            bike_upsell_score_quartile, upsell_list
                        )) AS json_string
                        FROM prodds.ai_customer_details
                        WHERE CustomerKey = {customer_key}
                    ),
                    prompt AS (
                        SELECT 'Craft a concise summary for customer service representatives, highlighting key customer metrics and their comparison to the average, as well as segmentation details and additional insights. Ensure its easily digestible for use during sales calls. 0 on quartiles means not eligible.  The result will be dumped to display on a dashboard, so make sure it is formatted well.' AS prompt
                    )
                    SELECT ai_query(
                        'databricks-meta-llama-3-70b-instruct',  -- Assuming this is the correct endpoint and model
                        prompt.prompt || json_data.json_string
                    ) AS summary
                    FROM json_data, prompt
                """
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    return result[0]  # Assuming the AI-generated summary is the first field in the result set
                else:
                    return "No summary available for this customer."
    except Exception as e:
        return f"An error occurred: {e}"

# Function to connect to Databricks and fetch customer orders
def get_orders(customer_key):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT DISTINCT
                        CustomerKey,
                        SalesOrderNumber,
                        b.FullDateAlternateKey OrderDate
                    FROM
                        adventure_works.v_fact_internet_sales a
                    LEFT JOIN adventure_works.dim_date b ON a.OrderDateKey = b.DateKey
                    WHERE 
                        CustomerKey = ?
                """
                cursor.execute(query, (customer_key,))
                result = cursor.fetchall()
                columns = ["CustomerKey", "SalesOrderNumber", "OrderDate"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

# Function to connect to Databricks and fetch customer orders
def get_order_details(customer_key):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT DISTINCT
                      CustomerKey,
                      SalesOrderNumber,
                      b.FullDateAlternateKey OrderDate,
                      SalesOrderLineNumber,
                      EnglishProductName,
                      OrderQuantity,
                      UnitPrice
                    FROM
                        adventure_works.v_fact_internet_sales a
                    LEFT JOIN adventure_works.dim_date b ON a.OrderDateKey = b.DateKey
                    WHERE 
                        customerkey = ?
                """
                cursor.execute(query, (customer_key,))
                result = cursor.fetchall()
                columns = ["CustomerKey", "SalesOrderNumber", "OrderDate", "SalesOrderLineNumber", "EnglishProductName", "OrderQuantity", "UnitPrice"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

# Function to connect to Databricks and fetch customer orders
def get_products():
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT DISTINCT
                      ProductKey,
                      EnglishProductName
                    FROM
                      adventure_works.dim_product
                """
                cursor.execute(query)
                result = cursor.fetchall()
                columns = ["ProductKey", "EnglishProductName"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

# Function to connect to Databricks and fetch customer orders
def get_inventory_data(product_key):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT DISTINCT
                      ProductKey,
                      UnitsBalance,
                      LeadTimeWeeks,
                      Week1Demand,
                      Week2Demand,
                      Week3Demand,
                      Week4Demand,
                      TotalForecastedDemand,
                      OrderQuantity,
                      HasStock,
                      NeedsOrder
                    FROM
                      adventure_works.dim_product
                    where productkey = {product_key}
                """
                cursor.execute(query)
                result = cursor.fetchall()
                columns = ["ProductKey", "UnitsBalance", "LeadTimeWeeks", "TotalForecastedDemand", "OrderQuantity", "HasStock", "NeedsOrder"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

def dataframe_with_selections(df):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Select", False)
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
    )
    selected_indices = list(np.where(edited_df.Select)[0])
    selected_rows = df[edited_df.Select]
    #return {"selected_rows_indices": selected_indices, "selected_rows": selected_rows}

def issue_return():
    st.write("Return Issued")

def generate_dashboard(customer_options, selected_customer_desc):
    customer_key = customer_options[selected_customer_desc]
    customer_name = selected_customer_desc.split(" - ")[0]

    if "orders" not in st.session_state:
        st.session_state["orders"]    = dummyData.get_dummy_orders()
        st.session_state["details"]   = dummyData.get_dummy_customer_details()
        st.session_state["inventory"] = dummyData.get_dummy_inventory_data()
    
    customer_orders         = st.session_state["orders"]
    customer_orders_no_index = customer_orders.reset_index(drop=True)
    details_df              = st.session_state["details"]
    inventory_df            = st.session_state["inventory"]

    #inventory_df_styled           = style_dataframe(inventory_df)

    # First row
    col1, col2, col3 = st.columns([4, 4, 2])

    with col1:
        with st.expander("Paragraph Summary", expanded=True):
            st.markdown("<h2 class='header'>GPT Generated Paragraph 📝 </h2>", unsafe_allow_html=True)
            st.markdown(f"<div class='section'>{details_df}</div>", unsafe_allow_html=True)


    with col2:
       with st.expander("Order History", expanded=True):
            st.markdown("<h2 class='header'>Order History 🛒 </h2>", unsafe_allow_html=True)
            
            st.dataframe(customer_orders, use_container_width=True, hide_index=True)

            # Selection Widget
            selected_order_id = st.selectbox("", customer_orders['SalesOrderNumber'], key="order_id")

            # Conditional Rendering
            if selected_order_id is not None:
                selected_order = customer_orders[customer_orders['SalesOrderNumber'] == selected_order_id].iloc[0]
                st.session_state["currOrder"] = dummyData.get_dummy_order_details()
                current_order_details = st.session_state["currOrder"]
                selectable_order_details = dataframe_with_selections(current_order_details)
                
                st.write(selectable_order_details)

            # Create a button using Streamlit
            button_clicked = st.button("Issue Return")

            # Check if the button is clicked
            if button_clicked:
                # Call the function when the button is clicked
                issue_return()


    with col3:
        with st.expander("Customer Badge and Score", expanded=True):
            st.markdown("<h2 class='header'>Customer Badge and Score 🎖️ </h2>", unsafe_allow_html=True)
            st.markdown("<div class='section'> <p>Customer badge and score info goes here.</p> <button class='button'>View Details</button> </div>", unsafe_allow_html=True)
            st.image("silver.png", caption="Silver Badge")

    # Second row
    col1, col2 = st.columns(2)

    with col1:
        with st.expander("Send an Email", expanded=True):
            st.markdown("<h2 class='header'>Email Generation 📧 </h2>", unsafe_allow_html=True)
            st.markdown("<div class='section'> <p>Email generation options.</p> <button class='button'>Generate Email</button> </div>", unsafe_allow_html=True)

    with col2:
        with st.expander("Inventory", expanded=True):
            st.markdown("<h2 class='header'>Inventory 📦 </h2>", unsafe_allow_html=True)

            st.dataframe(inventory_df, use_container_width=True, hide_index=True)          

def main():
    st.set_page_config(layout="wide")
    st.title('Customer Data Search')

    # Custom CSS styles
    st.markdown(
        """
        <style>
        /* Body styles */
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f2f2f2; /* Light gray background */
            margin: 0;
            padding: 0;
        }

        /* Title styles */
        .title {
            text-align: center;
            font-size: 2.5rem;
            margin-bottom: 20px;
            color: #444; /* Dark gray title */
        }

        /* Section styles */
        .section {
            background-color: #fff;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);
            padding: 30px;
            margin-bottom: 30px;
            transition: box-shadow 0.3s ease;
        }

        .section:hover {
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.2);
        }

        /* Header styles */
        .header {
            text-align: center;
            font-size: 1.8rem;
            margin-bottom: 20px;
            color: #555; /* Subtle gray header */
            border-bottom: 2px solid #6C757D; /* Subtle, colorful border */
            padding-bottom: 10px; /* Add some space between header and section content */
        }

        /* Table styles */
        .table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }

        .table th, .table td {
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }

        .table th {
            background-color: #f2f2f2;
        }

        /* Button styles */
        .button {
            background-color: #007bff;
            color: #fff;
            border: none;
            padding: 12px 25px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 1rem;
            transition: background-color 0.3s ease;
        }

        .button:hover {
            background-color: #0056b3;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    #df = get_customers()
    df  = dummyData.get_dummy_customers()

    if not df.empty:
        customer_options = {f"{row['FirstName']} {row['LastName']} - {row['EmailAddress']}": row['CustomerKey']
                            for index, row in df.iterrows()}

        selected_customer_desc = st.selectbox("Select a Customer", options=list(customer_options.keys()))
        if "customer" not in st.session_state:
            st.session_state["customer"] = selected_customer_desc
        
        if selected_customer_desc != st.session_state["customer"]:
            for key in st.session_state.keys():
                del st.session_state[key]
            st.session_state["customer"] = selected_customer_desc

        generate_dashboard_button = st.button("Generate Dashboard")

        if "orders" in st.session_state:
            generate_dashboard(customer_options, selected_customer_desc)

        if generate_dashboard_button:
            maintained_customer = st.session_state["customer"]
            for key in st.session_state.keys():
                del st.session_state[key]
            st.session_state["customer"] = maintained_customer

            generate_dashboard(customer_options, selected_customer_desc)
            
    else:
        st.write("No data available.")

    st.write()

if __name__ == "__main__":
    main()