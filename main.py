import json
from decimal import Decimal
import pandas as pd
from openai import OpenAI
import os
import databricks.sql as sql
import streamlit as st
import numpy as np
import requests
import time

# Database connection parameters
DB_HOSTNAME = "dbc-0e2cf586-b478.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/62a107e9f3119c6c"

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

def get_bikes(description_string):
    hostname = 'https://' + DB_HOSTNAME
    # Endpoint to trigger a notebook job
    endpoint = f"{hostname}/api/2.0/jobs/runs/submit"

    # Headers for authentication
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    # Endpoint to trigger a notebook job and to get job output
    submit_endpoint = f"{hostname}/api/2.0/jobs/runs/submit"
    output_endpoint = f"{hostname}/api/2.0/jobs/runs/get-output"

    # Data for the API call
    data = {
        "run_name": "Run from API",
        "existing_cluster_id": "0425-153951-lwvf16nk",
        "notebook_task": {
            "notebook_path": "/Workspace/Shared/AI/External API Call",
            "base_parameters": {
                "Search_text": f"{description_string}"  # Pass the value to the notebook widget
            }
        }
    }

    # Start the notebook job
    response = requests.post(submit_endpoint, headers=headers, json=data)
    if response.status_code == 200:
        run_id = response.json()['run_id']
        print("Notebook job started successfully. Run ID:", run_id)
    else:
        print("Failed to start notebook job. Status Code:", response.status_code)
        print("Message:", response.json())
        exit()

    # Function to check job status
    def check_job_status(run_id):
        status_response = requests.get(f"{hostname}/api/2.0/jobs/runs/get?run_id={run_id}", headers=headers)
        status_response_json = status_response.json()
        return status_response_json['state']['life_cycle_state']

    # Polling job status until it is terminated
    while True:
        job_status = check_job_status(run_id)
        if job_status in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            break
        time.sleep(1)  # Check every 1 seconds

    # Fetch job output if job completed successfully
    if job_status == "TERMINATED":
        output_response = requests.get(f"{output_endpoint}?run_id={run_id}", headers=headers)
        if output_response.status_code == 200:
            notebook_output = output_response.json().get('notebook_output', {}).get('result', 'No output found')
            df = pd.read_json(notebook_output)
            return df


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
                        b.FullDateAlternateKey OrderDate,
                        SalesOrderNumber
                    FROM
                        adventure_works.v_fact_internet_sales a
                    LEFT JOIN adventure_works.dim_date b ON a.OrderDateKey = b.DateKey
                    WHERE 
                        CustomerKey = ?
                """
                cursor.execute(query, (customer_key,))
                result = cursor.fetchall()
                columns = ["Customer Key", "Date", "Order Number"]
                df = pd.DataFrame(result, columns=columns)
                df['Customer Key'] = df['Customer Key'].apply(lambda x: f'"{x}"')
                #df.drop(columns=["CustomerKey"], inplace=True)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

def get_order_details(customer_key, Sales_Order_Number):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                query = """
                        SELECT DISTINCT
                          SalesOrderLineNumber,
                          b.FullDateAlternateKey AS OrderDate,
                          EnglishProductName,
                          OrderQuantity,
                          UnitPrice
                        FROM
                            adventure_works.v_fact_internet_sales a
                        LEFT JOIN adventure_works.dim_date b ON a.OrderDateKey = b.DateKey
                        WHERE 
                            customerkey = '{}' AND SalesOrderNumber = '{}'
                        ORDER BY 
                            SalesOrderLineNumber asc
                    """.format(str(customer_key), str(Sales_Order_Number))
                cursor.execute(query)
                result = cursor.fetchall()
                columns = ["Line Number", "Date", "Name", "Quantity", "Unit Price"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

def get_badge(customer_key):
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
                          future_spend_prediction_quartile,
                          future_purchase_prob_quartile
                        FROM
                            prodds.ai_customer_details
                        WHERE 
                            CustomerKey = '{}'
                    """.format(customer_key)
                cursor.execute(query)
                result = cursor.fetchall()
                # Assuming the database returns only one row for the given customer key
                if result:
                    _, spend_quartile, purchase_quartile = result[0]
                    badge = assign_badge(spend_quartile, purchase_quartile)
                    return badge
                else:
                    st.error("No data found for this customer.")
                    return None
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return None

def assign_badge(spend_quartile, purchase_quartile):
    if spend_quartile in [1, 2] and purchase_quartile in [1, 2]:
        return "Gold"
    elif spend_quartile in [3, 4] and purchase_quartile in [3, 4]:
        return "Bronze"
    else:
        return "Silver"

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
                      a.ProductKey,
                      EnglishProductName
                    FROM
                      adventure_works.dim_product a
                    INNER JOIN prodds.product_inventory_status b on a.productkey = b.productkey
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
                    SELECT
                        ProductKey,
                        UnitsBalance,
                        LeadTimeWeeks,
                        TotalForecastedDemand,
                        OrderQuantity,
                        HasStock,
                        NeedsOrder,
                        WeeksUntilStockAvailable,
                        Margin_Pct
                    FROM prodds.product_inventory_status
                    where productkey = '{}'
                """.format(str(product_key))
                cursor.execute(query)
                result = cursor.fetchall()
                columns = ["ProductKey", "UnitsBalance", "LeadTimeWeeks", "TotalForecastedDemand", "OrderQuantity", "HasStock", "NeedsOrder", "WeeksUntilStockAvailable", "Margin_Pct"]
                df = pd.DataFrame(result, columns=columns)
                return df
    except Exception as e:
        st.error(f"Failed to retrieve data: {e}")
        return pd.DataFrame()

def generate_customer_service_email(customer_key, sales_order_number, line_number):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                # Define the AI SQL query
                ai_query = f"""
                    WITH json_data AS (
                        SELECT to_json(struct(
                            EmailAddress, FirstName, LastName, SalesOrderNumber,
                            SalesOrderLineNumber, EnglishProductName, OrderQuantity,
                            UnitPrice, '30% off 1 Item' AS Offer, 'Qx2V3mW8FjCt' AS CouponCode,
                            'Adam' AS SalesRepName, 'Adventure Works' AS CompanyName
                        )) AS json_string
                        FROM adventure_works.v_fact_internet_sales
                        WHERE CustomerKey = '{customer_key}'
                          AND SalesOrderNumber = '{sales_order_number}'
                          AND SalesOrderLineNumber = {line_number}
                    ),
                    prompt AS (
                        SELECT 'Generate a professional customer service email regarding a recent product return. Include greeting, product details, offer details, and closing remarks. Use the data provided:' AS prompt
                    )
                    SELECT ai_query(
                        'databricks-dbrx-instruct',  -- Update with the correct AI model endpoint
                        prompt.prompt || json_data.json_string
                    ) AS generated_email
                    FROM json_data, prompt
                """
                cursor.execute(ai_query)
                result = cursor.fetchone()

                if result:
                    return result[0]  # Assuming the AI-generated email is the first field in the result set
                else:
                    return "No email content generated for this customer."
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None

def insert_return(order_number, line_number):
    try:
        with sql.connect(
            server_hostname=DB_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                # SQL command to insert the return
                insert_query = f"""
                    INSERT INTO adventure_works.fact_returns (OrderNumber, LineNumber)
                    VALUES ('{order_number}', {line_number})
                """
                cursor.execute(insert_query)
                connection.commit()  # Commit the transaction to make sure it saves
                st.success("Return inserted successfully into the database.")
    except Exception as e:
        st.error(f"An error occurred while inserting the return: {e}")

def order_history_order_line_selection_dataframe(df):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Select", False)
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
    )
    selected_indices = np.where(edited_df["Select"].astype(bool))[0]
    
    return selected_indices

def email_order_line_selection_dataframe(df):
    df_with_selections = df.copy()
    df_with_selections.insert(0, "Select", False)
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True)},
        disabled=df.columns,
    )
    selected_indices = np.where(edited_df["Select"].astype(bool))[0]
    return selected_indices


def generate_dashboard(customer_key, selected_customer_desc):
    customer_name = selected_customer_desc.split(" - ")[0]
   
    if "orders" not in st.session_state:
        st.session_state["orders"]    = get_orders(customer_key)
        st.session_state["badge"]     = get_badge(customer_key)
        st.session_state["details"]   = get_customer_details(customer_key)
        st.session_state["inventory"] = get_products()

    customer_orders         = st.session_state["orders"]
    details_df              = st.session_state["details"]
    inventory_df            = st.session_state["inventory"]

    # First row
    col1, col2, col3 = st.columns([4, 4, 2])

    with col1:
        with st.expander("Paragraph Summary", expanded=True):
            st.markdown("<h2 class='header'>GPT Generated Paragraph 📝 </h2>", unsafe_allow_html=True)
            st.markdown(f"<div class='section'>{details_df}</div>", unsafe_allow_html=True)

    if "returnsDf" not in st.session_state:
        st.session_state["returnsDf"] = pd.DataFrame(columns=['Sales Order Number', 'Order Line Number', 'Name'])

    with col2:
       with st.expander("Order History", expanded=True):
            st.markdown("<h2 class='header'>Order History 🛒 </h2>", unsafe_allow_html=True)
            st.markdown("<h5 class='subheader'>Customer Orders</h5>", unsafe_allow_html=True)

            st.dataframe(customer_orders, use_container_width=True, hide_index=True)

            # Selection Widget
            st.markdown("<h5 class='subheader'>Select Customer Order</h5>", unsafe_allow_html=True)
            
            selected_order_id = st.selectbox("", customer_orders['Order Number'], key="order_id")
            if "selectedOrderId" not in st.session_state:
                st.session_state["selectedOrderId"] = selected_order_id
                st.session_state["currOrder"] = get_order_details(customer_key, selected_order_id)

            # Conditional Rendering
            if selected_order_id != st.session_state["selectedOrderId"]:
                st.session_state["selectedOrderId"] = selected_order_id
                st.session_state["currOrder"] = get_order_details(customer_key, selected_order_id)
            
            st.markdown(f"<h5 class='subheader'>Details for Order #{selected_order_id}</h5>", unsafe_allow_html=True)

            
            selected_sales_order_numbers = order_history_order_line_selection_dataframe(st.session_state["currOrder"])

            # Create a button using Streamlit
            button_clicked = st.button("Issue Return")

            # Check if the button is clicked
            if button_clicked:

                # Call the function when the button is clicked
                for number in selected_sales_order_numbers:
                    line_number = st.session_state["currOrder"].iloc[number]["Line Number"]
                    insert_return(selected_order_id, line_number)
                    product_name = st.session_state["currOrder"].iloc[number]["Name"]
                    new_row = {'Sales Order Number': selected_order_id, 'Order Line Number': line_number, 'Name': product_name}
                    returns_df = st.session_state["returnsDf"]
                    returns_df = pd.concat([returns_df, pd.DataFrame([new_row], columns=returns_df.columns)], ignore_index=True)
                    st.session_state["returnsDf"] = returns_df

    with col3:
        with st.expander("Customer Badge", expanded=True):
            st.markdown("<h2 class='header'>Customer Badge 🎖️ </h2>", unsafe_allow_html=True)
            image_string = st.session_state["badge"]
            st.write(f"{customer_name} is a {image_string} customer.")
            st.image(image_string + ".png", caption = image_string + " Badge")
            

    # Second row
    col1, col2, col3 = st.columns(3)

    with col1:
        with st.expander("Search for Bike Products", expanded=True):
            st.markdown("<h2 class='header'>Bike Search 🚲 </h2>", unsafe_allow_html=True)
            
            # Add a large text input field
            search_query = st.text_area("Enter your search query:", height=100)

            # Add a search button
            if st.button("Search"):
                bike_df = get_bikes(search_query)
                st.session_state["bikeResults"] = bike_df

            if "bikeResults" in st.session_state:
                st.dataframe(st.session_state["bikeResults"], use_container_width=True, hide_index=True) 

    with col2:
        with st.expander("Inventory", expanded=True):
            st.markdown("<h2 class='header'>Inventory 📦 </h2>", unsafe_allow_html=True)

            inventory_options = {f"{row['EnglishProductName']}": row['ProductKey']
                for index, row in inventory_df.iterrows()}

            selected_product = st.selectbox("Select a Product", options=list(inventory_options.keys()))
            product_key = inventory_options[selected_product]

            if "selectedProduct" not in st.session_state:
                st.session_state["selectedProduct"] = selected_product
                st.session_state["productDesc"] = get_inventory_data(str(product_key))

            if selected_product != st.session_state["selectedProduct"]:
                st.session_state["selectedProduct"] = selected_product
                st.session_state["productDesc"] = get_inventory_data(str(product_key))

            st.dataframe(st.session_state["productDesc"], use_container_width=True, hide_index=True)  

    with col3:
        with st.expander("Generate a Return Email", expanded=True):
            st.markdown("<h2 class='header'>Email Generation 📧 </h2>", unsafe_allow_html=True)
            st.markdown("<h5 class='subheader'>Processed Returns</h5>", unsafe_allow_html=True)

            selected_returns = email_order_line_selection_dataframe(st.session_state["returnsDf"])

            generate_email_clicked = st.button("Generate Email")

            email_content = ""
            if generate_email_clicked:
                for number in selected_returns:
                    order_number = st.session_state["returnsDf"].iloc[number]["Sales Order Number"]
                    line_number  = st.session_state["returnsDf"].iloc[number]["Order Line Number"]
                    email_content = generate_customer_service_email(customer_key, order_number, line_number) + "\n\n"

            email_text_input = st.text_area("Email Content", email_content, height=400)
            email_send_clicked = st.button("Send Email")

            if email_send_clicked:
                st.success("Email sent successfully!")

            
def main():
    st.set_page_config(layout="wide")
    st.title('CART')

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

        .subheader {
            padding-top: 5px
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

    if "customerOptions" not in st.session_state:
        st.session_state["customerOptions"] = get_customers()

    df = st.session_state["customerOptions"]

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

        customer_key = customer_options[selected_customer_desc]

        if "badge" in st.session_state and not generate_dashboard_button:
            generate_dashboard(customer_key, selected_customer_desc)

        if generate_dashboard_button:
            maintained_customer = st.session_state["customer"]
            for key in st.session_state.keys():
                del st.session_state[key]
            st.session_state["customer"] = maintained_customer

            generate_dashboard(customer_key, selected_customer_desc)
    else:
        st.write("No data available.")

    st.write()
            
if __name__ == "__main__":
    main()