import pandas as pd
import streamlit as st

def get_dummy_customers():
    data = {
        'CustomerKey': [101, 102, 103, 104, 105],
        'FirstName': ['John', 'Alice', 'Bob', 'Emma', 'Michael'],
        'LastName': ['Doe', 'Smith', 'Johnson', 'Brown', 'Williams'],
        'EmailAddress': ['john@example.com', 'alice@example.com', 'bob@example.com', 'emma@example.com', 'michael@example.com'],
        'AddressLine1': ['123 Main St', '456 Elm St', '789 Oak St', '101 Pine St', '202 Maple St']
    }
    df = pd.DataFrame(data)
    return df

def get_dummy_customer_details():
    customer_info = """
        Here's a concise summary for customer service representatives:
Customer Profile:

Name: Wyatt Hill
Key Metrics:
Total Orders: 1 (vs. POP Avg: 1.5)
Total Units Ordered: 3 (vs. POP Avg: 3.3)
Total Sales Amount: 
2
,
332.28
(
�
�
.
�
�
�
�
�
�
:
2,332.28(vs.POPAvg:1,588.30)
Segmentation:
Demographics: Male, Homeowner, 1 Car, Commutes 5-10 Miles, $30,000 Yearly Income
Additional Insights:
First and Latest Order Date: February 9, 2013
Upsell Opportunity: Sport-100 Helmet, Blue
This summary provides a quick overview of the customer's key metrics, comparing them to the average, as well as segmentation details and additional insights. The formatting is clean and easy to read, making it perfect for display on a dashboard
    """
    return customer_info


def get_dummy_orders():
    data = {
        'SalesOrderNumber': ['SO001', 'SO002', 'SO003', 'SO004', 'SO005'],
        'OrderDate': ['2024-04-01', '2024-04-05', '2024-04-10', '2024-04-15', '2024-04-20']
    }
    df = pd.DataFrame(data)
    return df

def get_dummy_order_details():
    data = {
        'Item Number': [1, 2, 1, 2, 1],
        'Product': ['Product A', 'Product B', 'Product A', 'Product B', 'Product C'],
        'Quanity': [5, 10, 3, 7, 2],
        'Price': [20.0, 30.0, 25.0, 35.0, 50.0]
    }
    df = pd.DataFrame(data)
    return df

def get_dummy_products():
    data = {
        'ProductKey': [101, 102, 103, 104, 105],
        'EnglishProductName': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E']
    }
    df = pd.DataFrame(data)
    return df

def get_dummy_inventory_data():
    data = {
        'ProductKey': [101, 102, 103, 104, 105],
        'UnitsBalance': [100, 200, 150, 300, 250],
        'LeadTimeWeeks': [2, 3, 2, 4, 3],
        'TotalForecastedDemand': [500, 600, 550, 700, 800],
        'OrderQuantity': [200, 300, 250, 400, 350],
        'HasStock': [True, True, True, True, True],
        'NeedsOrder': [False, False, False, False, False]
    }
    df = pd.DataFrame(data)
    return df
