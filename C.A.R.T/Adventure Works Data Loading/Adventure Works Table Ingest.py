# Databricks notebook source
# MAGIC %sql
# MAGIC --drop schema adventure_works cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists adventure_works;

# COMMAND ----------

#Tables and fields to unnecessarily liquid cluster on
tables_dict = {
        'DimAccount': "AccountKey,ParentAccountKey",
        'DimCurrency': 'CurrencyKey',
        'DimCustomer': 'CustomerKey,GeographyKey',
        'DimDate': 'DateKey',
        'DimDepartmentGroup': 'DepartmentGroupKey,ParentDepartmentGroupKey',
        'DimEmployee': 'EmployeeKey,ParentEmployeeKey,SalesTerritoryKey',
        'DimGeography': 'GeographyKey',
        'DimOrganization': 'OrganizationKey,ParentOrganizationKey,CurrencyKey',
        'DimProduct': 'ProductKey,ProductSubcategoryKey',
        'DimProductCategory': 'ProductCategoryKey',
        'DimProductSubcategory': 'ProductSubcategoryKey,ProductCategoryKey',
        'DimPromotion': 'PromotionKey',
        'DimReseller': 'ResellerKey,GeographyKey',
        'DimSalesReason': 'SalesReasonKey',
        'DimSalesTerritory': 'SalesTerritoryKey',
        'DimScenario': 'ScenarioKey',
        'FactAdditionalInternationalProductDescription': 'ProductKey,CultureName',
        'FactCallCenter': 'FactCallCenterID,DateKey,Shift',
        'FactCurrencyRate': 'CurrencyKey,DateKey',
        'FactFinance': 'FinanceKey',
        'FactInternetSales': 'SalesOrderNumber,SalesOrderLineNumber',
        'FactInternetSalesReason': 'SalesOrderNumber,SalesOrderLineNumber,SalesReasonKey',
        'FactProductInventory': 'ProductKey,DateKey',
        'FactResellerSales': 'SalesOrderNumber,SalesOrderLineNumber',
        'FactSalesQuota': 'SalesQuotaKey',
        'FactSurveyResponse': 'SurveyResponseKey',
        'NewFactCurrencyRate': 'CurrencyKey,DateKey',
        'ProspectiveBuyer': 'ProspectiveBuyerKey',
        'sysdiagrams': 'name,principal_id,diagram_id'}

# COMMAND ----------

import re
from pyspark.sql.functions import expr

for i in tables_dict:
    #databricks forces all lowercase names, so adding underscores where capitalizations added readability previously
    snake_table_name = re.sub(r'([a-z])([A-Z])', '\\1_\\2', i).lower() 
    target_table = 'adventure_works.' + snake_table_name
    cluster_fields = tables_dict[i]
    print('Ingesting: ' + i + ' as ' + snake_table_name)
    remote_table = (spark.read
                .format("sqlserver")
                .option("host", "****.****.us-east-2.rds.amazonaws.com")
                .option("port", "1433")
                .option("user", "admin")
                .option("password", "****") 
                .option("database", "adventure_works")
                .option("dbtable", i)
                .option("encrypt", "false")
                .load() 
                )

    pre_filter_count = remote_table.count()

    #Date manipulation and filtering to make data look current
#    match i:
#            case "DimCustomer":
#                remote_table = remote_table.withColumn("BirthDate", expr("add_months(BirthDate, 132)"))
#                remote_table = remote_table.withColumn("DateFirstPurchase", expr("add_months(DateFirstPurchase, 132)"))
#                remote_table = remote_table.filter(remote_table.DateFirstPurchase < '2024-04-30')
#
#            case "DimDate":
#                remote_table = remote_table.withColumn("FullDateAlternateKey", expr("add_months(FullDateAlternateKey, 132)"))
#                remote_table = remote_table.withColumn("CalendarYear", expr("CalendarYear + 11"))
#                remote_table = remote_table.withColumn("FiscalYear", expr("FiscalYear + 11"))
#        
#            case "DimEmployee":
#                remote_table = remote_table.withColumn("BirthDate", expr("add_months(BirthDate, 132)"))
#                remote_table = remote_table.withColumn("HireDate", expr("add_months(HireDate, 132)"))
#                remote_table = remote_table.withColumn("StartDate", expr("add_months(StartDate, 132)"))
#                remote_table = remote_table.withColumn("EndDate", expr("add_months(EndDate, 132)"))
#                remote_table = remote_table.filter((remote_table.StartDate < '2024-04-30') & ((remote_table.EndDate < '2024-04-30') |  (remote_table.EndDate.isNull())))
#
#            case "DimProduct":
#                remote_table = remote_table.withColumn("StartDate", expr("add_months(StartDate, 132)"))
#                remote_table = remote_table.withColumn("EndDate", expr("add_months(EndDate, 132)"))
#                remote_table = remote_table.filter((remote_table.StartDate < '2024-04-30') & ((remote_table.EndDate < '2024-04-30') |  (remote_table.EndDate.isNull())))
#
#            case "DimPromotion":
#                remote_table = remote_table.withColumn("StartDate", expr("add_months(StartDate, 132)"))
#                remote_table = remote_table.withColumn("EndDate", expr("add_months(EndDate, 132)"))
#                remote_table = remote_table.filter((remote_table.StartDate < '2024-04-30') & ((remote_table.EndDate < '2024-04-30') |  (remote_table.EndDate.isNull())))
#
#            case "DimReseller":
#                remote_table = remote_table.withColumn("FirstOrderYear", expr("FirstOrderYear + 11"))
#                remote_table = remote_table.withColumn("LastOrderYear", expr("LastOrderYear + 11"))
#                remote_table = remote_table.withColumn("YearOpened", expr("YearOpened + 11"))
#
#            case "FactCallCenter":
#                remote_table = remote_table.withColumn("Date", expr("add_months(Date, 132)"))
#                #remote_table = remote_table.filter("Date < '2024-04-30'") #This drops all data. Table weirdly has really late dates compared to the rest of the tables
#
#            case "FactCurrencyRate":
#                remote_table = remote_table.withColumn("Date", expr("add_months(Date, 132)"))
#                remote_table = remote_table.filter(remote_table.Date < '2024-04-30')
#
#            case "FactFinance":
#                remote_table = remote_table.withColumn("Date", expr("add_months(Date, 132)"))
#                remote_table = remote_table.filter(remote_table.Date < '2024-04-30')
#
#            case "FactInternetSales":
#                remote_table = remote_table.withColumn("OrderDate", expr("add_months(OrderDate, 132)"))
#                remote_table = remote_table.withColumn("DueDate", expr("add_months(DueDate, 132)"))
#                remote_table = remote_table.withColumn("ShipDate", expr("add_months(ShipDate, 132)"))
#                
#                remote_table = remote_table.filter((remote_table.OrderDate < '2024-04-30') & (remote_table.ShipDate < '2024-04-30'))
#
#            case "FactProductInventory":
#                remote_table = remote_table.withColumn("MovementDate", expr("add_months(MovementDate, 132)"))
#                remote_table = remote_table.filter(remote_table.MovementDate < '2024-04-30') 
#
#            case "FactResellerSales":
#                remote_table = remote_table.withColumn("OrderDate", expr("add_months(OrderDate, 132)"))
#                remote_table = remote_table.withColumn("DueDate", expr("add_months(DueDate, 132)"))
#                remote_table = remote_table.withColumn("ShipDate", expr("add_months(ShipDate, 132)"))
#                remote_table = remote_table.filter((remote_table.OrderDate < '2024-04-30') & (remote_table.ShipDate < '2024-04-30'))
#
#            case "FactSalesQuota":
#                remote_table = remote_table.withColumn("Date", expr("add_months(Date, 132)"))   
#                remote_table = remote_table.filter(remote_table.Date < '2024-04-30')           
#
#            case "FactSurveyResponse":
#                remote_table = remote_table.withColumn("Date", expr("add_months(Date, 132)"))     
#                remote_table = remote_table.filter(remote_table.Date < '2024-04-30')   
#
#            case "NewFactCurrencyRate":
#                remote_table = remote_table.withColumn("CurrencyDate", expr("add_months(CurrencyDate, 132)"))  
#                remote_table = remote_table.filter(remote_table.CurrencyDate < '2024-04-30')  
#
#            case "ProspectiveBuyer":
#                remote_table = remote_table.withColumn("BirthDate", expr("add_months(BirthDate, 132)"))   
#
    post_filter_count = remote_table.count()
    print('\t' + 'orig_count = ' + str(pre_filter_count) + ' | post-filter count = ' + str(post_filter_count) + '\n')
    #Liquid clustering isn't gonna do much on these insanely tiny tables, but it's something I guess. Just being fancy.
    remote_table.write.format("delta").mode("overwrite").option("overwriteSchema", "true").clusterBy(cluster_fields.split(',')).saveAsTable(target_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE adventure_works;
# MAGIC ALTER TABLE Fact_Internet_Sales ADD COLUMN (Profit DOUBLE AFTER SalesAmount);
# MAGIC ALTER TABLE Fact_Reseller_Sales ADD COLUMN (Profit DOUBLE AFTER SalesAmount);
# MAGIC UPDATE Fact_Internet_Sales SET Profit = SalesAmount - TotalProductCost;
# MAGIC UPDATE Fact_Reseller_Sales SET Profit = SalesAmount - TotalProductCost;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE  adventure_works;
# MAGIC --String cast because union won't work with ints and dates in col 3. Must be same type
# MAGIC select "DimCustomer", "DateFirstPurchase", cast(max(DateFirstPurchase) as string) from Dim_Customer
# MAGIC UNION ALL 
# MAGIC select "DimDate", "FiscalYear", cast(max(FiscalYear) as string) from dim_date
# MAGIC UNION ALL 
# MAGIC select "DimEmployee", "StartDate", cast(max(StartDate) as string) from dim_employee
# MAGIC UNION ALL 
# MAGIC select "DimProduct", "EndDate", cast(max(EndDate) as string) from Dim_Product
# MAGIC UNION ALL 
# MAGIC select "DimPromotion", "EndDate", cast(max(EndDate) as string) from Dim_Promotion
# MAGIC UNION ALL 
# MAGIC select "FactCallCenter", "Date", cast(max(Date) as string) from Fact_Call_Center
# MAGIC UNION ALL 
# MAGIC select "FactFinance", "Date", cast(max(Date) as string) from Fact_Finance
# MAGIC UNION ALL 
# MAGIC select "FactInternetSales", "ShipDate", cast(max(ShipDate) as string) from Fact_Internet_Sales
# MAGIC UNION ALL 
# MAGIC select "FactResellerSales", "OrderDate", cast(max(OrderDate) as string) from Fact_Reseller_Sales
# MAGIC UNION ALL 
# MAGIC select "FactSurveyResponse", "Date", cast(max(Date) as string) from Fact_Survey_Response
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adventure_works.Fact_Internet_Sales

# COMMAND ----------

