-- Databricks notebook source
USE adventure_works;

create or replace view v_fact_call_center 
as 
select 
  a.*, 
  b.* except (DateKey)

from fact_call_center a

LEFT JOIN dim_date b 
ON a.DateKey = b.DateKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_currency_rate
as 
select 
  a.*, 
  b.* except (CurrencyKey), 
  c.* except (DateKey)

from fact_currency_rate a

LEFT JOIN dim_currency b
ON a.CurrencyKey = b.CurrencyKey

LEFT JOIN dim_date c 
ON a.DateKey = c.DateKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_finance
as 
select 
  a.*, 
  b.* except (AccountKey), 
  c.* except (DepartmentGroupKey), 
  d.* except (Organizationkey), 
  e.* except (DateKey)

from fact_finance a

LEFT JOIN dim_account b
ON a.AccountKey = b.AccountKey

LEFT JOIN dim_department_group c
ON a.DepartmentGroupKey = c.DepartmentGroupKey

LEFT JOIN dim_organization d
ON a.Organizationkey = d.Organizationkey

LEFT JOIN dim_date e 
ON a.DateKey = e.DateKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_internet_sales
as 
select 
  a.*, 
  b.CurrencyName, 
  c.* except (CustomerKey), 
  d.* except (ProductKey), 
  e.EnglishProductSubcategoryName,
  f.EnglishProductCategoryName,
  g.* except (DateKey)

from fact_internet_sales a

LEFT JOIN dim_Currency b
ON a.CurrencyKey = b.CurrencyKey

LEFT JOIN dim_Customer c
ON a.CustomerKey = c.CustomerKey

LEFT JOIN dim_Product d
ON a.ProductKey = d.ProductKey

LEFT JOIN dim_product_subcategory e
ON d.ProductSubcategoryKey = e.ProductSubcategoryKey

LEFT JOIN dim_product_category f
ON e.ProductCategoryKey = f.ProductCategoryKey

LEFT JOIN dim_date g
ON a.OrderDateKey = g.DateKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_internet_sales_reason
as 
select 
  a.*, 
  b.* except (SalesReasonKey), 
  c.* except (SalesOrderNumber, SalesOrderLineNumber), 
  d.* except (DateKey)

from fact_internet_sales_reason a

LEFT JOIN dim_sales_reason b
ON a.SalesReasonKey = b.SalesReasonKey

LEFT JOIN Fact_Internet_Sales c
ON a.SalesOrderNumber = c.SalesOrderNumber
AND a.SalesOrderLineNumber = c.SalesOrderLineNumber

LEFT JOIN dim_date d
ON c.OrderDateKey = d.DateKey

-- COMMAND ----------

USE adventure_works;

CREATE OR REPLACE VIEW v_fact_product_inventory AS
SELECT
  a.ProductKey,
  FLOOR(RAND() * (50 + 10 + 1) - 10) AS UnitsBalance, -- Generates random numbers between -10 and 50
  FLOOR(RAND() * 3 + 1) AS LeadTimeWeeks  -- Generates random numbers between 1 and 3
FROM
  fact_product_inventory a
LEFT JOIN
  Dim_Product b
ON
  a.ProductKey = b.ProductKey
GROUP BY
  a.ProductKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_reseller_sales
as 
select 
  a.*, 
  b.CurrencyName, 
  c.* except (EmployeeKey, StartDate, EndDate, Phone, SalesTerritoryKey, Status), c.StartDate as EmpStartDate, c.EndDate as EmpEndDate, c.Phone as EmpPhone, c.SalesTerritoryKey as EmpSalesTerritoryKey, c.Status as EmpStatus,
  d.* except (ProductKey), 
  e.EnglishProductSubcategoryName,
  f.EnglishProductCategoryName,
  h.* except(ResellerKey, ProductLine), h.ProductLine as ResellerProductLine,
  g.* except (DateKey)
from fact_reseller_sales a

LEFT JOIN dim_Currency b
ON a.CurrencyKey = b.CurrencyKey

LEFT JOIN dim_Employee c
ON a.EmployeeKey = c.EmployeeKey

LEFT JOIN dim_Product d
ON a.ProductKey = d.ProductKey

LEFT JOIN dim_product_subcategory e
ON d.ProductSubcategoryKey = e.ProductSubcategoryKey

LEFT JOIN dim_product_category f
ON e.ProductCategoryKey = f.ProductCategoryKey

LEFT JOIN dim_date g
ON a.OrderDateKey = g.DateKey

LEFT JOIN dim_reseller h
ON a.ResellerKey = h.ResellerKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_sales_quota
as 
select 
  a.*, 
  b.* except (EmployeeKey), 
  c.* except (DateKey, CalendarYear, CalendarQuarter)

from fact_sales_quota a

LEFT JOIN Dim_Employee b
ON a.EmployeeKey = b.EmployeeKey

LEFT JOIN dim_date c
ON a.DateKey = c.DateKey

-- COMMAND ----------

USE adventure_works;

create or replace view v_fact_survey_response
as 
select 
  a.*, 
  b.* except (CustomerKey), 
  c.* except (DateKey)

from fact_survey_response a

LEFT JOIN Dim_Customer b
ON a.CustomerKey = b.CustomerKey

LEFT JOIN dim_date c
ON a.DateKey = c.DateKey