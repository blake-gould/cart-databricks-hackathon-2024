-- Databricks notebook source
-- adventure_works.vDMPrep source

-- v_DM_Prep will be used as a data source by the other data mining views.  
-- Uses DW data at customer, product, day, etc. granularity and
-- gets region, model, year, month, etc.
CREATE OR REPLACE VIEW adventure_works.v_DM_Prep
AS
    SELECT
        pc.EnglishProductCategoryName
        ,Coalesce(p.ModelName, p.EnglishProductName) AS Model
        ,c.CustomerKey
        ,s.SalesTerritoryGroup AS Region
        /*
        ,CASE
            WHEN Month(GetDate()) < Month(c.BirthDate)
                THEN DateDiff(yy,c.BirthDate,GetDate()) - 1
            WHEN Month(GetDate()) = Month(c.BirthDate)
            AND Day(GetDate()) < Day(c.BirthDate)
                THEN DateDiff(yy,c.BirthDate,GetDate()) - 1
            ELSE DateDiff(yy,c.BirthDate,GetDate())
        END AS Age*/
        ,floor(date_diff(now(), c.BirthDate)/365.25) as Age
        ,CASE
            WHEN c.YearlyIncome < 40000 THEN 'Low'
            WHEN c.YearlyIncome > 60000 THEN 'High'
            ELSE 'Moderate'
        END AS IncomeGroup
        ,d.CalendarYear
        ,d.FiscalYear
        ,d.MonthNumberOfYear AS Month
        ,f.SalesOrderNumber AS OrderNumber
        ,f.SalesOrderLineNumber AS LineNumber
        ,f.OrderQuantity AS Quantity
        ,f.ExtendedAmount AS Amount  
    FROM
        adventure_works.Fact_Internet_Sales f
    INNER JOIN adventure_works.Dim_Date d
        ON f.OrderDateKey = d.DateKey
    INNER JOIN adventure_works.Dim_Product p
        ON f.ProductKey = p.ProductKey
    INNER JOIN adventure_works.Dim_Product_Subcategory psc
        ON p.ProductSubcategoryKey = psc.ProductSubcategoryKey
    INNER JOIN adventure_works.Dim_Product_Category pc
        ON psc.ProductCategoryKey = pc.ProductCategoryKey
    INNER JOIN adventure_works.Dim_Customer c
        ON f.CustomerKey = c.CustomerKey
    INNER JOIN adventure_works.Dim_Geography g
        ON c.GeographyKey = g.GeographyKey
    INNER JOIN adventure_works.Dim_Sales_Territory s
        ON g.SalesTerritoryKey = s.SalesTerritoryKey 
;

-- COMMAND ----------

select * from adventure_works.v_DM_Prep where model is null or region is null

-- COMMAND ----------

-- dbo.vAssocSeqLineItems source

CREATE OR REPLACE VIEW adventure_works.v_Assoc_Seq_Line_Items
AS
SELECT     OrderNumber, LineNumber, Model
FROM         adventure_works.v_DM_Prep
WHERE     (FiscalYear = '2013');

-- COMMAND ----------

-- dbo.vAssocSeqOrders source

/* vAssocSeqOrders supports assocation and sequence clustering data mmining models.
      - Limits data to FY2004.
      - Creates order case table and line item nested table.*/
CREATE OR REPLACE VIEW adventure_works.v_Assoc_Seq_Orders
AS
SELECT DISTINCT OrderNumber, CustomerKey, Region, IncomeGroup
FROM         adventure_works.v_DM_Prep
WHERE     (FiscalYear = '2013');

-- COMMAND ----------

-- adventure_works.vTargetMail source

-- vTargetMail supports targeted mailing data model
-- Uses vDMPrep to determine if a customer buys a bike and joins to DimCustomer
CREATE OR REPLACE VIEW adventure_works.v_Target_Mail 
AS
    SELECT
        c.CustomerKey, 
        c.GeographyKey, 
        c.CustomerAlternateKey, 
        c.Title, 
        c.FirstName, 
        c.MiddleName, 
        c.LastName, 
        c.NameStyle, 
        c.BirthDate, 
        c.MaritalStatus, 
        c.Suffix, 
        c.Gender, 
        c.EmailAddress, 
        c.YearlyIncome, 
        c.TotalChildren, 
        c.NumberChildrenAtHome, 
        c.EnglishEducation, 
        c.SpanishEducation, 
        c.FrenchEducation, 
        c.EnglishOccupation, 
        c.SpanishOccupation, 
        c.FrenchOccupation, 
        c.HouseOwnerFlag, 
        c.NumberCarsOwned, 
        c.AddressLine1, 
        c.AddressLine2, 
        c.Phone, 
        c.DateFirstPurchase, 
        c.CommuteDistance, 
        x.Region, 
        x.Age, 
        CASE x.Bikes 
            WHEN 0 THEN 0 
            ELSE 1 
        END AS BikeBuyer
    FROM
        adventure_works.Dim_Customer c INNER JOIN (
            SELECT
                CustomerKey
                ,Region
                ,Age
                ,Sum(
                    CASE EnglishProductCategoryName 
                        WHEN 'Bikes' THEN 1 
                        ELSE 0 
                    END) AS Bikes
            FROM
                adventure_works.v_DM_Prep 
            GROUP BY
                CustomerKey
                ,Region
                ,Age
            ) AS x
        ON c.CustomerKey = x.CustomerKey
;

-- COMMAND ----------

-- adventure_works.vTimeSeries source

-- vTimeSeries view supports the creation of time series data mining models.
--      - Replaces earlier bike models with successor models.
--      - Abbreviates model names to improve readability in mining model viewer
--      - Concatenates model and region so that table only has one input.
--      - Creates a date field indexed to monthly reporting date for use in prediction.
CREATE OR REPLACE VIEW adventure_works.v_Time_Series 
AS
    SELECT 
        concat(
          CASE Model 
            WHEN 'Mountain-100' THEN 'M200' 
            WHEN 'Road-150' THEN 'R250' 
            WHEN 'Road-650' THEN 'R750' 
            WHEN 'Touring-1000' THEN 'T1000' 
            ELSE concat(Left(Model, 1), Right(Model, 3))
        END, ' ' , Region) AS ModelRegion 
        ,(cast(CalendarYear as INT) * 100) + cast(Month as INT) AS TimeIndex 
        ,Sum(Quantity) AS Quantity 
        ,Sum(Amount) AS Amount
		,CalendarYear
		,Month
		--,adventure_works.udfBuildISO8601Date (CalendarYear, Month, 25) as ReportingDate
    ,date(concat(CalendarYear, '-', Month, '-25')) as ReportingDate
    FROM 
        adventure_works.v_DM_Prep 
    WHERE 
        Model IN ('Mountain-100', 'Mountain-200', 'Road-150', 'Road-250', 
            'Road-650', 'Road-750', 'Touring-1000') 
    GROUP BY 
        concat(
          CASE Model 
            WHEN 'Mountain-100' THEN 'M200' 
            WHEN 'Road-150' THEN 'R250' 
            WHEN 'Road-650' THEN 'R750' 
            WHEN 'Touring-1000' THEN 'T1000' 
            ELSE concat(Left(Model, 1), Right(Model, 3))
        END, ' ' , Region) 
        ,(cast(CalendarYear as INT) * 100) + cast(Month as INT) 
		,CalendarYear
		,Month
		--,adventure_works.udfBuildISO8601Date (CalendarYear, Month, 25);
    ,date(concat(CalendarYear, '-', Month, '-25'))

-- COMMAND ----------

select * from adventure_works.v_Time_Series 