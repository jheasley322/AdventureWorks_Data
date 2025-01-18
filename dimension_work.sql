-- Master Calendar
    -- Step 1: Define the Date Range
    WITH DateRange AS (
        SELECT 
            DATE_TRUNC('year', MIN("OrderDate")) AS StartDate, -- Start of the year of the earliest date
            DATE_TRUNC('year', MAX("OrderDate")) + INTERVAL '1 year - 1 day' AS EndDate -- End of the year of the latest date
        FROM "adventureworks"."Sales"
    ),
    DateSeries AS (
        -- Step 2: Generate a Series of Dates
        SELECT 
            GENERATE_SERIES(StartDate, EndDate, INTERVAL '1 day')::DATE AS "Date"
        FROM DateRange
    ),
    CalendarDetails AS (
        -- Step 3: Add Calendar Fields
        SELECT 
            "Date",
            EXTRACT(DOY FROM "Date") AS "DayNumber", -- Day of the year
            TO_CHAR("Date", 'Dy') AS "DayOfWeek", -- Short day name
            CASE 
                WHEN EXTRACT(ISODOW FROM "Date") IN (6, 7) THEN TRUE 
                ELSE FALSE 
            END AS "Weekend", -- Boolean for weekends
            EXTRACT(WEEK FROM "Date") AS "WeekNumber", -- Week number
            CONCAT('Week ', EXTRACT(WEEK FROM "Date"), '-', EXTRACT(YEAR FROM "Date")) AS "WeekName", -- Week name (Week-Year)
            EXTRACT(MONTH FROM "Date") AS "Month", -- Month number
            CONCAT(TO_CHAR("Date", 'Mon'), '-', EXTRACT(YEAR FROM "Date")) AS "MonthName", -- Month name (Short Month-Year)
            CONCAT('Q', EXTRACT(QUARTER FROM "Date")) AS "Quarter", -- Quarter (Q1/Q2/Q3/Q4)
            CONCAT('Q', EXTRACT(QUARTER FROM "Date"), '-', EXTRACT(YEAR FROM "Date")) AS "QuarterName", -- Quarter name (Quarter-Year)

            -- Step 4: Prior Periods
            CONCAT('Week ', 
                CASE 
                    WHEN EXTRACT(WEEK FROM "Date") > 1 THEN EXTRACT(WEEK FROM "Date") - 1 
                    ELSE 52 -- Handle year boundary
                END, 
                '-', 
                CASE 
                    WHEN EXTRACT(WEEK FROM "Date") > 1 THEN EXTRACT(YEAR FROM "Date") 
                    ELSE EXTRACT(YEAR FROM "Date") - 1 -- Adjust year for first week
                END) AS "PriorWeek", -- Prior week name
            CONCAT(TO_CHAR("Date" - INTERVAL '1 month', 'Mon'), '-', EXTRACT(YEAR FROM "Date" - INTERVAL '1 month')) AS "PriorMonth", -- Prior month name
            CONCAT('Q', EXTRACT(QUARTER FROM "Date" - INTERVAL '3 months'), '-', EXTRACT(YEAR FROM "Date" - INTERVAL '3 months')) AS "PriorQuarter", -- Prior quarter name

            -- Step 5: Corresponding Periods
            CONCAT('Week ', EXTRACT(WEEK FROM "Date"), '-', EXTRACT(YEAR FROM "Date") - 1) AS "CorrespondingWeek", -- Same week last year
            CONCAT(TO_CHAR("Date" - INTERVAL '1 year', 'Mon'), '-', EXTRACT(YEAR FROM "Date" - INTERVAL '1 year')) AS "CorrespondingMonth", -- Same month last year
            CONCAT('Q', EXTRACT(QUARTER FROM "Date"), '-', EXTRACT(YEAR FROM "Date") - 1) AS "CorrespondingQuarter" -- Same quarter last year
        FROM DateSeries
    )
    -- Select CalendarDetails to populate the Master Calendar
    SELECT * FROM CalendarDetails;