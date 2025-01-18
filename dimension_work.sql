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

-- adding Cost Breaks
    -- Add CostBreakID to adventureworks.Product
    ALTER TABLE "adventureworks"."Product"
    ADD COLUMN CostBreakID INT;

    -- Create the CostBreaks table
    CREATE TABLE "adventureworks"."CostBreaks" (
        CostBreakID INT PRIMARY KEY,
        CostBreak VARCHAR(50),
        PriceFamily VARCHAR(50),
        ItemClass VARCHAR(50)
    );

    -- Populate the CostBreaks table
    INSERT INTO "adventureworks"."CostBreaks" (CostBreakID, CostBreak, PriceFamily, ItemClass)
    VALUES
        (1, '0 to 25', 'Less than 100', 'Regular'),
        (2, '25 to 50', 'Less than 100', 'Regular'),
        (3, '50 to 100', 'Less than 100', 'Regular'),
        (4, '100 to 300', '100 to 500', 'Regular'),
        (5, '300 to 500', '100 to 500', 'Regular'),
        (6, '500 to 750', 'Over 500', 'Premium'),
        (7, '750 to 1000', 'Over 500', 'Premium'),
        (8, 'over 1000', 'Over 500', 'Premium');

    -- Update CostBreakID in adventureworks.Product based on Standard Cost
    UPDATE "adventureworks"."Product"
    SET CostBreakID = CASE
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 25 THEN 1
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 25 
            AND CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 50 THEN 2
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 50 
            AND CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 100 THEN 3
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 100 
            AND CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 300 THEN 4
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 300 
            AND CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 500 THEN 5
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 500 
            AND CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 750 THEN 6
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 750 
            AND CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 1000 THEN 7
        WHEN CAST(REGEXP_REPLACE("Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 1000 THEN 8
        ELSE NULL
    END;

    -- Add Foreign Key Constraint to Product Table
    ALTER TABLE "adventureworks"."Product"
    ADD CONSTRAINT FK_Product_CostBreak
    FOREIGN KEY (CostBreakID)
    REFERENCES "adventureworks"."CostBreaks" (CostBreakID);

    -- Create a Trigger Function to Populate CostBreakID with Cleaned Standard Cost
    CREATE OR REPLACE FUNCTION populate_cost_break_id()
    RETURNS TRIGGER AS $$
    BEGIN
    NEW.CostBreakID := CASE
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 25 THEN 1
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 25 
            AND CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 50 THEN 2
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 50 
            AND CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 100 THEN 3
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 100 
            AND CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 300 THEN 4
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 300 
            AND CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 500 THEN 5
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 500 
            AND CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 750 THEN 6
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 750 
            AND CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) <= 1000 THEN 7
        WHEN CAST(REGEXP_REPLACE(NEW."Standard Cost", '[^0-9.]', '', 'g') AS NUMERIC) > 1000 THEN 8
        ELSE NULL
    END;
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Create a Trigger to Automate CostBreakID Population
    CREATE TRIGGER trg_populate_cost_break_id
    BEFORE INSERT OR UPDATE ON "adventureworks"."Product"
    FOR EACH ROW
    EXECUTE FUNCTION populate_cost_break_id();