--Summarize sales for insertion to targets column
    -- Step 1: Clean the Target Column
    -- Remove "$" and "," from the Target column and convert it to a numeric type
    UPDATE "adventureworks"."Targets"
    SET "Target" = REPLACE(REPLACE("Target", '$', ''), ',', '')::NUMERIC;

    -- Step 2: Add Necessary Columns to the Targets Table
    -- Add columns for TotalSales, TotalMargin, and ExceedsTarget if they don't already exist
    ALTER TABLE "adventureworks"."Targets"
    ADD COLUMN IF NOT EXISTS "TotalSales" NUMERIC, -- Column to store the summarized total sales
    ADD COLUMN IF NOT EXISTS "TotalMargin" NUMERIC, -- Column to store the summarized total margin
    ADD COLUMN IF NOT EXISTS "ExceedsTarget" BOOLEAN; -- Column to indicate whether sales exceeded the target

    ALTER TABLE "adventureworks"."Targets"
    ALTER COLUMN "Target" TYPE NUMERIC USING "Target"::NUMERIC;

    -- Step 3: Summarize Sales and Margin Data
    -- Use a Common Table Expression (CTE) to calculate TotalSales and TotalMargin for each EmployeeID and TargetMonth
    WITH SalesSummary AS (
        SELECT 
            sp."EmployeeID",
            DATE_TRUNC('month', so."OrderDate")::DATE AS "TargetMonth",
            SUM(so."TotalSales") AS "TotalSales",
            SUM(so."TotalSales" - so."TotalCost") AS "TotalMargin"
        FROM "adventureworks"."SalesOrders" so
        JOIN "adventureworks"."Salesperson" sp
        ON so."EmployeeKey" = CAST(sp."EmployeeKey" AS INTEGER)
        GROUP BY sp."EmployeeID", DATE_TRUNC('month', so."OrderDate")
    )
    -- Update the Targets Table
    UPDATE "adventureworks"."Targets" t
    SET 
        "TotalSales" = COALESCE(s."TotalSales", 0), -- Set TotalSales to 0 if NULL
        "TotalMargin" = COALESCE(s."TotalMargin", 0), -- Set TotalMargin to 0 if NULL
        "ExceedsTarget" = CASE
            WHEN COALESCE(s."TotalSales", 0) > t."Target" THEN TRUE -- Compare TotalSales with Target
            ELSE FALSE
        END
    FROM SalesSummary s
    WHERE t."EmployeeID" = s."EmployeeID"
    AND t."TargetMonth" = s."TargetMonth";

    -- Handle NULL Values
    UPDATE "adventureworks"."Targets"
    SET 
        "TotalSales" = COALESCE("TotalSales", 0), -- Replace NULL in TotalSales with 0
        "TotalMargin" = COALESCE("TotalMargin", 0), -- Replace NULL in TotalMargin with 0
        "ExceedsTarget" = COALESCE("ExceedsTarget", FALSE); -- Replace NULL in ExceedsTarget with FALSE

-- geocoding work
    ALTER TABLE "adventureworks"."Region"
    ADD COLUMN IF NOT EXISTS "Latitude" DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS "Longitude" DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS "CityShape" GEOMETRY(POLYGON, 4326);