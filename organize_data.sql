-- Active: 1735622010150@@localhost@5432@adventureworks
--Initial Setup
    --create the adventureworks schema
    CREATE SCHEMA "adventureworks";

    --move the tables to the adventureworks schema
    ALTER TABLE public."Product" SET SCHEMA "adventureworks";
    ALTER TABLE public."Region" SET SCHEMA "adventureworks";
    ALTER TABLE public."Reseller" SET SCHEMA "adventureworks";
    ALTER TABLE public."Sales" SET SCHEMA "adventureworks";
    ALTER TABLE public."Salesperson" SET SCHEMA "adventureworks";
    ALTER TABLE public."SalespersonRegion" SET SCHEMA "adventureworks";
    ALTER TABLE public."Targets" SET SCHEMA "adventureworks";
    ALTER TABLE public."defects" SET SCHEMA "adventureworks";

    -- Set primary keys for each table
    ALTER TABLE "adventureworks"."Product"
    ADD CONSTRAINT "PK_Product" PRIMARY KEY ("ProductKey");

    ALTER TABLE "adventureworks"."Region"
    ADD CONSTRAINT "PK_Region" PRIMARY KEY ("SalesTerritoryKey");

    ALTER TABLE "adventureworks"."Reseller"
    ADD CONSTRAINT "PK_Reseller" PRIMARY KEY ("ResellerKey");

    ALTER TABLE "adventureworks"."Salesperson"
    ADD CONSTRAINT "PK_Salesperson" PRIMARY KEY ("EmployeeKey");

    -- fix the date fields
    -- Remove day names and convert verbose dates to YYYY-MM-DD
    UPDATE "adventureworks"."Sales"
    SET "OrderDate" = TO_CHAR(
        TO_DATE(REGEXP_REPLACE("OrderDate", '^[A-Za-z]+, ', ''), 'Month DD, YYYY'),
        'YYYY-MM-DD'
    );

    -- Change the column type to DATE
    ALTER TABLE "adventureworks"."Sales"
    ALTER COLUMN "OrderDate" TYPE DATE USING "OrderDate"::DATE;

    UPDATE "adventureworks"."Targets"
    SET "TargetMonth" = TO_CHAR(
        TO_DATE(REGEXP_REPLACE("TargetMonth", '^[A-Za-z]+, ', ''), 'Month DD, YYYY'),
        'YYYY-MM-DD'
    );

    -- Change the column type to DATE
    ALTER TABLE "adventureworks"."Targets"
    ALTER COLUMN "TargetMonth" TYPE DATE USING "TargetMonth"::DATE;

-- Modify Sales Table
    -- Add the new PK column to the sales table
    ALTER TABLE "adventureworks"."Sales"
    ADD COLUMN "SalesOrderKey" TEXT;

    -- Populate the new column for existing rows
    UPDATE "adventureworks"."Sales"
    SET "SalesOrderKey" = CONCAT("SalesOrderNumber", '.', "ProductKey");

    -- Drop the existing primary key (if any) and set the new column as the primary key
    ALTER TABLE "adventureworks"."Sales"
    DROP CONSTRAINT IF EXISTS "PK_Sales";

    ALTER TABLE "adventureworks"."Sales"
    ADD CONSTRAINT "PK_SalesOrderKey" PRIMARY KEY ("SalesOrderKey");

    -- Create a trigger function to populate SalesOrderKey automatically
    CREATE OR REPLACE FUNCTION populate_sales_order_key()
    RETURNS TRIGGER AS $$
    BEGIN
    NEW."SalesOrderKey" := CONCAT(NEW."SalesOrderNumber", '.', NEW."ProductKey");
    RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Create the trigger to execute the function before insert or update
    CREATE TRIGGER "trg_populate_sales_order_key"
    BEFORE INSERT OR UPDATE ON "adventureworks"."Sales"
    FOR EACH ROW
    EXECUTE FUNCTION populate_sales_order_key();

    --create a margin field
    ALTER TABLE "adventureworks"."Sales"
    ADD COLUMN "Margin" NUMERIC;

    -- populate the margin field
    UPDATE "adventureworks"."Sales"
    SET "Margin" = "Sales" - "Cost";

    -- Sales Summary Table
    -- Create the SalesOrders summary table
    CREATE TABLE "adventureworks"."SalesOrders" (
        "SalesOrderNumber" TEXT PRIMARY KEY,
        "OrderDate" DATE,
        "OrderedProducts" INT,
        "EmployeeKey" INT,
        "OrderedQuantity" INT,
        "TotalPrice" NUMERIC,
        "TotalSales" NUMERIC,
        "TotalCost" NUMERIC,
        "TotalMargin" NUMERIC
    );

    -- Populate the SalesOrders summary table
    INSERT INTO "adventureworks"."SalesOrders" (
        "SalesOrderNumber",
        "OrderDate",
        "OrderedProducts",
        "EmployeeKey",
        "OrderedQuantity",
        "TotalPrice",
        "TotalSales",
        "TotalCost",
        "TotalMargin"
    )
    SELECT 
        "SalesOrderNumber",
        MIN("OrderDate") AS "OrderDate",
        COUNT("ProductKey") AS "OrderedProducts",
        MIN("EmployeeKey"::INTEGER) AS "EmployeeKey",
        SUM("Quantity") AS "OrderedQuantity",
        SUM("Unit Price") AS "TotalPrice",
        SUM("Sales") AS "TotalSales",
        SUM("Cost") AS "TotalCost",
        SUM("Sales") - SUM("Cost") AS "TotalMargin"
    FROM "adventureworks"."Sales"
    GROUP BY "SalesOrderNumber";

-- Foreign Keys
    -- Add Foreign Key: ProductKey TO Product.ProductKey with cascading deletion
    ALTER TABLE "adventureworks"."Sales"
    ADD CONSTRAINT "FK_Sales_Product"
    FOREIGN KEY ("ProductKey")
    REFERENCES "adventureworks"."Product" ("ProductKey")
    ON DELETE CASCADE;

    -- Add Foreign Key: ResellerKey TO Reseller.ResellerKey with cascading deletion
    ALTER TABLE "adventureworks"."Sales"
    ADD CONSTRAINT "FK_Sales_Reseller"
    FOREIGN KEY ("ResellerKey")
    REFERENCES "adventureworks"."Reseller" ("ResellerKey")
    ON DELETE CASCADE;

    -- Add Foreign Key: EmployeeKey TO Salesperson.EmployeeKey with cascading deletion
    ALTER TABLE "adventureworks"."Sales"
    ADD CONSTRAINT "FK_Sales_Employee"
    FOREIGN KEY ("EmployeeKey")
    REFERENCES "adventureworks"."Salesperson" ("EmployeeKey")
    ON DELETE CASCADE;

    -- Add Foreign Key: SalesTerritoryKey TO Region.SalesTerritoryKey with cascading deletion
    ALTER TABLE "adventureworks"."Sales"
    ADD CONSTRAINT "FK_Sales_Territory"
    FOREIGN KEY ("SalesTerritoryKey")
    REFERENCES "adventureworks"."Region" ("SalesTerritoryKey")
    ON DELETE CASCADE;

    -- Add Foreign Key: SalesOrder TO Salesperson.EmployeeID with cascading deletion
    ALTER TABLE "adventureworks"."Sales"
    ADD CONSTRAINT "FK_Sales_SalesOrders"
    FOREIGN KEY ("SalesOrderNumber")
    REFERENCES "adventureworks"."SalesOrders" ("SalesOrderNumber")
    ON DELETE CASCADE;