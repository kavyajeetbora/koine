/* =====================================================================
   MOON distributor simulation - source database setup (Azure SQL / T-SQL)
   Schema + tables with a last_modified audit column. No CDC, no GO
   separators (portal Query editor safe).
   last_modified is the LAST column in every table, matching the CSV order.
   ===================================================================== */

-- Run against your source database (e.g. moon_source), not master.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'moon')
    EXEC('CREATE SCHEMA moon');

/* Drop in dependency order so the script is re-runnable */
DROP TABLE IF EXISTS moon.fact_sales;
DROP TABLE IF EXISTS moon.dim_customer;
DROP TABLE IF EXISTS moon.dim_distributor;
DROP TABLE IF EXISTS moon.dim_product;

/* ---------- Dimensions ---------- */

CREATE TABLE moon.dim_product (
    product_id     VARCHAR(10)  NOT NULL,
    product_name   VARCHAR(50)  NOT NULL,
    last_modified  DATETIME2(0) NOT NULL CONSTRAINT DF_dim_product_lm DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_dim_product PRIMARY KEY (product_id)
);

CREATE TABLE moon.dim_distributor (
    distributor_code  VARCHAR(10)   NOT NULL,   -- e.g. DIST-01
    distributor_name  VARCHAR(100)  NOT NULL,
    lat               DECIMAL(9,6)  NOT NULL,
    lon               DECIMAL(9,6)  NOT NULL,
    city              VARCHAR(50)   NOT NULL,
    last_modified     DATETIME2(0)  NOT NULL CONSTRAINT DF_dim_distributor_lm DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_dim_distributor PRIMARY KEY (distributor_code),
    CONSTRAINT CK_dist_city CHECK (city IN ('Delhi','Gurgaon','Noida'))
);

CREATE TABLE moon.dim_customer (
    customer_code  VARCHAR(10)   NOT NULL,      -- e.g. CC-01
    customer_name  VARCHAR(100)  NOT NULL,
    city           VARCHAR(50)   NOT NULL,
    lat            DECIMAL(9,6)  NOT NULL,
    lon            DECIMAL(9,6)  NOT NULL,
    vpo_class      VARCHAR(10)   NOT NULL,      -- Gold / Silver / Bronze
    outlet_type    VARCHAR(20)   NOT NULL,      -- QSR / Supermarket / E&D
    last_modified  DATETIME2(0)  NOT NULL CONSTRAINT DF_dim_customer_lm DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_dim_customer PRIMARY KEY (customer_code),
    CONSTRAINT CK_cust_city CHECK (city IN ('Delhi','Gurgaon','Noida')),
    CONSTRAINT CK_cust_vpo  CHECK (vpo_class IN ('Gold','Silver','Bronze')),
    CONSTRAINT CK_cust_type CHECK (outlet_type IN ('QSR','Supermarket','E&D'))
);

/* ---------- Fact ---------- */

CREATE TABLE moon.fact_sales (
    transaction_id    VARCHAR(15)    NOT NULL,  -- e.g. TXN-00001
    [date]            DATE           NOT NULL,
    volume            DECIMAL(12,1)  NOT NULL,
    customer_code     VARCHAR(10)    NOT NULL,
    distributor_code  VARCHAR(10)    NOT NULL,
    product_id        VARCHAR(10)    NOT NULL,
    last_modified     DATETIME2(0)   NOT NULL CONSTRAINT DF_fact_sales_lm DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_fact_sales PRIMARY KEY (transaction_id),
    CONSTRAINT FK_sales_customer    FOREIGN KEY (customer_code)    REFERENCES moon.dim_customer (customer_code),
    CONSTRAINT FK_sales_distributor FOREIGN KEY (distributor_code) REFERENCES moon.dim_distributor (distributor_code),
    CONSTRAINT FK_sales_product     FOREIGN KEY (product_id)       REFERENCES moon.dim_product (product_id),
    CONSTRAINT CK_sales_volume CHECK (volume > 0)
);

CREATE INDEX IX_fact_sales_date ON moon.fact_sales ([date]);
CREATE INDEX IX_fact_sales_customer ON moon.fact_sales (customer_code);
CREATE INDEX IX_fact_sales_last_modified ON moon.fact_sales (last_modified);

/* ---------- Triggers: keep last_modified current on UPDATE ----------
   The DEFAULT covers INSERTs. Nothing in SQL Server updates the column on
   UPDATE by itself, so each table gets an AFTER UPDATE trigger.
   CREATE TRIGGER must be first in its batch, hence the EXEC() wrappers.
   (Triggers fire for the app/portal, not for BULK INSERT, so the CSV load
   keeps the last_modified values from the files.) */

EXEC('CREATE OR ALTER TRIGGER moon.trg_dim_product_lm ON moon.dim_product AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE t SET last_modified = SYSUTCDATETIME()
  FROM moon.dim_product t JOIN inserted i ON i.product_id = t.product_id;
END');

EXEC('CREATE OR ALTER TRIGGER moon.trg_dim_distributor_lm ON moon.dim_distributor AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE t SET last_modified = SYSUTCDATETIME()
  FROM moon.dim_distributor t JOIN inserted i ON i.distributor_code = t.distributor_code;
END');

EXEC('CREATE OR ALTER TRIGGER moon.trg_dim_customer_lm ON moon.dim_customer AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE t SET last_modified = SYSUTCDATETIME()
  FROM moon.dim_customer t JOIN inserted i ON i.customer_code = t.customer_code;
END');

EXEC('CREATE OR ALTER TRIGGER moon.trg_fact_sales_lm ON moon.fact_sales AFTER UPDATE AS
BEGIN SET NOCOUNT ON;
  UPDATE t SET last_modified = SYSUTCDATETIME()
  FROM moon.fact_sales t JOIN inserted i ON i.transaction_id = t.transaction_id;
END');