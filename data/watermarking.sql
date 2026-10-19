CREATE TABLE ops.watermarktable (
    TableName VARCHAR(255),
    watermarkvalue datetime
)

INSERT INTO ops.watermarktable VALUE ('moon.fact_sales', '2026-07-01 12:00:00 AM')

--STORED PROCEDURE TO UPDATE THE watermark value in the TABLE

CREATE PROCEDURE update_watermarking_table @LastModifiedTime datetime, @TableName VARCHAR(100) AS

BEGIN
    UPDATE ops.watermarktable
    SET [watermarkvalue] = @LastModifiedTime
    WHERE [TableName] = @TableName
END


