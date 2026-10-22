UPDATE dbo.orders SET <col> = <val> WHERE <key> = <id>;


SELECT TOP(10) * FROM moon.fact_sales fs ORDER BY transaction_id DESC;

INSERT INTO moon.fact_sales
    (transaction_id, [date], volume, customer_code, distributor_code, product_id, last_modified)
VALUES
    ('TXN-08151', '2026-10-21', 48.5, 'CC-12', 'DIST-03', 'P03', SYSUTCDATETIME());


SELECT __$operation, __$start_lsn, transaction_id, volume, last_modified
FROM cdc.moon_fact_sales_CT
ORDER BY __$start_lsn;


UPDATE moon.fact_sales SET volume = 55.0 WHERE transaction_id = 'TXN-08151';
DELETE FROM moon.fact_sales WHERE transaction_id = 'TXN-08151';