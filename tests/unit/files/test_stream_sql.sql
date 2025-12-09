MERGE INTO default.test_upsert_table AS t
USING test AS s
    ON s.id = t.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
