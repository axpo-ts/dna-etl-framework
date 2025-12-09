MERGE INTO {target_catalog_name}.{target_schema_name}.{target_table_name} AS t  -- noqa
USING {source_table_name} AS s ON t.catalog_name = s.catalog_name -- noqa
AND t.schema_name = '{target_schema_name}' -- noqa
AND t.table_name = s.table_name
AND t.column_name = s.column_name
AND t.proposed_tag_key = s.proposed_tag_key

WHEN MATCHED THEN
UPDATE
SET
  t.column_comment = s.column_comment,
  t.proposed_tag_value = s.proposed_tag_value,
  t.UpdatedAt = CURRENT_TIMESTAMP(),
  t.UpdatedBy = CURRENT_USER()
  WHEN NOT MATCHED THEN
INSERT (catalog_name, schema_name, table_name, column_name, column_comment, proposed_tag_key, proposed_tag_value, CreatedAt, CreatedBy, UpdatedAt, UpdatedBy))
  VALUES (
  s.catalog_name,
  '{target_schema_name}', -- noqa
  s.table_name,
  s.column_name,
  s.column_comment,
  s.proposed_tag_key,
  s.proposed_tag_value,
  s.CreatedAt,
  s.CreatedBy,
  s.UpdatedAt,
  s.UpdatedBy
  )