{% test table_not_empty(model) %}
    SELECT ROW_COUNT
        FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{{model.schema.upper()}}'
        AND TABLE_NAME = '{{model.identifier.upper()}}'
        AND ROW_COUNT=0
{% endtest %}