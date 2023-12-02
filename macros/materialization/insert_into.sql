{% macro insert_into(key_column_src, key_column_tgt, tgt_database, tgt_schema, tgt_table, src_database, src_schema, src_table) %}
{% set target_relation = api.Relation.create(database=this.database, schema=this.schema, identifier=this.identifier) %}
{% set table_exists = adapter.get_relation(target_relation.database, target_relation.schema, target_relation.identifier) is not none %}

{% if not table_exists %}
    CREATE TABLE {{ this.database }}.{{ this.schema }}.{{ this.table }} AS
    SELECT
        *
    FROM {{ src_database }}.{{ src_schema }}.{{ src_table }}
    WHERE 1 = 0; 
{% endif %}
{%- set columns_info = adapter.get_columns_in_relation(target_relation) -%}
{%- set columns_list = columns_info | map(attribute="column") -%}
{%- set columns_quoted = [] -%}
{% for column in columns_list %}
  {%- set column_quoted = adapter.quote(column) -%}
  {%- do columns_quoted.append(column_quoted) -%}
{% endfor %}
{%- set tgt_columns_csv = columns_quoted | join(',') -%}

INSERT INTO {{ target_relation }} (
    {{ tgt_columns_csv }}
)
SELECT 
    {{ tgt_columns_csv }}
FROM {{ src_database }}.{{ src_schema }}.{{ src_table }} src
WHERE NOT EXISTS (
    SELECT NULL
    FROM (
        SELECT {{key_column_src}},
            LOAD_DTS,
            LEAD(LOAD_DTS) OVER (PARTITION BY {{key_column_src}} ORDER BY LOAD_DTS ASC) EFFECTIVE_END_DTS
        FROM {{ this.database }}.{{ this.schema }}.{{ this.table }}
    ) S
    WHERE S.EFFECTIVE_END_DTS IS NULL
    AND S.{{ key_column_tgt }} = src.{{ key_column_src }}
);
{% endmacro %}
