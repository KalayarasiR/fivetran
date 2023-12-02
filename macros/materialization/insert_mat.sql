{%- materialization insert_mat, default -%}
  {% set src_pk = config.get('src_pk') %}
  {% set src_database = config.get('source_database') %}
  {% set src_schema = config.get('source_schema') %}
  {% set src_table = config.get('source_table') %}

  {% call statement('main') -%}
    {{ insert_into(
      key_column_src = src_pk, key_column_tgt = src_pk,tgt_table = this.table, src_database = src_database, src_schema = src_schema, src_table = src_table ) }}
  {%- endcall %}
  {{ return({'relations': [this]}) }}

{%- endmaterialization -%}