{% macro get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=database,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}

{#
    Checks if the relation contains all the required fields with the right type for a hub table
#}
{% macro is_valid_hub_relation(target_relation, surrogate_key_type) %}
    {% do log("Check if '" ~ target_relation ~ "' is a valid hub relation") %}
    {% set columns = adapter.get_columns_in_relation(target_relation) %}

    {% if surrogate_key_type == 'hash'  %}
        {% set key_type = 'TEXT' %}
    {% else %}
        {% set key_type = 'NUMBER' %}
    {% endif %} 

    {% set col_names = [] %}
    {% set col_types = {} %}
    {% for column in columns %}
        {% do col_names.append(column.name.upper()) %}
        {% do col_types.update({column.name.upper(): column.dtype}) %} 
    {% endfor %}

    {% if (
           ('ID' in col_types) and col_types['ID']==key_type and 
           ('KEY' in col_types) and col_types['KEY']=='TEXT' and 
           ('LOAD_TS' in col_types) and col_types['LOAD_TS'] == 'TIMESTAMP_LTZ' and 
           ('SOURCE_SYSTEM' in col_types) and col_types['SOURCE_SYSTEM'] == 'TEXT' and 
           ('LAST_SEEN_TS' in col_types) and col_types['LAST_SEEN_TS'] == 'TIMESTAMP_LTZ' and 
           ('LAST_SEEN_SYSTEM' in col_types) and col_types['LAST_SEEN_SYSTEM']== 'TEXT') -%}
        {% do return(True) %}
    {% else %}
        {% do return(False) %}
    {% endif %}
{% endmacro %}

{#
    Create a temp table
#}
{% macro build_hub_temp_table(tmp_relation, surrogate_key_type, business_key, load_field_name, source_system_field_name, sql) %}
    {% do log("Build hub temp table '" ~ tmp_relation ~ "'") %}
    {% if source_system_field_name is not none  %}
        {% set src_sys_field_name = source_system_field_name %}
    {% else %}
        {% set src_sys_field_name = "'UNKNOWN'" %}
    {% endif %} 

    {% if load_field_name is not none  %}
        {% set load_ts_field_name = load_field_name %}
    {% else %}
        {% set load_ts_field_name = 'CURRENT_TIMESTAMP' %}
    {% endif %} 

    CREATE OR REPLACE TEMP TABLE {{ tmp_relation }} AS
    SELECT key, load_ts, MIN(source_system) source_system
    FROM
        (SELECT {{ business_key }} AS key, 
                {{ src_sys_field_name }} AS source_system, 
                MAX({{ load_ts_field_name }}) load_ts
         FROM ({{ sql }}) AS src
         GROUP BY {{ business_key }}, {{ src_sys_field_name }})
    GROUP BY key, load_ts
{% endmacro %}

{# 
   Creates (or replaces it if exists) the target relation
   TODO: check if the Snowflake specific SQL can be replaced by dbt macro 
#}
{% macro create_dv_hub(relation, surrogate_key_type) %}
    {% do log("Build data vault hub table '" ~ relation ~ "'") %}

    {% if surrogate_key_type == 'hash'  %}
        {% set key_type = "STRING" %}
    {% else %}
        {% set key_type = "NUMBER" %}
    {% endif %} 

    CREATE OR REPLACE TABLE {{ relation }} (
        id                {{ key_type }},
        key               STRING,
        load_ts	          TIMESTAMP_LTZ,
        source_system     STRING,
        last_seen_ts      TIMESTAMP_LTZ,
        last_seen_system  STRING,
        CONSTRAINT {{ relation.identifier }}_pk PRIMARY KEY (id),
        CONSTRAINT {{ relation.identifier }}_uk UNIQUE (key)
    );
    CREATE SEQUENCE IF NOT EXISTS {{ relation.schema }}.hub_seq;
{% endmacro %}

{# 
   Creates (or replaces it if exists) the target relation
#}
{% macro hub_merge_sql(temp_relation, target_relation, surrogate_key_type) %}
    {% do log("Merge data vault hub temp table '" ~ temp_relation ~ "' into '" ~ target_relation ~ "'") %}

    {% if surrogate_key_type == 'hash'  %}
        {% set key_expr = "MD5_HEX(s.key)" %}
    {% else %}
        {% set key_expr = temp_relation.schema ~ ".hub_seq.nextval" %}
    {% endif %} 

    MERGE INTO {{ target_relation }} dst
    USING (SELECT {{ key_expr }} id,
                s.key,
                NVL(t.load_ts, s.load_ts) load_ts,
                NVL(t.source_system, s.source_system) source_system,
                s.load_ts last_seen_ts,
                s.source_system last_seen_system
        FROM {{ temp_relation }} s
        LEFT JOIN {{ target_relation }} t ON s.key=t.key) src
    ON src.key=dst.key
    WHEN MATCHED THEN UPDATE SET 
        dst.last_seen_ts     = src.last_seen_ts,
        dst.last_seen_system = src.last_seen_system
    WHEN NOT MATCHED THEN INSERT (id, key, load_ts, source_system, last_seen_ts, last_seen_system)
                        VALUES (src.id, src.key, src.load_ts, src.source_system, src.last_seen_ts, src.last_seen_system)
{% endmacro %}


{% materialization dv_hub, default %}

    {%- set config = model.get('config') -%}
    {%- set sql = model.get('injected_sql') -%}
    
    {%- set target_database = model.get('database') -%}
    {%- set target_schema = model.get('schema') -%}
    {%- set target_table = model.get('alias', model.get('name')) -%}
    {%- set temp_table = target_table ~ "__DBT_TMP" -%}

    {%- set surrogate_key_type = config.get('surrogate_key_type', 'sequence') -%}
    {%- set business_key = config.get('business_key', 'key') -%}
    {%- set load_field_name = config.get('load_field_name') -%}
    {%- set source_system_field_name = config.get('source_system_field_name') -%}

    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

    {%- set old_relation = adapter.get_relation(database=target_database, schema=target_schema, identifier=target_table) -%}
    {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}
    {%- set should_drop = (full_refresh_mode or exists_not_as_table) -%}

    {%- if old_relation is not none and should_drop -%}
        {{ adapter.drop_relation(old_relation) }}
        {%- set old_relation = none -%}
    {%- endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if not adapter.check_schema_exists(target_database, target_schema) %}
        {% do create_schema(target_database, target_schema) %}
    {% endif %}

    {% set target_relation_exists, target_relation = get_or_create_relation(
        database=target_database,
        schema=target_schema,
        identifier=target_table,
        type='table') -%}

    {% if full_refresh_mode or not target_relation_exists -%}
        {%- call statement() -%}
            {{ create_dv_hub(target_relation, surrogate_key_type) }}
        {%- endcall -%}
    {% endif %}

    {% do log("MAIN target_relation '" ~ target_relation ~ "'") %}

    {%- if not is_valid_hub_relation(target_relation) -%}
        {% do exceptions.relation_wrong_type(target_relation, 'hub') %}
    {%- endif -%}

    {%- set temp_relation = api.Relation.create(database=target_relation.database,
                                                schema=target_relation.schema,
                                                identifier=target_relation.identifier ~ "__dbt_tmp",
                                                type=target_relation.type
                                              ) %}
    {% call statement() %}
        {{ build_hub_temp_table(temp_relation, surrogate_key_type, business_key, load_field_name, source_system_field_name, sql) }}
    {% endcall %}

    {% call statement('main') %}
        {{ hub_merge_sql(temp_relation, target_relation, surrogate_key_type) }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    
    {% do return({'relations': [target_relation]}) %}
{% endmaterialization %}