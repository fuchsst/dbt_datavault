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
    Checks if the relation contains all the required fields with the right type for a satellite table
#}
{% macro is_valid_satellite_relation(target_relation, 
                                     parent_table, 
                                     business_key, 
                                     link_fields, 
                                     secondary_pk_field, 
                                     load_field_name, 
                                     is_historical, 
                                     tombstone_field_name) %}
    {# TODO: add proper validation #}
    {% do return(True) %}
{% endmacro %}

{#
    Create a temp table
#}
{% macro build_satellite_temp_table(tmp_relation, 
                                    parent_table, 
                                    business_key, 
                                    link_fields, 
                                    secondary_pk_field, 
                                    load_field_name, 
                                    is_historical, 
                                    tombstone_field_name, sql) %}
    {% do log("Build satellite temp table '" ~ tmp_relation ~ "'") %}
{% endmacro %}

{# 
   Creates (or replaces it if exists) the target relation
   TODO: check if the Snowflake specific SQL can be replaced by dbt macro 
#}
{% macro create_dv_satellite(relation, 
                             parent_table, 
                             business_key, 
                             link_fields, 
                             secondary_pk_field, 
                             load_field_name, 
                             is_historical, 
                             tombstone_field_name) %}
    {% do log("Build data vault link table '" ~ relation ~ "'") %}
    {% if surrogate_key_type == 'hash'  %}
        {% set key_type = "STRING" %}
    {% else %}
        {% set key_type = "NUMBER" %}
    {% endif %} 
    CREATE OR REPLACE TABLE {{ relation }} (
        id                {{ key_type }},
        md5_hash          STRING,
        {% for link_field in link_fields %}
            {{ link_field.name }}  NUMBER, {# TODO: proper type for sequence/hash PK in hub table #}
        {% endfor %}
        load_ts	          TIMESTAMP_LTZ,
        {% if is_historical %}
        unload_ts         TIMESTAMP_LTZ,
        {% endfor %}
        CONSTRAINT {{ relation.identifier }}_pk PRIMARY KEY (id)
    );
    CREATE SEQUENCE IF NOT EXISTS {{relation.schema}}.{{relation.identifier}}_link_seq;
{% endmacro %}

{# 
   Creates (or replaces it if exists) the target relation
#}
{% macro satellite_merge_sql(temp_relation, 
                             target_relation, 
                             parent_table, 
                             business_key, 
                             link_fields, 
                             secondary_pk_field, 
                             load_field_name, 
                             is_historical, 
                             tombstone_field_name) %}
    {% do log("Merge data vault satellite temp table '" ~ temp_relation ~ "' into '" ~ target_relation ~ "'") %}
    {# TODO: merge logic #}
{% endmacro %}


{% materialization dv_satellite, default %}
    {% set config = model.get('config') %}
    {% set sql = model.get('injected_sql') %}
    
    {% set target_database = model.get('database') %}
    {% set target_schema = model.get('schema') %}
    {% set target_table = model.get('alias', model.get('name')) %}
    {% set temp_table = target_table ~ "__DBT_TMP" %}

    {% set surrogate_key_type = config.get('surrogate_key_type', 'sequence') %}
    
    {% set load_field_name = config.get('load_field_name', 'load_ts') %}
    {% set unload_field_name = config.get('load_field_name', 'unload_ts') %}
    {% set source_system_field_name = config.get('source_system_field_name') %}

    {% set full_refresh_mode = (flags.FULL_REFRESH == True) %}

    {% set old_relation = adapter.get_relation(database=target_database, schema=target_schema, identifier=target_table) %}
    {% set exists_as_table = (old_relation is not none and old_relation.is_table) %}
    {% set exists_not_as_table = (old_relation is not none and not old_relation.is_table) %}
    {% set should_drop = (full_refresh_mode or exists_not_as_table) %}

    {% set business_key = config.get('business_key') %}
    {% set link_fields = config.get('link_fields') %}

    {% if old_relation is not none and should_drop %}
        {{ adapter.drop_relation(old_relation) }}
        {% set old_relation = none %}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% if not adapter.check_schema_exists(target_database, target_schema) %}
        {% do create_schema(target_database, target_schema) %}
    {% endif %}

    {% set target_relation_exists, target_relation = get_or_create_relation(
          database=target_database,
          schema=target_schema,
          identifier=target_table,
          type='table') %}

    {% if full_refresh_mode or old_relation is none %}
        {% call statement() %}
            {{ create_dv_satellite(target_relation, surrogate_key_type, link_fields) }}
        {% endcall %}
    {% endif %}
    
    {% if target_relation_exists and not is_valid_satellite_relation(target_relation, surrogate_key_type, business_key, link_fields) %}
        {% do exceptions.relation_wrong_type(target_relation, 'satellite') %}
    {% endif %}

    {% set temp_relation = api.Relation.create(database=target_relation.database,
                                               schema=target_relation.schema,
                                               identifier=target_relation.identifier ~ "__DBT_TMP",
                                               type=target_relation.type
                                              ) %}
    {% call statement() %}
        {{ build_satellite_temp_table(temp_relation, surrogate_key_type, load_field_name, source_system_field_name, business_key, link_fields, sql) }}
    {% endcall %}

    {% call statement('main') %}
        {{ satellite_merge_sql(temp_relation, target_relation, surrogate_key_type, business_key, link_fields) }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}
    
    {% do return({'relations': [target_relation]}) %}
{% endmaterialization %}