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
    Checks if the relation contains all the required fields with the right type for a link table
#}
{% macro is_valid_link_relation(target_relation, surrogate_key_type, link_fields) %}
    {% do log("Check if '" ~ target_relation ~ "' is a valid link relation") %}
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
           ('ID' in col_types) and col_types['ID'] == key_type and
           ('LOAD_TS' in col_types) and (col_types['LOAD_TS'] == 'TIMESTAMP_LTZ' or col_types['LOAD_TS'] == 'TIMESTAMP_NTZ') and
           ('SOURCE_SYSTEM' in col_types) and col_types['SOURCE_SYSTEM'] == 'TEXT' and
           ('LAST_SEEN_TS' in col_types) and (col_types['LAST_SEEN_TS'] == 'TIMESTAMP_LTZ' or col_types['LAST_SEEN_TS'] == 'TIMESTAMP_NTZ') and
           ('LAST_SEEN_SYSTEM' in col_types) and col_types['LAST_SEEN_SYSTEM'] == 'TEXT') %}
        {% for link_field in link_fields %}
            {% if ((link_field.name.upper() not in col_types) or col_types[link_field.name.upper()] != 'NUMBER') %}
                {% do return(False) %}
            {% endif %}
        {% endfor %}

        {% do return(True) %}
    {% else %}
        {% do return(False) %}
    {% endif %}
{% endmacro %}


{#
Create a temp table
#}
{% macro build_link_temp_table(tmp_relation, surrogate_key_type, load_field_name, source_system_field_name, link_fields, sql) %}
    {% do log("Build link temp table '" ~ tmp_relation ~ "'") %}
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

    {% set link_field_names = [] %}
    {% for link_field in link_fields %}
        {% do link_field_names.append(link_field.name) %}
    {% endfor %}

    CREATE OR REPLACE TEMP TABLE {{ tmp_relation }} AS
    SELECT DISTINCT {{ link_field_names|join(", ") }},
        FIRST_VALUE(source_system) IGNORE NULLS OVER (PARTITION BY {{ link_field_names|join(", ") }} ORDER BY load_ts ASC, source_system) AS source_system,
        FIRST_VALUE(source_system) IGNORE NULLS OVER (PARTITION BY {{ link_field_names|join(", ") }} ORDER BY load_ts DESC, source_system) AS last_seen_system,
        FIRST_VALUE(load_ts) IGNORE NULLS OVER (PARTITION BY {{ link_field_names|join(", ") }} ORDER BY load_ts ASC) AS load_ts,
        FIRST_VALUE(load_ts) IGNORE NULLS OVER (PARTITION BY {{ link_field_names|join(", ") }} ORDER BY load_ts DESC) AS last_seen_ts
    FROM (
            SELECT
                {% for link_field in link_fields %}
                {{link_field.hub}}_{{loop.index}}.id AS {{ link_field.name }},
                {% endfor %}
                src.{{ src_sys_field_name }} AS source_system,
                src.{{ load_ts_field_name }} AS load_ts
            FROM (
                    {{ sql }}
                ) src
            {% for link_field in link_fields %}
                {% set hub_schema = link_field.get(link_field.hub_schema, tmp_relation.schema) %}
                JOIN {{tmp_relation.database}}.{{hub_schema}}.{{link_field.hub}} AS {{link_field.hub}}_{{loop.index}} ON src.{{link_field.source_field_name}}={{link_field.hub}}_{{loop.index}}.key
            {% endfor %}
        )
{% endmacro %}

{#
    Creates (or replaces it if exists) the target relation
    TODO: check if the Snowflake specific SQL can be replaced by dbt macro
#}
{% macro create_dv_link(relation, surrogate_key_type, link_fields) %}
    {% do log("Build data vault link table '" ~ relation ~ "'") %}
    {% if surrogate_key_type == 'hash'  %}
        {% set key_type = "STRING" %}
    {% else %}
        {% set key_type = "NUMBER" %}
    {% endif %}
    CREATE OR REPLACE TABLE {{ relation }} (
        id                {{ key_type }},
    {% for link_field in link_fields %}
        {{ link_field.name }}  NUMBER, {# TODO: proper type for sequence/hash PK in hub table #}
    {% endfor %}
       load_ts	         TIMESTAMP_LTZ,
       source_system     STRING,
       last_seen_ts      TIMESTAMP_LTZ,
       last_seen_system  STRING,
       CONSTRAINT {{ relation.identifier }}_pk PRIMARY KEY (id)
    );
    CREATE SEQUENCE IF NOT EXISTS {{ relation.schema }}.link_seq;
{% endmacro %}

{#
   Creates (or replaces it if exists) the target relation
#}
{% macro link_merge_sql(temp_relation, target_relation, surrogate_key_type, link_fields) %}
    {% do log("Merge data vault link temp table '" ~ temp_relation ~ "' into '" ~ target_relation ~ "'") %}

    {% if surrogate_key_type == 'hash'  %}
        {# TODO: set proper hashkey based on link columns {% set key_expr = "MD5_HEX(s.key)" %} #}
    {% else %}
        {% set key_expr = temp_relation.schema ~ ".link_seq.nextval" %}
    {% endif %}

    {% set join_expressions = [] %}
    {% set refs = [] %}
    {% set src_field_list = [] %}
    {% for link_field in link_fields %}
        {% do join_expressions.append("src." ~ link_field.name ~ "=dst." ~ link_field.name) %}
        {% do src_field_list.append(link_field.name) %}
        {% do refs.append( ref(link_field.hub)) %}
    {% endfor %}

    MERGE INTO {{ target_relation }} dst
    USING ( SELECT {{ key_expr }} AS id,
            {{ src_field_list|join(", ") }},
            load_ts,
            source_system,
            last_seen_ts,
            last_seen_system
    FROM {{ temp_relation }}
    ) src
    ON {{ join_expressions|join(" AND ") }}
    WHEN MATCHED AND dst.last_seen_ts < src.last_seen_ts THEN
        UPDATE SET
            dst.last_seen_ts     = src.last_seen_ts,
            dst.last_seen_system = src.last_seen_system
    WHEN NOT MATCHED THEN
        INSERT (id, {% for fieldname in src_field_list %}{{ fieldname }}, {% endfor %}load_ts, source_system, last_seen_ts, last_seen_system)
        VALUES (src.id, src.{{ src_field_list|join(", src.") }}, src.load_ts, src.source_system, src.last_seen_ts, src.last_seen_system)
{% endmacro %}


{% materialization dv_link, default %}

    {%- set config = model.get('config') -%}
    {%- set sql = model.get('injected_sql') -%}

    {%- set target_database = model.get('database') -%}
    {%- set target_schema = model.get('schema') -%}
    {%- set target_table = model.get('alias', model.get('name')) -%}
    {%- set temp_table = target_table ~ "__DBT_TMP" -%}

    {%- set surrogate_key_type = config.get('surrogate_key_type', 'sequence') -%}

    {%- set load_field_name = config.get('load_field_name') -%}
    {%- set source_system_field_name = config.get('source_system_field_name') -%}

    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

    {%- set old_relation = adapter.get_relation(database=target_database, schema=target_schema, identifier=target_table) -%}
    {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}
    {%- set should_drop = (full_refresh_mode or exists_not_as_table) -%}

    {% set link_fields = config.get('link_fields') %}

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

    {% if full_refresh_mode or old_relation is none -%}
        {%- call statement() -%}
            {{ create_dv_link(target_relation, surrogate_key_type, link_fields) }}
        {%- endcall -%}
    {%- endif %}

    {%- if target_relation_exists and not is_valid_link_relation(target_relation, surrogate_key_type, link_fields) -%}
        {% do exceptions.relation_wrong_type(target_relation, 'link') %}
    {%- endif -%}

    {%- set temp_relation = api.Relation.create(database=target_relation.database,
                                                schema=target_relation.schema,
                                                identifier=target_relation.identifier ~ "__DBT_TMP",
                                                type=target_relation.type
                                                                                              ) %}
    {% call statement() %}
        {{ build_link_temp_table(temp_relation, surrogate_key_type, load_field_name, source_system_field_name, link_fields, sql) }}
    {% endcall %}

    {% call statement('main') %}
        {{ link_merge_sql(temp_relation, target_relation, surrogate_key_type, link_fields) }}
    {% endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {% do return({'relations': [target_relation]}) %}
{% endmaterialization %}