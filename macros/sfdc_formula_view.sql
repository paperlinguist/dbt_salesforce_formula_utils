{%- macro sfdc_formula_view(source_table, source_name='salesforce', materialization='view', using_quoted_identifiers=False, full_statement_version=true, reserved_table_name=none, fields_to_include=none) -%}
-- Best practice for this model is to be materialized as view. That is why we have set that here.
{{
    config(
        materialized = materialization
    )
}}
-- Raise a warning if users are trying to use full_statement_version=false. We are keeping the variable in the macro, however, since we don't want errors if they previously set it to true.
{% if not full_statement_version %}
    {{ exceptions.warn("\nERROR: The full_statement_version=false, reserved_table_name, and fields_to_include parameters are no longer supported. Please update your " ~ this.identifier|upper ~ " model to remove these parameters.\n") }}
{% else %}
    {# Validate required parameters #}
    {% if not source_table %}
        {{ exceptions.raise_compiler_error("source_table parameter is required for sfdc_formula_view macro") }}
    {% endif %}
    
    {# Check if running with --empty flag #}
    {% set is_empty_run = flags.EMPTY | default(false) %}
    
    {# During parsing, provide a placeholder - the real logic runs during execution #}
    {% if not execute %}
        select * from {{ source(source_name, source_table) }}
    {% else %}
        {# Execution phase - get the relation object #}
        {% set formula_model_source = source(source_name, 'fivetran_formula_model') %}
        {% set _ = formula_model_source.render() %}
        
        {# Build fully qualified table name to bypass dbt's --empty wrapping #}
        {% set fully_qualified_table = formula_model_source.database ~ '.' ~ formula_model_source.schema ~ '.' ~ formula_model_source.identifier %}
        
        {# Build query to get the formula SQL from fivetran_formula_model #}
        {% if using_quoted_identifiers %}
            {% set column_name = '"MODEL"' if target.type in ('snowflake') else '"model"' if target.type in ('postgres', 'redshift') else '`model`' %}
            {% set where_clause = '"OBJECT" = ' if target.type in ('snowflake') else '"object" = ' if target.type in ('postgres', 'redshift') else '`object` = ' %}
        {% else %}
            {% set column_name = 'MODEL' if target.type in ('snowflake') else 'model' %}
            {% set where_clause = 'OBJECT = ' if target.type in ('snowflake') else 'object = ' %}
        {% endif %}
        
        {# Use the fully qualified table name directly instead of the relation object #}
        {% set query %}
            select {{ column_name }}
            from {{ fully_qualified_table }}
            where {{ where_clause }}'{{ source_table }}'
        {% endset %}
        
        {# Execute the query to get the formula SQL #}
        {% set results = run_query(query) %}
        
        {% if results and results.rows | length > 0 %}
            {% set formula_sql = results.rows[0][0] %}
            
            {# If running with --empty flag, wrap the formula SQL to return 0 rows #}
            {% if is_empty_run %}
select * from (
{{ formula_sql }}
) where false limit 0
            {% else %}
                {# Normal run - return full formula SQL as-is #}
{{ formula_sql }}
            {% endif %}
        {% else %}
            {# Fallback if no formula model found - return base source table #}
            select * from {{ source(source_name, source_table) }}
        {% endif %}
    {% endif %}
{% endif %}
{%- endmacro -%}
