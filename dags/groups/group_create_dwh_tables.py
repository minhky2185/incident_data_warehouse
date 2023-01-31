from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from scripts.common import full_sql_queries


def create_dwh_tables_task(redshift_conn_id: str = ""):
    """
    Init schemas and create corresponding tables in data warehouse

    :param redshift_conn_id: name of airflow connection to redshfit
    """
    with TaskGroup(
        "create_dwh_tables", tooltip="Create dwh tables"
    ) as group_create_dwh_tables:

        with TaskGroup(
            "staging_tables", tooltip="Create staging tables"
        ) as group_stg_tables:

            init_stg_schema_task = PostgresOperator(
                task_id="init_stg_schema",
                postgres_conn_id=redshift_conn_id,
                sql=full_sql_queries.stg_schema_create,
            )

            create_stg_tables_task = PostgresOperator(
                task_id="create_stg_tables",
                postgres_conn_id=redshift_conn_id,
                sql=full_sql_queries.stg_tables_create,
            )

            init_stg_schema_task >> create_stg_tables_task

        with TaskGroup(
            "product_tables", tooltip="Create product tables"
        ) as group_prod_tables:

            init_prod_schema_task = PostgresOperator(
                task_id="init_prod_schema",
                postgres_conn_id=redshift_conn_id,
                sql=full_sql_queries.prod_schema_create,
            )

            create_prod_tables_task = PostgresOperator(
                task_id="create_prod_tables",
                postgres_conn_id=redshift_conn_id,
                sql=full_sql_queries.prod_tables_create,
            )

            init_prod_schema_task >> create_prod_tables_task

        group_stg_tables >> group_prod_tables

        return group_create_dwh_tables
