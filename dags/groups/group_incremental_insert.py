from airflow.utils.task_group import TaskGroup
from scripts.common import incremental_sql_queries
from airflow.operators.postgres_operator import PostgresOperator


def incremental_insert_task(redshift_conn_id: str = ""):
    """
    Tasks perform inserting full data into product data warehouse tables

    :param redshift_conn_id: name of connection in Airflow used to connect to Redshift
    """

    with TaskGroup(
        "insert_data_to_product_tables",
        tooltip="Incremental load data to product tables",
    ) as group_incremental_insert:

        insert_dim_table_task = PostgresOperator(
            task_id="incremental_insert_dim_tables",
            postgres_conn_id=redshift_conn_id,
            sql=incremental_sql_queries.dim_tables_incremental_insert,
        )

        insert_bridge_table_task = PostgresOperator(
            task_id="incremental_insert_bridge_table",
            postgres_conn_id=redshift_conn_id,
            sql=incremental_sql_queries.category_group_table_incremental_insert,
        )

        insert_fact_table_task = PostgresOperator(
            task_id="incremental_insert_fact_table",
            postgres_conn_id=redshift_conn_id,
            sql=incremental_sql_queries.incremental_load_fact_incident,
        )
        insert_dim_table_task >> insert_bridge_table_task >> insert_fact_table_task

        return group_incremental_insert
