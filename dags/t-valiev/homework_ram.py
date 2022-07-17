import logging
from pathlib import Path

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

# ...plugins.
from t_valiev_plugins.ram import TValievRamSectionOperator

DEFAULT_ARGS = {
    "start_date": days_ago(1),
    "owner": "t-valiev",
}

dag_id = Path(__file__).parent.stem + "__" + Path(__file__).stem

with DAG(
    dag_id=dag_id,
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
    max_active_runs=1,
    tags=["t-valiev"],
    ) as dag:

    get_locations_task = TValievRamSectionOperator(
        task_id="get_locations",
        conn_id="dina_ram",
        section="location",
        n=3,
        key=lambda x: len(x["residents"]),
        reverse=True,
        do_xcom_push=True,
    )

    create_locations_table_task = PostgresOperator(
        task_id="create_locations_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
        create TABLE IF not EXISTS t_valiev_ram_locations(
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            type VARCHAR(100),
            dimension VARCHAR(100),
            resident_cnt INTEGER
        );
        """,
    )

    clear_table_task = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            TRUNCATE TABLE t_valiev_ram_locations;
        """,
    )

    def fill_table(**kwargs):
        hook = PostgresHook(
            postgres_conn_id="conn_greenplum_write",
        )

        SQL_INSERT = """
            INSERT INTO t_valiev_ram_locations VALUES
                ({id}, '{name}', '{type}', '{dimension}', {resident_cnt});
        """

        selected_locations = kwargs["templates_dict"]["selected_locations"]
        for loc in selected_locations:
            format_dict = {
                    "id": loc["id"],
                    "name": loc["name"],
                    "type": loc["type"],
                    "dimension": loc["dimension"],
                    "resident_cnt": len(loc["residents"]),
            }
            logging.info(format_dict)
            hook.run(SQL_INSERT.format(**format_dict), False)

    fill_table_task = PythonOperator(
        task_id="fill_table",
        python_callable=fill_table,
        templates_dict={"selected_locations": '{{ ti.xcom_pull(task_ids="get_locations") }}'},
        provide_context=True,
    )

    check_table_task = PostgresOperator(
        task_id="check_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            SELECT * FROM t_valiev_ram_locations LIMIT 10;
        """,
        do_xcom_push=True,
    )

    create_locations_table_task >> clear_table_task
    [get_locations_task, clear_table_task] >> fill_table_task >> check_table_task