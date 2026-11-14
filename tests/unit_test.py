
def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"


def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars

    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"


def test_dags_integrity(dagbag):

    # 1. Check DAG import errors
    assert dagbag.import_errors == {}, (
        f"Import errors found: {dagbag.import_errors}"
    )

    print("\n========== DAG IMPORT ERRORS ==========")
    print(dagbag.import_errors)

    # 2. Check expected DAG IDs
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())

    print("\n========== LOADED DAG IDS ==========")
    print(loaded_dag_ids)

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, (
            f"DAG {dag_id} is missing. Loaded DAGs: {loaded_dag_ids}"
        )

    # 3. Check total number of DAGs
    assert dagbag.size() == 3, (
        f"Expected 3 DAGs, but found {dagbag.size()}"
    )

    print("\n========== TOTAL DAG COUNT ==========")
    print(dagbag.size())

    # 4. Check individual DAG task counts
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }

    print("\n========== DAG TASK COUNTS ==========")

    for dag_id, expected_count in expected_task_counts.items():
        dag = dagbag.dags[dag_id]
        actual_count = len(dag.tasks)

        assert actual_count == expected_count, (
            f"DAG {dag_id} has {actual_count} tasks, "
            f"expected {expected_count}."
        )

        print(f"{dag_id}: {actual_count} tasks")
