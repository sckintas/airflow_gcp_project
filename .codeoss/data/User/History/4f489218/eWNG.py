import subprocess

def run_tests():
    # Run the tests using pytest
    print("Running tests...")
    result = subprocess.run(["pytest", "/app/etl_airflow/tests"], check=True)
    print("Tests completed with status code:", result.returncode)

def push_to_remote():
    # Push changes to the remote repository (Git)
    print("Pushing changes to the remote repository...")
    subprocess.run(["git", "add", "."], check=True)
    subprocess.run(["git", "commit", "-m", "Automated commit from CI/CD pipeline"], check=True)
    subprocess.run(["git", "push"], check=True)
    print("Changes pushed successfully.")

def trigger_airflow_dag(dag_id):
    # Trigger the Airflow DAG using the Airflow CLI
    print(f"Triggering Airflow DAG: {dag_id}")
    subprocess.run(["airflow", "dags", "trigger", dag_id], check=True)

def main():
    try:
        # Step 1: Run Tests
        run_tests()

        # Step 2: Push Changes to Remote Repository
        push_to_remote()

        # Step 3: Trigger Airflow DAG
        dag_id = "snowflake_to_gcs_and_bigquery_with_dbt"
        trigger_airflow_dag(dag_id)

        print("Pipeline execution completed successfully.")

    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()
