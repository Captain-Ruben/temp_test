import random
from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from prefect_shell import ShellOperation

shell_operation = ShellOperation.load("shell-pandas")

FIRST_NAMES = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "James", "Emma", "Daniel", "Olivia", "William", "Sophia"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
STATUSES = ["Active", "Inactive", "Pending"]

@task(name="Run Shell Update")
def execute_update_block():
    # Laad het opgeslagen block uit Prefect
    shell_op = ShellOperation.load("shell-pandas")
    
    # Voer de operatie uit (dit runt de commando's die in het block staan)
    shell_op.run()

@task(name="Generate Fake Data")
def generate_fake_data(num_rows: int) -> list[dict]:
    """Generates fake data as a list of dictionaries."""
    start_date = datetime(2023, 1, 1)
    rows = []
    
    for i in range(1, num_rows + 1):
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        age = random.randint(18, 70)
        random_days = random.randint(0, 1000)
        registration_date = (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')
        status = random.choice(STATUSES)
        
        rows.append({
            "ID": i,
            "Name": name,
            "Age": age,
            "Registration Date": registration_date,
            "Status": status
        })
        
    return rows

@task(name="Publish Prefect Artifacts")
def publish_artifacts(data: list[dict]):
    """Publishes the generated data as table and markdown artifacts in the Prefect UI."""
    logger = get_run_logger()
    
    artifact_id = create_table_artifact(
        key="fake-data-table",
        table=data,
        description=f"Generated Fake Data ({len(data)} rows)"
    )
    logger.info(f"Table artifact created with ID: {artifact_id}")

    active_count = sum(1 for row in data if row["Status"] == "Active")
    inactive_count = sum(1 for row in data if row["Status"] == "Inactive")
    pending_count = sum(1 for row in data if row["Status"] == "Pending")
    
    summary_md = f"""
    # 📊 Fake Data Generation Summary
    
    - **Total Rows:** {len(data)}
    - **Active:** {active_count}
    - **Inactive:** {inactive_count}
    - **Pending:** {pending_count}
    """
    
    create_markdown_artifact(
        key="fake-data-summary",
        markdown=summary_md,
        description="Summary metrics for the generated dataset"
    )

@flow(name="fake-data-artifact-pipeline", log_prints=True)
def fake_data_artifact_pipeline(num_rows: int = 1000):
    """Prefect flow that orchestrates fake data generation and artifact publishing."""
    data = generate_fake_data(num_rows=num_rows)
    publish_artifacts(data=data)

if __name__ == "__main__":
    fake_data_artifact_pipeline(num_rows=1000)