def deadline_missed_callback(**kwargs):
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run", {})
    deadline = context.get("deadline", {})

    print(
        "DEADLINE MISSED: "
        f"dag_id={getattr(dag_run, 'dag_id', 'unknown')} "
        f"run_id={getattr(dag_run, 'run_id', 'unknown')} "
        f"deadline={getattr(deadline, 'deadline_time', 'unknown')}"
    )
