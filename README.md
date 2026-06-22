# Ash Shahada Repository

## Airflow Dag Processor Watchdog

The dag-processor is supervised by `airflow/watch_dag_processor.sh`.

Check watchdog and dag-processor status:

```bash
cd /home/asha && pgrep -af "watch_dag_processor.sh|/home/asha/airflow_env/bin/airflow dag-processor"
```

Check recent watchdog events:

```bash
cd /home/asha && tail -n 40 airflow/airflow-dag-processor-watchdog.log
```

Quick recovery (restart watchdog + parser):

```bash
cd /home/asha && pgrep -f "watch_dag_processor.sh|/home/asha/airflow_env/bin/airflow dag-processor" | xargs -r kill -9 && nohup /home/asha/airflow/watch_dag_processor.sh >> /home/asha/airflow/airflow-dag-processor-watchdog.log 2>&1 < /dev/null &
```