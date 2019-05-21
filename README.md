# AirFlow Standalone REST API

I have created this API for a specific usecase, which required the ability to view DAGs and TASKs based on owners/teams. Also, the ability to perform all the actions via REST calls.

This is a standalone API, which does not require AirFlow webserver to run. Although, you do have to have AirFlow installed and have a setup via Celery. If you do not have Celery setup, you can remove the "WorkerView" class, which uses it to get details about all the workers and queues.

It is also configured to use gevent server, but you are free to change your configuration.

## Prerequisites:

Python Libraries:
* airflow==1.9+
* celery
* flask
* flask_restful
* flask_mail
* sqlalchemy
* pendulum

Enviromental Variables:

* AIRFLOW_LOG_PATH
* AIRFLOW__CELERY__BROKER_URL
* AIRFLOW__CELERY__RESULT_BACKEND


## Actions:

DAG:

Enable DAG:
http://HOST_URL/airflow/api/v0.1/dagcontrol?dag_id=<dag_id>&action=enable

Disable DAG:
http://HOST_URL/airflow/api/v0.1/dagcontrol?dag_id=<dag_id>&action=disable

TASK:

Kill TASK:
http://HOST_URL/airflow/api/v0.1/taskcontrol?dag_id=<dag_id>&task_id=<task_id>&execution_date=<execution_date>&action=kill

Restart Aborted TASK:
http://HOST_URL/airflow/api/v0.1/taskcontrol?dag_id=<dag_id>&task_id=<task_id>&execution_date=<execution_date>&action=restart&options=<upstream,downstream>

Mark Failed TASK Success:
http://HOST_URL/airflow/api/v0.1/taskcontrol?dag_id=<dag_id>&task_id=<task_id>&execution_date=<execution_date>&action=mark_success&options=<upstream,downstream>

## View DAG/TASK Stats in JSON:

DAG:

View Dags, filtered by owners:
http://HOST_URL/airflow/api/v0.1/dagview?owners=<owners>

TASK:

View Running Tasks that are in running and success states, filtered by owners:
http://HOST_URL/airflow/api/v0.1/taskview?owners=<owners>

## Get Latest Log for specific run:

http://HOST_URL/airflow/api/v0.1/getlog?dag_id=<dag_id>&task_id=<task_id>&execution_date=<execution_date>

## Celery Worker view, filtered by owners:
http://HOST_URL/airflow/api/v0.1/workerview?owners=<owners>

## Long Running Tasks, filtered by owners:
http://HOST_URL/airflow/api/v0.1/longrunning?owners=<owners>


This is still a work in progress project and once it is more matured, will submit it as PR to the AirFlow official release.