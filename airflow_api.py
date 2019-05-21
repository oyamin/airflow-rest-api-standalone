"""Webhook and API code for AirFlow data"""

import os
import re
import socket
from datetime import datetime
from collections import OrderedDict
from multiprocessing import Process
import statistics
import pendulum
from flask import Flask, jsonify
from airflow import settings
from airflow.models import DagModel, DagBag, DagRun, TaskInstance, clear_task_instances
from airflow.utils.state import State
from airflow.utils import timezone
from sqlalchemy import and_
from sqlalchemy.ext.declarative import declarative_base
from flask_restful import Resource, Api, reqparse
from celery import Celery
from gevent import server
from gevent.server import _tcp_listener
from gevent.pywsgi import WSGIServer

app = Flask(__name__)

LOG_PATH = os.environ['AIRFLOW_LOG_PATH']
TIMEZONE = "America/Los_Angeles"
HOST_NAME = socket.gethostbyaddr(socket.gethostname())[0]
HOST_URL = "http://{}".format(HOST_NAME)
INIT_URL = "/airflow/api/v0.1"

Base = declarative_base()


class Init(object):
    def __init__(self):
        self.session = settings.Session()
        self.DM = DagModel
        self.TI = TaskInstance
        self.DR = DagRun
        self.parse = reqparse.RequestParser()
        self.parse.add_argument('dag_id', type=str)
        self.parse.add_argument('task_id', type=str)
        self.parse.add_argument('execution_date', type=str)
        self.parse.add_argument('action', type=str)
        self.parse.add_argument('option', type=str)
        self.parse.add_argument('owners', type=str)
        self.args = self.parse.parse_args()
        self.cur_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.local_tz = pendulum.timezone(TIMEZONE)

    def datetime_iso(self, obj):
        """
        Converts datetime to local timezone in ISO Format.
        Removes microseconds.
        """
        try:
            if isinstance(obj, datetime):
                local_datetime = self.local_tz.convert(obj)
                local_datetime = local_datetime.replace(tzinfo=None)
                return local_datetime.replace(microsecond=0).isoformat()
        except TypeError:
            pass

    def seconds_to(self, obj):
        """
        Converts seconds to readable format: Day:Hour:Seconds
        """
        try:
            if isinstance(obj, datetime):
                obj = self.local_tz.convert(obj)
                current = self.local_tz.convert(datetime.now())
                obj = (current - obj).total_seconds()
            m, s = divmod(obj, 60)
            h, m = divmod(m, 60)
            run_format = "%d:%02d:%02d" % (h, m, s)
            return run_format
        except Exception as err:
            print(err)

    def __del__(self):
        self.session.close()


class DagControl(Resource, Init):
    """
    DagControl allows you to pause and unpause your dags.
    """
    def __init__(self):
        super().__init__()
        self.dag = self.args['dag_id']
        self.action = self.args['action'].lower()

    def get(self):
        """
        A way to programatically pause and unpause a DAG.
        :param dag_id: DAG object
        :param action: enable or disable
        :return: dag.is_paused is now False or True
        """
        if self.action not in ("enable", "disable"):
            return jsonify({"message": "Invalid Action. Please use 'enable' or 'disable'."})
        if self.action == "enable":
            q_action = False
        elif self.action == "disable":
            q_action = True
        try:
            query = self.session.query(self.DM).filter(self.DM.dag_id == self.dag).first()
        except Exception as err:
            self.session.rollback()
            return jsonify({"message": str(err)})
        else:
            if query.is_paused == q_action:
                return jsonify({"message": "DAG Status is already set to {}d.".format(self.action)})
            query.is_paused = q_action
            message = {"message": "{} DAG Status has been updated".format(self.dag)}
            self.session.commit()
            return jsonify(message)
        finally:
            self.session.close()


class DagView(Resource, Init):
    """
    Gives output of DAGs and their status, filtered by the owners.
    """
    def __init__(self):
        super().__init__()
        self.owners = self.args['owners'].lower()

    def get(self):
        """
        :param owners: DAG owners
        """
        data_list = []
        data = {}
        dags = self.session.query(self.DM).filter(self.DM.owners == self.owners).all()
        for dag in dags:
            if dag.is_paused is False:
                data.update({'state': 'Enabled'})
            else:
                data.update({'state': 'Disabled'})
            data_list.append(OrderedDict([
                ('state', data['state']),
                ('dag', dag.dag_id)]))
        return jsonify(data_list)


class TaskView(Resource, Init):
    """
    Gives output of running Tasks and their stats, filtered by the owners.
    """
    def __init__(self):
        super().__init__()
        self.owners = self.args['owners'].lower()

    def get(self):
        """
        :param owners: DAG owners
        """
        data_list = []
        data = {}
        ignore = ["success", "removed", "skipped"]
        cond = and_(
            self.TI.dag_id == self.DM.dag_id,
            self.TI.state.notin_(ignore),
            self.DM.owners == self.owners)
        tasks = self.session.query(
            self.TI.job_id,
            self.TI.dag_id,
            self.TI.task_id,
            self.TI._try_number,
            self.TI.execution_date,
            self.TI.start_date,
            self.TI.end_date,
            self.TI.queued_dttm,
            self.TI.hostname,
            self.TI.duration,
            self.TI.state,
            self.DM.owners).filter(cond).all()
        data_list = []
        for task in tasks:
            if task.duration is not None:
                data['duration'] = self.seconds_to(task.duration)
            else:
                data['duration'] = self.seconds_to(task.start_date)
            execution_date = self.datetime_iso(task.execution_date)
            start_date = self.datetime_iso(task.start_date)
            end_date = self.datetime_iso(task.end_date)
            data.update({"logfile": {
                "html_url":
                    "{}/getlog?dag_id={}&task_id={}&execution_date={}".format(
                        HOST_URL,
                        task.dag_id,
                        task.task_id,
                        execution_date
                        ),
                "summary": "Log File"}})
            data.update({"job_id": str(task.job_id) + "." + str(task._try_number).zfill(2)})
            data_list.append(OrderedDict([
                ('job_id', data['job_id']),
                ('dag_id', task.dag_id),
                ('task_id', task.task_id),
                ('duration', data['duration']),
                ('status', task.state),
                ('logfile', data['logfile']),
                ('hostname', task.hostname),
                ('execution_date', execution_date),
                ('start_date', start_date),
                ('end_date', end_date)]))
        ordered = sorted(data_list, key=lambda element: element['execution_date'], reverse=True)
        return jsonify(ordered)


class TaskControl(Resource, Init):
    """
    TaskControl allows you to stop, restart, and mark_success your tasks.
    """
    def __init__(self):
        super().__init__()
        self.dagbag = DagBag()
        self.dag = self.args['dag_id']
        self.task = self.args['task_id']
        self.execution_date = self.args['execution_date']
        self.action = self.args['action']
        self.options = self.args['option']
        self.execution_date = datetime.strptime(self.execution_date, "%Y-%m-%dT%H:%M:%S")
        self.execution_date = timezone.convert_to_utc(self.execution_date)
        assert timezone.is_localized(self.execution_date)

    def _set_dag_run_state(self, state):
        """
        Helper method that sets task state in the DB.
        """
        dr = self.session.query(self.DR).filter(
            self.DR.dag_id == self.dag,
            self.DR.execution_date == self.execution_date
        ).first()
        dr.state = state
        if state == State.RUNNING:
            dr.start_date = timezone.utcnow()
            dr.end_date = None
        else:
            dr.end_date = timezone.utcnow()
        self.session.merge(dr)
        self.session.commit()

    def get(self):
        """
        :param dag_id: dag_id of target dag run
        :param task_id: task_id of target task run
        :param execution_date: the execution date from which to start looking
        :param action: which action to perform
            - stop running task: kill
            - restart running task: restart
            - mark success: mark_success
        :param options (comma separated):
            - upstream
            - downstream
        """
        try:
            dag = self.dagbag.get_dag(self.dag)
            task = dag.get_task(self.task)
            task_list = [task]
        except Exception as err:
            self.session.rollback()
            return jsonify({"message": str(err)})
        else:
            if "upstream" in self.options:
                data = task.get_flat_relatives(upstream=False)
                task_list.extend(data)
            if "downstream" in self.options:
                data = task.get_flat_relatives(upstream=True)
                task_list.extend(data)
            task_ids = [task.task_id for task in task_list]
            tis = self.session.query(self.TI).filter(
                self.TI.dag_id == self.dag,
                self.TI.execution_date == self.execution_date)
            if self.action == "restart":
                tis_all = tis.filter(
                    self.TI.state != "running", self.TI.task_id.in_(task_ids)).all()
                clear_task_instances(tis_all, self.session, dag=task.dag)
                self.session.commit()
            elif self.action == "kill":
                tis_all = tis.filter(self.TI.task_id == self.task).all()
                this_state = tis_all[0].state
                if this_state in (None, "running"):
                    message = {
                        "message": "Can't kill/stop task. Current state is {}.".format(this_state)}
                    return jsonify(message)
                clear_task_instances(tis_all, self.session, activate_dag_runs=False)
                self.session.commit()
            elif self.action == "mark_success":
                tis_all = tis.filter(
                    self.TI.state != "running", self.TI.task_id.in_(task_ids)).all()
                for ti in tis_all:
                    ti.state = State.SUCCESS
                if not self.options:
                    self._set_dag_run_state(State.RUNNING)
                else:
                    self._set_dag_run_state(State.SUCCESS)
            modified_tasks = '\n'.join('{}'.format(item) for item in tis_all)
            message = {
                "message": "The following tasks had status set to: {}\n{}".format(
                    self.action.upper(), modified_tasks)}
            self.session.commit()
            return jsonify(message)
        finally:
            self.session.close()


class GetLog(Resource, Init):
    """
    Get latest log for specific task run.
    """
    def __init__(self):
        super().__init__()
        self.dag = self.args['dag_id']
        self.task = self.args['task_id']
        self.execution_date = self.args['execution_date']
        self.execution_date = datetime.strptime(self.execution_date, "%Y-%m-%dT%H:%M:%S")
        self.execution_date = timezone.convert_to_utc(self.execution_date)
        self.path = "{}/{}/{}/{}/".format(LOG_PATH, self.dag, self.task, self.execution_date)

    def get(self):
        """
        Helper method that retrieves latest log for task run.
        :param dag_id: dag_id of target dag run
        :param task_id: task_id of target task run
        :param execution_date: the execution date from which to start looking
        """
        lst = os.listdir(self.path)
        lst = ",".join(lst)
        logs = [e for e in re.split("[^0-9]", lst) if e != '']
        latest = str(max(map(int, logs))) + ".log"
        with open(self.path + latest) as f:
            contents = f.readlines()
        return jsonify(contents)


class WorkerView(Resource, Init):
    """
    WorkerView shows the celery hosts and which host belongs to which queue.
    Also, shows available and taken worker slots.
    """
    def __init__(self):
        super().__init__()
        self.dagbag = DagBag()
        self.queues = set()
        self.broker = os.environ['AIRFLOW__CELERY__BROKER_URL']
        self.backend = os.environ['AIRFLOW__CELERY__RESULT_BACKEND']
        self.celery = Celery('airflow_api', broker=self.broker, backend=self.backend)
        self.owners = self.args['owners'].lower()

    def get(self):
        """
        :param owners: DAG owners
        """
        dags = self.session.query(self.DM).filter(self.DM.owners == self.owners).all()
        for each in dags:
            dag = self.dagbag.get_dag(each.dag_id)
            try:
                queue = dag.default_args.get("queue", "airflow")
                self.queues.add(queue)
            except Exception:
                pass
        all_hosts = self.celery.control.inspect().stats().keys()
        data_list = []
        for host in all_hosts:
            celery = self.celery.control.inspect([host])
            queue = set([value[0].get("routing_key") for value in celery.active_queues().values()])
            if self.queues & queue:
                queue = next(iter(queue))
                running = [len(r) for r in celery.active().values()]
                available = [value["pool"]["writes"]["inqueues"]['total'] for value in celery.stats().values()]
                data_list.append(OrderedDict([
                    ('queue', queue),
                    ('host', host[7:]),
                    ('running', running[0]),
                    ('available', available[0] - running[0])]))
            else:
                continue
        return jsonify(data_list)


class LongRunningView(Resource, Init):
    def __init__(self):
        super().__init__()
        self.owners = self.args['owners'].lower()

    def get(self):
        data_list = []
        cond = and_(
            self.TI.dag_id == self.DM.dag_id,
            self.TI.state == "running",
            self.DM.owners == self.owners)
        tasks = self.session.query(
            self.TI.dag_id,
            self.TI.task_id,
            self.TI.execution_date,
            self.TI.start_date,
            self.TI.duration).filter(cond).all()
        for task in tasks:
            last_runs = self.session.query(self.TI.duration).filter(
                self.TI.task_id == task.task_id,
                self.TI.state == "success",
                self.TI.duration > 5).order_by(
                    self.TI.execution_date.desc()).limit(10).all()
            average = statistics.mean(
                duration[0] for duration in last_runs)
            runtime = (pendulum.utcnow() - task.start_date).seconds
            execution_date = self.datetime_iso(task.execution_date)
            if runtime > (average + 1800):
                data_list.append(OrderedDict([
                    ('dag_id', task.dag_id),
                    ('task_id', task.task_id),
                    ('execution_date', execution_date),
                    ('runtime', self.seconds_to(runtime)),
                    ('average', self.seconds_to(average))]))
            else:
                continue
        return jsonify(data_list)


if __name__ == '__main__':
    api = Api(app)
    api.add_resource(DagView, "{}/dagview".format(INIT_URL))
    api.add_resource(DagControl, "{}/dagcontrol".format(INIT_URL))
    api.add_resource(TaskView, "{}/taskview".format(INIT_URL))
    api.add_resource(GetLog, "{}/getlog".format(INIT_URL))
    api.add_resource(TaskControl, "{}/taskcontrol".format(INIT_URL))
    api.add_resource(WorkerView, "{}/workerview".format(INIT_URL))
    api.add_resource(LongRunningView, "{}/longrunning".format(INIT_URL))
    number_of_processes = 5
    listen = _tcp_listener((HOST_NAME, 80))

    def serve_forever(listener):
        WSGIServer(listener, app).serve_forever()

    for i in range(number_of_processes):
        Process(target=serve_forever, args=(listen,)).start()

    serve_forever(listen)
