import os
import logging
#from StringIO import StringIO
import io

from datetime import datetime
from tempfile import NamedTemporaryFile
from dag_action.setting import DAG_CREATION_MANAGER_DAG_TEMPLATES_DIR
from airflow import settings
from airflow.utils.file import TemporaryDirectory
from airflow.models import DagBag, TaskInstance

def create_dagbag_by_dag_code(dag_name, dag_code)->None:
    with open(os.path.join(settings.DAGS_FOLDER, dag_name+".py"),"wb") as f:
            f.write(dag_code.encode('UTF-8'))
            f.flush()
            DagBag().collect_dags(dag_folder=os.path.join(settings.DAGS_FOLDER, dag_name+".py"))


def load_dag_template(template_name):
    logging.info("loading dag template: %s" % template_name)
    with open(os.path.join(DAG_CREATION_MANAGER_DAG_TEMPLATES_DIR, template_name + ".template"), "r") as f:
        res = f.read()
    return res