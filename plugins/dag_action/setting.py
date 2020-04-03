import os
import socket

from airflow import configuration

DAG_CREATION_MANAGER_DAG_TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "dag_templates")