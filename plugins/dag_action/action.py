from dag_action.utils import create_dagbag_by_dag_code,load_dag_template

def addDagService(dag_name):
    dagCodes = load_dag_template("dag_code")%(dag_name)
    create_dagbag_by_dag_code(dag_name, dagCodes)