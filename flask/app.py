from flask import Flask
from flask_http_client import HTTPClient

app = Flask(__name__)
http_client = HTTPClient(base_url="http://localhost:3000",username="", password="",verify=False)
http_client.init_app(app)


@app.route("/index/<dag_name>",methods=['GET'])
def index(dag_name):
    print(dag_name)
    params = {"dag_name":dag_name}
    resp = http_client.get('/api', params=params)
    print(resp)
    return "resp"
if __name__ == '__main__':
    app.run(debug=True)