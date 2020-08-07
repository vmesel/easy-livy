import requests
import json
import pprint
import textwrap

class LivyConnector:
    def __init__(self, livy_url):
        self.livy_url = livy_url
    
    def create_session(self, session_name="mr-livy"):
        directive = '/sessions'
        headers = {'Content-Type': 'application/json'}

        data = {
            'kind': 'pyspark',
            'name': '{}'.format(session_name)
        }

        resp = requests.post(self.livy_url+directive, headers=headers, data=json.dumps(data))

        if resp.status_code == requests.codes.created:
            self.session_id = resp.json()['id']
        else:
            ValueError("Session with same name already created")

    def run_statement(self, statement):
        directive = f'/sessions/{self.session_id}/statements'
        resp = requests.post(
            self.livy_url + directive,
            headers=headers,
            data=json.dumps(statement)
        )
        return resp.json()
        
    def retrieve_statement_result(self, statement_id):
        return requests.get(self.livy_url + f'/sessions/{self.session_id}/statements/{statement_id}').json()
