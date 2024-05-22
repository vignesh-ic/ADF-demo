import unittest
import requests
from azure.identity import DefaultAzureCredential
import json

def get_access_token():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default")
    return token.token

def load_config():
    with open('config.json', 'r') as file:
        config = json.load(file)
    return config

class TestAzureDataFactory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = load_config()
        cls.subscription_id = config['subscription_id']
        cls.resource_group_name = config['resource_group_name']
        cls.data_factory_name = config['data_factory_name']

        cls.access_token = get_access_token()
        cls.base_url = f"https://management.azure.com/subscriptions/{cls.subscription_id}/resourceGroups/{cls.resource_group_name}/providers/Microsoft.DataFactory/factories/{cls.data_factory_name}"

        cls.linked_services = config['linked_services']
        cls.integration_runtimes = config['integration_runtimes']
        cls.pipeline_name = config['pipeline_name']
        cls.dataset_name = config['dataset_name']

    def get_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def test_pipeline_exists(self):
        missing_pipelines = []
        for pipeline in self.pipeline_name:
            url = f"{self.base_url}/pipelines/{pipeline}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Pipeline '{pipeline}' exists. success code 200")
            else:
                missing_pipelines.append(pipeline)
                print(f"Pipeline '{pipeline}' does not exist. code 400")
        self.assertEqual(len(missing_pipelines), 0, f"Some pipelines do not exist: {', '.join(missing_pipelines)}")

    def test_linked_services_exist(self):
        missing_services = []
        for linked_service_name in self.linked_services:
            url = f"{self.base_url}/linkedservices/{linked_service_name}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Linked Service '{linked_service_name}' exists. success code 200")
            else:
                missing_services.append(linked_service_name)
                print(f"Linked Service '{linked_service_name}' does not exist. code 400")
        self.assertEqual(len(missing_services), 0, f"Some linked services do not exist: {', '.join(missing_services)}")

    def test_integration_runtimes_exist(self):
        missing_integration_runtimes = []
        for ir_name in self.integration_runtimes:
            url = f"{self.base_url}/integrationruntimes/{ir_name}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Integration Runtime '{ir_name}' exists. Success code 200.")
            else:
                missing_integration_runtimes.append(ir_name)
                print(f"Integration Runtime '{ir_name}' does not exist.")
        self.assertEqual(len(missing_integration_runtimes), 0, f"Some integration runtimes do not exist: {', '.join(missing_integration_runtimes)}")

    def test_dataset_exists(self):
        missing_datasets = []
        for dataset in self.dataset_name:
            url = f"{self.base_url}/datasets/{dataset}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Dataset '{dataset}' exists. success code 200")
            else:
                missing_datasets.append(dataset)
                print(f"Dataset '{dataset}' does not exist.")
        self.assertEqual(len(missing_datasets), 0, f"Some datasets do not exist: {', '.join(missing_datasets)}")

    def test_pipeline_run_status(self):
        pipeline_name = 'pl_Master'
        trigger_url = f"{self.base_url}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"
        
        # Trigger the pipeline run
        trigger_response = requests.post(trigger_url, headers=self.get_headers())
        self.assertEqual(trigger_response.status_code, 200, "Failed to trigger the pipeline run.")
        
        run_id = trigger_response.json().get('runId')
        self.assertIsNotNone(run_id, "No run ID returned.")
        print(f"Triggered pipeline '{pipeline_name}' with run ID: {run_id}")

        # Check the pipeline run status
        status_url = f"{self.base_url}/pipelineruns/{run_id}?api-version=2018-06-01"
        status_response = requests.get(status_url, headers=self.get_headers())
        self.assertEqual(status_response.status_code, 200, "Failed to get pipeline run status.")
        
        run_status = status_response.json().get('status')
        print(f"Pipeline run status: {run_status}")
        self.assertIsNotNone(run_status, "Pipeline run status is not available.")

        import unittest
import requests
from azure.identity import DefaultAzureCredential
import json

def get_access_token():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default")
    return token.token

class TestAzureDataFactory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.subscription_id = 'c15d1375-675d-49d7-bbd2-4aaf95f666ab'
        cls.resource_group_name = 'dev-icint-hrfra-001-rg'
        cls.data_factory_name = 'dev-icint-hrfra-001-adf'

        cls.access_token = get_access_token()
        cls.base_url = f"https://management.azure.com/subscriptions/{cls.subscription_id}/resourceGroups/{cls.resource_group_name}/providers/Microsoft.DataFactory/factories/{cls.data_factory_name}"

        cls.linked_services = [
           'ls_hrfra_AzureBlobStorage',
           'ls_hrfra_AzureSqlDatabase',
           'ls_hrfra_Oracle',
           'ls_hrfra_adf_key_vault'
        ]

        cls.integration_runtimes = [
            'SHIR-Sandbox-banner-int',
            'dev-icint-hrfra-001-shir',
            
        ]
        
        cls.pipeline_name = [
            'pl_child',
            'pl_Logging',
            'pl_Master'
        ]
        
        cls.dataset_name = [
            'ds_AzureSQLConfig',
            'ds_OracleParametrizedTable',
            'ds_SinkStorage'
        ]

    def get_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

    def test_pipeline_exists(self):
        missing_pipelines = []
        for pipeline in self.pipeline_name:
            url = f"{self.base_url}/pipelines/{pipeline}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Pipeline '{pipeline}' exists. success code 200")
            else:
                missing_pipelines.append(pipeline)
                print(f"Pipeline '{pipeline}' does not exist. code 400")
        self.assertEqual(len(missing_pipelines), 0, f"Some pipelines do not exist: {', '.join(missing_pipelines)}")

    def test_linked_services_exist(self):
        missing_services = []
        for linked_service_name in self.linked_services:
            url = f"{self.base_url}/linkedservices/{linked_service_name}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Linked Service '{linked_service_name}' exists. success code 200")
            else:
                missing_services.append(linked_service_name)
                print(f"Linked Service '{linked_service_name}' does not exist. code 400")
        self.assertEqual(len(missing_services), 0, f"Some linked services do not exist: {', '.join(missing_services)}")

    def test_integration_runtimes_exist(self):
        missing_integration_runtimes = []
        for ir_name in self.integration_runtimes:
            url = f"{self.base_url}/integrationruntimes/{ir_name}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Integration Runtime '{ir_name}' exists. Success code 200.")
            else:
                missing_integration_runtimes.append(ir_name)
                print(f"Integration Runtime '{ir_name}' does not exist.")
        self.assertEqual(len(missing_integration_runtimes), 0, f"Some integration runtimes do not exist: {', '.join(missing_integration_runtimes)}")

    def test_dataset_exists(self):
        missing_datasets = []
        for dataset in self.dataset_name:
            url = f"{self.base_url}/datasets/{dataset}?api-version=2018-06-01"
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Dataset '{dataset}' exists. success code 200")
            else:
                missing_datasets.append(dataset)
                print(f"Dataset '{dataset}' does not exist.")
        self.assertEqual(len(missing_datasets), 0, f"Some datasets do not exist: {', '.join(missing_datasets)}")

    def test_pipeline_run_status(self):
        pipeline_name = 'pl_Master'
        trigger_url = f"{self.base_url}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"
        
        # Trigger the pipeline run
        trigger_response = requests.post(trigger_url, headers=self.get_headers())
        self.assertEqual(trigger_response.status_code, 200, "Failed to trigger the pipeline run.")
        
        run_id = trigger_response.json().get('runId')
        self.assertIsNotNone(run_id, "No run ID returned.")
        print(f"Triggered pipeline '{pipeline_name}' with run ID: {run_id}")

        # Check the pipeline run status
        status_url = f"{self.base_url}/pipelineruns/{run_id}?api-version=2018-06-01"
        status_response = requests.get(status_url, headers=self.get_headers())
        self.assertEqual(status_response.status_code, 200, "Failed to get pipeline run status.")
        
        run_status = status_response.json().get('status')
        print(f"Pipeline run status: {run_status}")
        self.assertIsNotNone(run_status, "Pipeline run status is not available.")

        

if __name__ == '__main__':
    unittest.main()


if __name__ == '__main__':
    unittest.main()
