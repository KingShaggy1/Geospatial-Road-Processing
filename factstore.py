from elasticsearch import Elasticsearch, RequestsHttpConnection

class FactStore:

    def __init__(self, esAnalytics, esAnalyticsPort):
        self.esAnalytics = esAnalytics
        self.esAnalyticsPort = esAnalyticsPort
        self.client = None

    def start(self):
        print("Connecting to ES Datasource")

        self.client = Elasticsearch(self.esAnalytics, port=self.esAnalyticsPort, connection_class=RequestsHttpConnection, use_ssl=True, timeout=300, max_retries=10, retry_on_timeout=True)

        if self.client.ping():
            recOffset= 0
            print('Connection established for ES')
        else:
            print('Could not establish connection for ES')

    def getES(self):

        return self.client

    def search(self, index, field, linear_id):

        query = {"query":{"term":{field:{"value":linear_id,"boost":1.0}}}}

        res = self.client.search(index=index, body=query)

        results = []

        for hit in res['hits']['hits']:
            results.append(hit["_source"])

        return results




        
