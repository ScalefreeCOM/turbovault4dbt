from google.cloud import bigquery
import datetime
import uuid



class GoogleBigQuery(object):

    def __init__(self, dataset, ):

        self.client = bigquery.Client()
        self.dataset = self.client.dataset(dataset)

        print("Google Cloud Platform on dataset {} initialized.".format(dataset))

    def query(self, query):
        try:
            query_job = self.client.query(query)

            results = query_job.result()

            return results
            
        except Exception as e:
            print("Error while processing following query: {0}; Exception {1}".format(query, e))

        