from google.cloud import bigquery
import requests
import os

def query_vta_7_dias():

  os.environ["HTTP_PROXY"] = "http://proxyfal.falabella.cl:8080"
  os.environ["HTTPS_PROXY"] = "http://proxyfal.falabella.cl:8080"

  print(requests.certs.where())

  bq_client = bigquery.Client.from_service_account_json('tot-bi-cl-logistica-dev-dbebbf94a4f0.json')
  # bq_client = bigquery.Client()
  query_job = bq_client.query("""
    SELECT *
    FROM tot-bi-cl-logistica-dev.trf_tot_corp_ecommerce.vta_futura_7_dias
    """)

  results = query_job.result()  # Waits for job to complete.

  for row in results:
    print(row)

if __name__ == "__main__":
  query_vta_7_dias()
