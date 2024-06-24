from google.cloud import bigquery
import requests
import datetime
import time
import pandas as pd
import pyarrow
from google.cloud import pubsub_v1

def obtenerAlumnos(event, context):
  client = bigquery.Client()

  lw_client = "62b182eea31d8d9863079f42"
  authorization = "Bearer Vbs2rQlAvSUvuMhFpEZf9uAbrDCcXtnausV6ytXq"
  accept = "application/json"

  df_users = pd.DataFrame(columns=['id', 'email', 'username', 'subscribed_for_marketing_emails', 'eu_customer', 'is_admin', 'is_instructor', 'is_suspended', 'is_reporter',
                                  'is_affiliate', 'referrer_id', 'created', 'last_login', 'signup_approval_status', 'email_verification_status', 'tags', 'billing_info', 'nps_score', 'nps_comment'])

  userLst = []

  url = "https://academy.turiscool.com/admin/api/v2/users"
  headers = {
      "Lw-Client": lw_client,
      "Authorization": authorization,
      "Accept": accept
  }

  x = 101

  y = x + 51

  for i in range(x, y):
    response = requests.get(url, headers=headers, params={"page": str(i), "items_per_page": "20"}).json()
    userLst.append(response)

  time.sleep(5)

  x = y
  y = x + 50
  
  for i in range(x, y):
    response = requests.get(url, headers=headers, params={"page": str(i), "items_per_page": "20"}).json()
    userLst.append(response)
  
  userTransf = []
  userTransf = [pd.DataFrame.from_dict(user['data']) for user in userLst]
  df_users = pd.concat(userTransf, ignore_index=True)

  df_users = df_users[['id','email','username','created','last_login','tags','nps_score','nps_comment']]
  df_users['tags'] = df_users['tags'].astype('str')
  df_users['nps_comment'] = df_users['nps_comment'].astype('str')
  df_users['nps_score'] = df_users['nps_score'].astype('float').fillna('')
  df_users = df_users.fillna('')
  df_users = df_users.drop_duplicates(keep='last')

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("id", "STRING"),
          bigquery.SchemaField("email", "STRING"),
          bigquery.SchemaField("username", "STRING"),
          bigquery.SchemaField("created", "STRING"),
          bigquery.SchemaField("last_login", "STRING"),
          bigquery.SchemaField("tags", "STRING"),
          bigquery.SchemaField("nps_score", "STRING"),
          bigquery.SchemaField("nps_comment", "STRING"),
          ],
      autodetect=False,
      source_format=bigquery.SourceFormat.CSV,

  )

  job = client.load_table_from_dataframe(df_users, 'my-project-83697-juan-carlos.datosCustomerSuccess.tablaAlumnos', job_config=job_config)
  job.result()

  publisher = pubsub_v1.PublisherClient()
  message_payload = "Lanzar ObtenerAlumnos2"
  future = publisher.publish("projects/my-project-83697-juan-carlos/topics/triggerObtenerAlumnos-PubSub3", data=message_payload.encode('utf-8'))
  future.result()

  return "Trabajo completado"  
