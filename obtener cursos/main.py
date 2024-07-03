from google.cloud import bigquery
import requests
import datetime
import time
import pandas as pd
import pyarrow
from google.cloud import pubsub_v1



def limpiarBigQuery(event, context):
  client = bigquery.Client()

  QUERY = ('DELETE FROM my-project-83697-juan-carlos.datosCustomerSuccess.tablaCursos WHERE id IS NOT NULL')
  query_job = client.query(QUERY)
  rows = query_job.result()

  lw_client = "62b182eea31d8d9863079f42"
  # authorization = "Bearer ZwRg90VrUWLNBhEpfQnCuyJSPWWI7bjtpmO3QbH2"
  authorization = "Bearer F65NS88uyXckn88Vv6P5Egv5hfQALxMoIJVCbykr"
  accept = "application/json"

  # OBTENER CURSOS.

  df_courses = pd.DataFrame()

  for i in range(10):
      url = "https://academy.turiscool.com/admin/api/v2/courses"
      querystring = {"page": str(i)}
      headers = {
          "Lw-Client": lw_client,
          "Authorization": authorization,
          "Accept": accept
      }
      response = requests.get(url, headers= headers, params= querystring)
      json = response.json()
      df_loop = pd.DataFrame(json['data'])
      df_courses = pd.concat([df_courses,df_loop])

  df_courses = df_courses.reset_index()

  df_courses = df_courses[['id','title','categories','label','created','modified']]

  df_courses['categories'] = df_courses['categories'].astype('str')

  df_courses['created'] = datetime.date.today().strftime('%Y-%m-%d')

  df_courses['modified'] = datetime.date.today().strftime('%Y-%m-%d')

  df_courses.fillna('')

  df_courses = df_courses.drop_duplicates(keep='last')

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("id", "STRING"),
          bigquery.SchemaField("title", "STRING"),
          bigquery.SchemaField("categories", "STRING"),
          bigquery.SchemaField("label", "STRING"),
          bigquery.SchemaField("created", "STRING"),
          bigquery.SchemaField("modified", "STRING"),
          ],
      autodetect=False,
      source_format=bigquery.SourceFormat.CSV,

  )

  job = client.load_table_from_dataframe(df_courses, 'my-project-83697-juan-carlos.datosCustomerSuccess.tablaCursos', job_config=job_config)
  job.result()

  publisher = pubsub_v1.PublisherClient()
  message_payload = "Lanzar ObtenerAlumnos"
  future = publisher.publish("projects/my-project-83697-juan-carlos/topics/triggerObtenerAlumnos-PubSub", data=message_payload.encode('utf-8'))
  future.result()



  return "Trabajo completado"