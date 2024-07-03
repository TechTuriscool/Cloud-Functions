from google.cloud import bigquery
import requests
import datetime
import time
import pandas as pd
import pyarrow
from google.cloud import pubsub_v1

def obtenerProgreso(event, context):

  client = bigquery.Client()
  
  user_progress = pd.DataFrame()
  userid = {}
  progressLst = []

  sql = """
    SELECT * FROM `my-project-83697-juan-carlos.datosCustomerSuccess.tablaAlumnos`
    ORDER BY `id`
    LIMIT 150 OFFSET 299
  """

  datasetAlumnos = client.query_and_wait(sql).to_dataframe()
  
  user_Id = datasetAlumnos[['id']]

  x = 0
  y = x + 10

  for n in range(0, 21):

      for i in range(x,y):

          try:
              user_id = user_Id['id'][i]
              url = "https://academy.turiscool.com/admin/api/v2/users/" + str(user_id) + "/progress"
              querystring = {"page": "1", "items_per_page": "150"}
              headers = {
                  "Lw-Client": "62b182eea31d8d9863079f42",
                  "Authorization": "Bearer ZwRg90VrUWLNBhEpfQnCuyJSPWWI7bjtpmO3QbH2",
                  "Accept": "application/json"
              }
              response = requests.get(url, headers=headers, params=querystring)
              json_data = response.json()
              df_loop_3 = pd.DataFrame(json_data['data'])
              df_loop_3['id'] = user_id
              user_progress = pd.concat([user_progress, df_loop_3])
          except:
              pass

      x = y
      y = x + 10

  df_def = user_progress.merge(datasetAlumnos, on='id', how='left')
  df_def = df_def.reset_index()

  print(y)

  df_def['Fecha'] = datetime.date.today().strftime('%Y-%m-%d')

  df_def = df_def.fillna('')
  df_def = df_def[['course_id','status','progress_rate','average_score_rate','time_on_course','total_units','completed_units','id','email','username','created','last_login','tags','nps_score','nps_comment','Fecha']]

  df_def = df_def.drop_duplicates(keep='last')

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("course_id", "STRING"),
          bigquery.SchemaField("status", "STRING"),
          bigquery.SchemaField("progress_rate", "STRING"),
          bigquery.SchemaField("average_score_rate", "STRING"),
          bigquery.SchemaField("time_on_course", "STRING"),
          bigquery.SchemaField("total_units", "STRING"),
          bigquery.SchemaField("completed_units", "STRING"),
          bigquery.SchemaField("id", "STRING"),
          bigquery.SchemaField("email", "STRING"),
          bigquery.SchemaField("username", "STRING"),
          bigquery.SchemaField("created", "STRING"),
          bigquery.SchemaField("last_login", "STRING"),
          bigquery.SchemaField("tags", "STRING"),
          bigquery.SchemaField("nps_score", "STRING"),
          bigquery.SchemaField("nps_comment", "STRING"),
          bigquery.SchemaField("Fecha", "STRING"),            
          ],
      autodetect=False,
      source_format=bigquery.SourceFormat.CSV,

  )
  job = client.load_table_from_dataframe(df_def, 'my-project-83697-juan-carlos.datosCustomerSuccess.tablaProgreso', job_config=job_config)
  job.result()

  publisher = pubsub_v1.PublisherClient()
  message_payload = "Lanzar ObtenerProgreso4"
  future = publisher.publish("projects/my-project-83697-juan-carlos/topics/triggerObtenerProgreso3", data=message_payload.encode('utf-8'))
  future.result()

  return "Trabajo completado"   