# Script used to store fucntions connected to BigQuery Functionality

import google.cloud.bigquery as bq

def bq_query(query, project_name = "sportsbook-insights"):
    """Function used to fetch data using BigQuery"""
    query_df = bq.Client(project=project_name).query(query).to_dataframe()
    return query_df

def bq_persist(
    input_df, 
    table_name, 
    project_name = 'sportsbook-insights', 
    chunksize = 10000, 
    if_exists = 'fail'
):
    """Function used to persist data using BigQuery"""
    input_df.to_gbq(
            destination_table = table_name, 
            project_id = project_name,
            chunksize= chunksize,
            if_exists = if_exists 
    )
    print('Dataframe persisted')