import os
import time
import datetime
import requests
import json
import sys
from collections import OrderedDict

#fill in required params below
#credentials available on azure portal
client_id = ''
client_secret = ''
tenant_id = ''
workspace_id = ''


#query to be passed
#all queries are limited to 30000
query = 'SecurityEvent | where TimeGenerated between(datetime("2019-10-29T00:00:00Z") .. now()) | where Computer == "GB-MAN-DC2"'



def get_access_token(client_id, client_secret_key, tenant_id):
    #format should be https://login.microsoftonline.com/{tenantId}/oauth2/token
    tokenUrl = 'https://login.microsoftonline.com/' + str(tenant_id) + '/oauth2/token' 

    tokenPayload = {
        'grant_type' : 'client_credentials',
        'client_id' : client_id,
        'client_secret' : client_secret_key,
        'resource' : 'https://api.loganalytics.io'
    }

    tokenResponse = requests.post(tokenUrl, data=tokenPayload)
    tokenresponse_json = tokenResponse.json()

    if tokenResponse.status_code == 200: 
        access_token = tokenresponse_json.get('access_token')
        return access_token
    else:
        print("Token Error "+ str(tokenResponse.status_code))

def get_headers(access_token):
    """
    Header parameters are declared.
    Valid Access token should be keyed in here

    """        
    headers = {
        'Authorization' : 'Bearer ' + access_token,
        'Content-Type': 'application/json',
        'Prefer' : 'wait = 30'
        }
    
    return headers

def get_query_url(workspace_id):
    queryUrl = "https://api.loganalytics.io/v1/workspaces/" + workspace_id + "/query"
    return queryUrl

def check_response(query_response):
    if query_response.status_code != 200:  
        query_response_json = query_response.json()

        if 'error_description' in query_response_json:           
            print("Error " + str(query_response.status_code) + " " + str(query_response_json['error']) + "\nError Description: " + str(query_response_json['error_description']))               

        if 'code' in query_response_json['error']:            
            print("Error " + str(query_response.status_code) + " " + str(query_response_json['error']['code']) + "\nError Description: " + str(query_response_json['error']['message']))
            
        sys.exit()

def parseSuccessResponse(query_response): 
    """
    Success response is parsed into (key, value) as (column name[], row name[])
    And the final list is written into a file
    
    """
    query_response_json = query_response.json()
    cur_time = datetime.datetime.now()

    fileName = str(cur_time) + '_LA_logs.json'
    outputfile = open(fileName, 'w')

    outputfile.write(json.dumps(query_response_json))
                
    print(fileName + " has been saved")                      
    outputfile.close()

def constructQuery(query):
    body = {
        "query" : query + ' | limit 30000'

    }
    
    return body

def downloadLogs(access_token, query, workspace_id):     
    try:
        query = constructQuery(query)
        query_response = requests.post(url=get_query_url(workspace_id), headers=get_headers(access_token), json=query)
        
        check_response(query_response)
        
        if query_response.status_code == 200:                   
            parseSuccessResponse(query_response)

    except(requests.exceptions.RequestException, requests.exceptions.HTTPError):                
        print("Request Exception while connecting to Workspace!")                
        exit()
        
    print("Completed the operation")

access_token = get_access_token(client_id, client_secret, tenant_id)
downloadLogs(access_token, query, workspace_id)