from typing import Any
import composer2_airflow_rest_api
import base64
import json
from flask import jsonify,request
 
def trigger_dag_gcf(request, context=None):
    try:
        web_server_url = "https://2836bd65dd584c97b35bb1334106efca-dot-europe-west1.composer.googleusercontent.com"
        # Replace with the ID of the DAG that you want to run.
        dag_id = 'ie_cf_trigger'
        # Replace with the ID of the DAG that you want to run.
        print("request is : " + str(request))
        print("trigger dag")
        if request.is_json:
            print("inside if......")
            data = request.get_json()
            print("data")
            print(type(data))
            print(data)
            message_data=data['message']
            attributes = message_data['attributes']
            print('message attributes: ',attributes)
            fetched_object=attributes['objectId']
            print('fetched object path: ',fetched_object)
            #skips objects ending with / or success and trigger only one dag run.
            if (not fetched_object.endswith('/') and not fetched_object.endswith('_SUCCESS') and not '/error/' in fetched_object.lower() and fetched_object.lower().endswith('.csv')):
                #return jsonify({"message":"skipped triggering dag"}),200
                print("Valid CSV file found- triggering the DAG")
                #print("dag Response:")
                return_response = composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, data)
                print("return response: ",return_response)
                return jsonify({"message": "processing complete"}), 200
            else:
                skip_reason="Skipped Because: "
                if fetched_object.endswith('/'):
                    skip_reason+="it's a directory; "
                if fetched_object.endswith('_SUCCESS'):
                    skip_reason+="it's a success file; "
                if '/error/' in fetched_object.lower():
                    skip_reason+="it's in an error folder; "
                if not fetched_object.lower().endswith('.csv'):
                    skip_reason+="it's not a csv file;"
                print(skip_reason)
                return jsonify({'message':skip_reason})
        else:           
            return jsonify({"error":"Request is not JSON"}),400
    except Exception as e:
        return jsonify({"error":str(e)}),500
 
 
    #return jsonify({"message": "processing complete"}), 200