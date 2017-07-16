from flask import Flask, jsonify, request, json
from flask_restful import Resource, Api
import werkzeug
import uuid, os
import json
from tempfile import NamedTemporaryFile
from os import remove
from azure.storage.blob import BlockBlobService
from azure.storage.table import TableService, Entity

accountname = 'pythonstorageaccount'
accountkey = 'gCRNgo2AXvDpOQW83l64WqDaTskZwvZdzjUq/Cnal9bMpAn4RFGj/bqjZUXI+NEkctg9eT3zm34y6ZAYEVTFFA=='
tablename = 'flaskrestdemo'

block_blob_service = BlockBlobService(account_name=accountname, account_key=accountkey)
block_blob_service.create_container('pythonblobstorecontainer')

table_service = TableService(account_name=accountname, account_key=accountkey)
table_service.create_table(tablename)

app = Flask(__name__)
app.config['DEBUG'] = True
api = Api(app)

class FlaskRestApi(Resource):

    container = 'pythonblobstorecontainer'
    partition_name = str(uuid.uuid4())

    def get(self):
        
        bloblist = []
        generator = block_blob_service.list_blobs(self.container)
        for blob in generator:
            data = block_blob_service.get_blob_to_text(self.container, blob.name)
            user = json.loads(str(data.content))
            bloblist.append({ 
                'user': user, 
                'blobname': data.name 
            })
        blobres = bloblist

        tasklist = []    
        tasks = table_service.query_entities(tablename, filter=None, select=None)
        for task in tasks: 
            tasklist.append({
            'PartitionName': task['PartitionKey'],    
            'UserName': task['RowKey'],    
            'FirstName': task['FirstName'],
            'LastName': task['LastName'],
            'EmailAddress': task['EmailAddress'],
            'CellNo': task['CellNo']
        })
        tableres = tasklist 
        
        return jsonify({
            'BlobResponse': blobres,
            'TableResponse': tableres
        })


    def post(self):

        res = {}

        try:
            # get the posted user data
            userdata = request.data
        
            # save to blob
            blob_file_name = self.partition_name + '.json'        
            block_blob_service.create_blob_from_text(
                self.container,
                blob_file_name,
                userdata
            )

            #save to table
            task = Entity()
            task.PartitionKey = self.partition_name
            userModel = json.loads(userdata)
            task.RowKey = userModel['userName']
            task.FirstName = userModel['firstName']
            task.LastName = userModel['lastName']
            task.EmailAddress = userModel['emailAddress']
            task.CellNo = userModel['cellNo']
            table_service.insert_entity(tablename, task)

            res = {
                'message' : 'save successful'
            }

        except expression as identifier:
            
            res = {
                'message' : 'save failed'
            }

        return jsonify(res)

    def put(self):

        userModel = json.loads(request.data)
        userName = str(userModel['userName'])
        
        #save to table
        task = Entity()
        task.PartitionKey = 'f6dcfcfe-169e-4731-a589-2f8c6c75768a'#self.partition_name        
        task.RowKey = userModel['userName']
        task.FirstName = userModel['firstName']
        task.LastName = userModel['lastName']
        task.EmailAddress = userModel['emailAddress']
        task.CellNo = userModel['cellNo']
        table_service.update_entity(tablename, task, if_match='*')       


api.add_resource(FlaskRestApi, '/flaskrestapi', endpoint = 'flaskrest')


if __name__ == '__main__':
    app.run(debug=True,  port=8080)