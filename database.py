from aws import get_secret
import snowflake.connector

import boto3
import json
import pandas as pd


class SnowflakeConnection:
    def __init__(self,
                 user: str,
                 password: str,
                 account: str,
                 warehouse: str,
                 database: str,
                 schema: str,
                 role: str
                ) -> None :
        
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
    
    def connect(self):
        try:
            con = snowflake.connector.connect(
                user = self.user, 
                password = self.password ,
                account = self.account ,
                warehouse = self.warehouse,
                database =self.database,
                schema = self.schema ,
                role= self.role 
            )
            return con
        except Exception as e:
            raise e
            
    def load_snowflake_json(self, json_data):
        
        ctx = self.connect()

        cs = ctx.cursor()
        try:
            cs.execute(f"use warehouse surfline_wh")
            cs.execute("insert into surf_logs.surf_logs (select PARSE_JSON('%s'))" % json.dumps(json_data))
        finally:
            cs.close()
        ctx.close()
        
    def query_snowflake_json(self):

        ctx = self.connect()


        cs = ctx.cursor()
        try:
            cs.execute(f"use warehouse surfline_wh")
            results = cs.execute("select * from surf_logs.surf_logs")
            rows = cs.fetchall()
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cs.description])
            return df
        finally:
            cs.close()
            ctx.close()
