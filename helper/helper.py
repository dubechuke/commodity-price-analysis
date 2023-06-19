import boto3
import io
import pandas as pd
from dotenv import load_dotenv
import os
import logging
from botocore.exceptions import ClientError


environ = load_dotenv()

class S3_Connect:
    """
    The class to read/write to/from s3 bucket
    """
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")


    def read_from_s3(self, filename, key, ext='csv'):
        """

        :param filename:
        :param key:
        :param ext:
        :return:
        """

        filename = f'{key}/{filename}.{ext}'
        obj = self.s3_client.get_object(Bucket=self.AWS_S3_BUCKET, Key=filename)
        # print(obj)
        data = obj.get('Body').read()
        df = pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False)

        return df

    def write_to_s3(self, df, final_df_name, key, ext='csv'):
        """
        :param df:
        :param final_df_name:
        :param key:
        :param ext:
        :return:
        """
        Key = f"{key}/{final_df_name}.{ext}"
        # print(Key)
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            response = self.s3_client.put_object(
                Bucket=self.AWS_S3_BUCKET, Key=Key, Body=csv_buffer.getvalue()
            )

            # print(response)
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")

    def upload_file(self, file_name, key, object_name=None, ext="csv"):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)
        print(object_name)
        object_name = f'{key}/{object_name}.{ext}'
        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, self.AWS_S3_BUCKET, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

class Snowflake:
    pass

def read_file(path):
    ext = path.split('.')[-1]
    assert ext in ['csv', 'xlsx', 'parquet', 'json'], 'Please. choose a file from csv, xlsx, parquet, json'

    if ext == 'csv':
        return pd.read_csv(path)
    elif ext == 'xlsx':
        return pd.read_excel(path)
    elif ext == 'parquet':
        return pd.read_parquet(path)
    elif ext == 'json'
        return pd.read_json(path)


    print(ext)

if __name__ == "__main__":
    #s3conn = S3_Connect()
    # sf = S3_Connect().read_from_s3('transformed3', 'exceltest')
    # sf.to_csv("Dubetest.csv")
    #s3conn.upload_file("Dubetest.csv", "exceltest", "chuck")
    read_file("/Users/dube/PycharmProjects/WorldFoodPricesDataProject/helper/Dubetest.csv")

