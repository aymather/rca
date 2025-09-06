import boto3

from .env import AWS_ACCESS_KEY, AWS_SECRET_KEY

S3_BUCKET_NAME = 'busd-rca-projects'

class Aws:

    def __init__(self):
        self.s3 = None
        self.retry_count = 3

    def connect_s3(self):

        count = 0
        while count < self.retry_count:

            try:

                self.s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY, aws_secret_access_key = AWS_SECRET_KEY)
                print('Connected to S3')
                return

            except Exception as e:
                count += 1
                print(str(e))

        raise Exception (f'Error connecting to s3 bucket with access key: {AWS_ACCESS_KEY}')

    def upload_s3(self, local_fullfile, s3_fullfile):

        """
            Upload a local file to the s3 bucket
            @param local_fullfile
            @param s3_fullfile
        """

        if self.s3 is None:
            raise Exception('Aws s3 client not connected, you must first call self.connect_s3()')

        try:

            self.s3.upload_file(local_fullfile, S3_BUCKET_NAME, s3_fullfile)
            print('INFO: Uploaded To S3 Successfully')
            return True

        except FileNotFoundError:

            print('ERROR: The file was not found')
            return False

        except BaseException as e:

            print(str(e))
            return False

    def delete_file_s3(self, s3_fullfile):

        if self.s3 is None:
            raise Exception('Aws s3 client not connected, you must first call self.connect_s3()')

        try:

            self.s3.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_fullfile)
            print(f'INFO: Deleted {s3_fullfile} successfully')

        except BaseException as e:
            print(str(e))

    def download_s3(self, s3_fullfile, local_fullfile):
        """
        Download a file from the s3 bucket to local storage
        @param s3_fullfile: S3 key/path of the file to download
        @param local_fullfile: Local path where the file will be saved
        """
        
        if self.s3 is None:
            raise Exception('Aws s3 client not connected, you must first call self.connect_s3()')
        
        try:
            self.s3.download_file(S3_BUCKET_NAME, s3_fullfile, local_fullfile)
            print(f'INFO: Downloaded {s3_fullfile} to {local_fullfile} successfully')
            return True
            
        except FileNotFoundError:
            print(f'ERROR: The file {s3_fullfile} was not found in S3')
            return False
            
        except BaseException as e:
            print(f'ERROR downloading file: {str(e)}')
            return False

    def list_s3_files(self, prefix=''):
        """
        List files in the S3 bucket with optional prefix filter
        @param prefix: Optional prefix to filter files
        @return: List of file keys
        """
        
        if self.s3 is None:
            raise Exception('Aws s3 client not connected, you must first call self.connect_s3()')
        
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
                
        except BaseException as e:
            print(f'ERROR listing files: {str(e)}')
            return []