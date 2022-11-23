from .env import AWS_ACCESS_KEY, AWS_SECRET_KEY
import boto3

S3_BUCKET_NAME = 'busd-rca-projects'

class Aws:

    def upload_s3(self, local_fullfile: str, s3_fullfile: str) -> bool:

        """
            Upload a local file to the s3 bucket
            @param local_fullfile
            @param s3_fullfile
        """

        s3 = boto3.client('s3', aws_access_key_id = AWS_ACCESS_KEY, aws_secret_access_key = AWS_SECRET_KEY)
        print('Connected to S3')

        try:

            s3.upload_file(local_fullfile, S3_BUCKET_NAME, s3_fullfile)
            print('INFO: Uploaded To S3 Successfully')
            return True

        except FileNotFoundError:

            print('ERROR: The file was not found')
            return False

        except BaseException as e:

            print(str(e))
            return False