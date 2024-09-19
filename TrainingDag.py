import os
import yaml
from datetime import datetime

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator

from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperator
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerModelOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


os.environ['AWS_ACCESS_KEY_ID'] = 'xyz'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'xyz'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1' 
training_script ="s3://churn-airflow/python-scripts/churn_training.py"
yml_loc = r"/opt/airflow/dags/scripts"

# Get the current date and time
current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


with open(f'{yml_loc}/config.yml', 'r') as config_file:
    config = yaml.safe_load(config_file)


# Defining default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
    'email_on_retry': False,}

# Defining DAG
dag = DAG('TrainingDag', default_args=default_args, schedule_interval=None)



start_task = DummyOperator(
    task_id = 'start_task',
    dag = dag
)

end_task = DummyOperator(
    task_id = 'end_task',
    dag = dag
)


preprocessing_to_s3 = LocalFilesystemToS3Operator(
    task_id='preprocessing_script_to_s3',
    filename=f"{config['script_to_s3']['preprocessing_file_path']}",
    dest_key=f"{config['script_to_s3']['preprocessing_s3_key']}",
    dest_bucket=f"{config['script_to_s3']['s3_bucket_name']}",
    aws_conn_id= "aws_default",
    dag=dag,
    replace=True,
)

training_to_s3 = LocalFilesystemToS3Operator(
    task_id='training_script_to_s3',
    filename=f"{config['script_to_s3']['training_file_path']}",
    dest_key=f"{config['script_to_s3']['training_s3_key']}",
    dest_bucket=f"{config['script_to_s3']['s3_bucket_name']}",
    aws_conn_id= "aws_default",
    dag=dag,
    replace=True,
)

prediction_to_s3 = LocalFilesystemToS3Operator(
    task_id='prediction_script_to_s3',
    filename=f"{config['script_to_s3']['prediction_file_path']}",
    dest_key=f"{config['script_to_s3']['prediction_s3_key']}",
    dest_bucket=f"{config['script_to_s3']['s3_bucket_name']}",
    aws_conn_id= "aws_default",
    dag=dag,
    replace=True,
)

config_to_s3 = LocalFilesystemToS3Operator(
    task_id='config_file_to_s3',
    filename=f"{config['script_to_s3']['config_file_path']}",
    dest_key=f"{config['script_to_s3']['config_s3_key']}",
    dest_bucket=f"{config['script_to_s3']['s3_bucket_name']}",
    aws_conn_id= "aws_default",
    dag=dag,
    replace=True,
)


pre_processing_job = SageMakerProcessingOperator(
        task_id="pre_processing_job",
        config={
            "ProcessingInputs": [
            {
            "InputName": "custom-script",
            "AppManaged": False,
            "S3Input": {
                "S3Uri": "s3://churn-airflow/python-scripts/churn_preprocessing.py",
                "LocalPath": "/opt/ml/processing/input/script",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3DataDistributionType": "FullyReplicated",
                "S3CompressionType": "None",
            },
                },

            {
            "InputName": "config-file",
            "AppManaged": False,
            "S3Input": {
                "S3Uri": "s3://churn-airflow/python-scripts/config.yml",
                "LocalPath": "/opt/ml/processing/input/config",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3DataDistributionType": "FullyReplicated",
                "S3CompressionType": "None",
            },
                },
        ],
            "ProcessingOutputConfig": {
                "Outputs": [
                    {
                        "OutputName": f"training-data",
                        "S3Output": {
                            "S3Uri": "s3://churn-airflow/training-data",
                            "LocalPath": "/opt/ml/processing/input/data",
                            "S3UploadMode": "EndOfJob",
                        },
                        "AppManaged": False,
                    },
                   
                ],
            },
            "ProcessingJobName": f"preprocessing-job-{current_datetime}",
            "ProcessingResources": {
                "ClusterConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.t3.medium",
                    "VolumeSizeInGB": 1,
                },
            },
            "StoppingCondition": {"MaxRuntimeInSeconds": "300"},
            "AppSpecification": {
                "ImageUri": "654654507899.dkr.ecr.us-east-1.amazonaws.com/churn-airflow:latest",
                            
                "ContainerEntrypoint": [
                    "python3",
                    "/opt/ml/processing/input/script/churn_preprocessing.py",
                ],
            },
            "RoleArn": "xyz",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
        check_interval=30,
        deferrable=False,  
        dag = dag
)


modelling_job = SageMakerTrainingOperator(
        task_id='training_job',
        
        aws_conn_id='aws_default',
        config={

            'AlgorithmSpecification': {
            'TrainingImage': "382416733822.dkr.ecr.us-east-1.amazonaws.com/knn:1",
            'TrainingInputMode': 'File',
        },
        
        "InputDataConfig": [
                {"ChannelName": "train",
                 "DataSource": {
                     "S3DataSource": {
                         "S3DataType": "S3Prefix",
                         "S3Uri": "s3://churn-airflow/training-data",
                         "S3DataDistributionType": "FullyReplicated",
                     }
                 },
                 "ContentType": "text/csv",
                 "InputMode": "File"
                 }
            ],
            "OutputDataConfig": {
                "S3OutputPath": "s3://churn-airflow/churn-models"
            },
            "ResourceConfig": {
                "InstanceCount": 1,
                "InstanceType": "ml.m4.xlarge",
                "VolumeSizeInGB": 1
            },
            "RoleArn": "xyz",
            "StoppingCondition": {
                "MaxRuntimeInSeconds": 300
            },

            "HyperParameters": {
                "feature_dim": "8",  
                "k": "5",
                "predictor_type": "classifier",
                "sample_size": "10000",

            },
            "TrainingJobName": f"training-job-{current_datetime}"
        },
        wait_for_completion=True,
        dag=dag,
    )

deploy_job =  SageMakerModelOperator(
    task_id="deploy_sagemaker_model",
    config={
        "ModelName": f"churn-model-{current_datetime}", 
        "PrimaryContainer": {
            "Image": "382416733822.dkr.ecr.us-east-1.amazonaws.com/knn:1", 
            "ModelDataUrl": "s3://churn-airflow/churn-models/training-job-2024-03-22-18-39-43/output/model.tar.gz",  
        },
        "ExecutionRoleArn": "xyz",
    },
    aws_conn_id="aws_default", 
    dag = dag
)



#start_task>>sql_to_s3_task>>pre_processing_job>>modelling_job>>deploy_job>>end_task
start_task >> [preprocessing_to_s3,training_to_s3,prediction_to_s3,config_to_s3] >> pre_processing_job >> modelling_job >> deploy_job >> end_task


