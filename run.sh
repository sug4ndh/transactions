aws emr-serverless start-job-run \
  --application-id 00g4imca24pthf1d \
  --execution-role-arn arn:aws:iam::195874016307:role/EMRServerlessExecutionRole \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://input-transactions/code/main.py",
      "entryPointArguments": ["--approach", "selfjoin"],
      "sparkSubmitParameters": "--py-files s3://input-transactions/code/pipeline_code.zip"
    }
  }' \
  --configuration-overrides '{
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "s3://input-transactions/logs/"
      }
    }
  }' \
  --region eu-north-1
