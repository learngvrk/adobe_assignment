output "input_bucket" {
  description = "Upload TSV files here to trigger processing"
  value       = module.s3.input_bucket_name
}

output "output_bucket" {
  description = "Attribution results are written here"
  value       = module.s3.output_bucket_name
}

output "lambda_function" {
  description = "Name of the Lambda function"
  value       = module.lambda.function_name
}

output "emr_application_id" {
  description = "EMR Serverless application ID — use with scripts/submit_emr_job.sh"
  value       = module.emr.application_id
}

output "emr_job_role_arn" {
  description = "IAM role ARN for EMR Serverless job execution"
  value       = module.emr.job_role_arn
}
