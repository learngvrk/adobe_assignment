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
