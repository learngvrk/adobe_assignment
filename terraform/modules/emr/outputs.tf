output "application_id" {
  description = "EMR Serverless application ID — used in start-job-run"
  value       = aws_emrserverless_application.this.id
}

output "application_arn" {
  description = "EMR Serverless application ARN"
  value       = aws_emrserverless_application.this.arn
}

output "job_role_arn" {
  description = "IAM execution role ARN for Spark jobs"
  value       = aws_iam_role.emr.arn
}
