variable "input_bucket_name" {
  description = "Name of the S3 bucket that receives input TSV files"
  type        = string
}

variable "output_bucket_name" {
  description = "Name of the S3 bucket for output attribution results"
  type        = string
}
