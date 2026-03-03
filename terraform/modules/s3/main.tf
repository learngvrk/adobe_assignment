# S3 module — creates input and output buckets for the pipeline.
#
# The input bucket receives hit-level TSV files and triggers Lambda.
# The output bucket stores the tab-delimited attribution results.

resource "aws_s3_bucket" "input" {
  bucket = var.input_bucket_name
}

resource "aws_s3_bucket" "output" {
  bucket = var.output_bucket_name
}

# Block all public access — these buckets are internal pipeline resources
resource "aws_s3_bucket_public_access_block" "input" {
  bucket                  = aws_s3_bucket.input.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "output" {
  bucket                  = aws_s3_bucket.output.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
