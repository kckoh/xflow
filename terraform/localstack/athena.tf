# S3 Bucket for Athena Query Results
resource "aws_s3_bucket" "athena_results" {
  bucket = "xflow-athena-results"

  tags = {
    Name        = "xflow-athena-results"
    Environment = var.environment
    Purpose     = "athena-query-results"
  }
}