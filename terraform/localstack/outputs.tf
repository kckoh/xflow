output "s3_data_lake_bucket" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "s3_raw_data_bucket" {
  description = "S3 raw data bucket name"
  value       = aws_s3_bucket.raw_data.bucket
}

output "s3_processed_data_bucket" {
  description = "S3 processed data bucket name"
  value       = aws_s3_bucket.processed_data.bucket
}
