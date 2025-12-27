# S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "xflow-data-lake"

  tags = {
    Name        = "xflow-data-lake"
    Environment = var.environment
  }
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket for Raw Data
resource "aws_s3_bucket" "raw_data" {
  bucket = "xflow-raw-data"

  tags = {
    Name        = "xflow-raw-data"
    Environment = var.environment
    Type        = "raw"
  }
}

# S3 Bucket for Processed Data
resource "aws_s3_bucket" "processed_data" {
  bucket = "xflow-processed-data"

  tags = {
    Name        = "xflow-processed-data"
    Environment = var.environment
    Type        = "processed"
  }
}
