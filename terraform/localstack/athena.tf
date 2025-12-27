# S3 Bucket for Athena Query Results
resource "aws_s3_bucket" "athena_results" {
  bucket = "xflow-athena-results"

  tags = {
    Name        = "xflow-athena-results"
    Environment = var.environment
    Purpose     = "athena-query-results"
  }
}

# Athena Workgroup
resource "aws_athena_workgroup" "main" {
  name        = "xflow-workgroup"
  description = "XFlow main Athena workgroup for querying data lake"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "AUTO"
    }
  }

  tags = {
    Name        = "xflow-athena-workgroup"
    Environment = var.environment
  }
}

# Athena Named Query (Example - optional)
resource "aws_athena_named_query" "example" {
  name        = "show_databases"
  description = "Example query to show all databases"
  workgroup   = aws_athena_workgroup.main.id
  database    = aws_glue_catalog_database.main.name
  query       = "SHOW DATABASES;"
}
