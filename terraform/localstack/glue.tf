# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name        = "xflow_db"
  description = "XFlow main Glue catalog database"

  tags = {
    Name        = "xflow-glue-db"
    Environment = var.environment
  }
}

# IAM Role for Glue Crawler
resource "aws_iam_role" "glue_crawler" {
  name = "xflow-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "xflow-glue-crawler-role"
    Environment = var.environment
  }
}

# IAM Policy for Glue Crawler to access S3
resource "aws_iam_role_policy" "glue_crawler_s3" {
  name = "xflow-glue-crawler-s3-policy"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}",
          "${aws_s3_bucket.data_lake.arn}/*",
          "${aws_s3_bucket.raw_data.arn}",
          "${aws_s3_bucket.raw_data.arn}/*",
          "${aws_s3_bucket.processed_data.arn}",
          "${aws_s3_bucket.processed_data.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# Glue Crawler for Raw Data (Parquet files)
resource "aws_glue_crawler" "raw_data" {
  name          = "xflow-raw-data-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name
  description   = "Crawler for raw data in Parquet format"

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  tags = {
    Name        = "xflow-raw-data-crawler"
    Environment = var.environment
    DataType    = "raw"
  }
}

# Glue Crawler for Processed Data (Parquet files)
resource "aws_glue_crawler" "processed_data" {
  name          = "xflow-processed-data-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name
  description   = "Crawler for processed data in Parquet format"

  s3_target {
    path = "s3://${aws_s3_bucket.processed_data.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  tags = {
    Name        = "xflow-processed-data-crawler"
    Environment = var.environment
    DataType    = "processed"
  }
}

# Glue Crawler for Data Lake (Parquet files)
resource "aws_glue_crawler" "data_lake" {
  name          = "xflow-data-lake-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name
  description   = "Crawler for data lake in Parquet format"

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })

  tags = {
    Name        = "xflow-data-lake-crawler"
    Environment = var.environment
    DataType    = "datalake"
  }
}

# Products 테이블용 크롤러
resource "aws_glue_crawler" "products" {
  name          = "xflow-products-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name
  description   = "Crawler for products data"

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.bucket}/products/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Name        = "xflow-products-crawler"
    Environment = var.environment
    DataType    = "products"
  }
}

# User Events 테이블용 크롤러
resource "aws_glue_crawler" "user_events" {
  name          = "xflow-user-events-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name
  description   = "Crawler for user events data"

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.bucket}/user_events/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Name        = "xflow-user-events-crawler"
    Environment = var.environment
    DataType    = "user_events"
  }
}

# Transactions 테이블용 크롤러
resource "aws_glue_crawler" "transactions" {
  name          = "xflow-transactions-crawler"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.main.name
  description   = "Crawler for transactions data"

  s3_target {
    path = "s3://${aws_s3_bucket.raw_data.bucket}/transactions/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Name        = "xflow-transactions-crawler"
    Environment = var.environment
    DataType    = "transactions"
  }
}
