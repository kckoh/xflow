# OpenSearch Domain for Data Catalog Search
resource "aws_opensearch_domain" "catalog_search" {
  domain_name    = "xflow-catalog-search"
  engine_version = "OpenSearch_2.7"

  # ========== Cluster Configuration ==========
  # LocalStack (Development)
  cluster_config {
    instance_type  = "t3.small.search"
    instance_count = 1
  }

  # Production (주석 해제)
  # cluster_config {
  #   instance_type            = "r6g.large.search"  # 2 vCPU, 16GB RAM
  #   instance_count           = 3                    # High Availability
  #   zone_awareness_enabled   = true
  #   availability_zone_count  = 3
  #   dedicated_master_enabled = true
  #   dedicated_master_type    = "r6g.large.search"
  #   dedicated_master_count   = 3
  # }

  # ========== Storage ==========
  ebs_options {
    ebs_enabled = true
    volume_size = 10  # LocalStack: 10GB, Production: 100GB+
    volume_type = "gp3"
  }

  # ========== VPC Configuration (Production only) ==========
  # Production: 주석 해제하고 실제 subnet/security group ID 입력
  # vpc_options {
  #   subnet_ids         = ["subnet-xxxxx", "subnet-yyyyy", "subnet-zzzzz"]
  #   security_group_ids = ["sg-xxxxx"]
  # }

  # ========== Security Options ==========
  # LocalStack (Development)
  advanced_security_options {
    enabled                        = false
    internal_user_database_enabled = false
  }

  # Production (주석 해제)
  # advanced_security_options {
  #   enabled                        = true
  #   internal_user_database_enabled = true
  #   master_user_options {
  #     master_user_name     = "admin"
  #     master_user_password = "ChangeMe123!"  # TODO: Use AWS Secrets Manager
  #   }
  # }

  # ========== Encryption ==========
  # LocalStack (Development)
  encrypt_at_rest {
    enabled = false
  }

  # Production (주석 해제)
  # encrypt_at_rest {
  #   enabled    = true
  #   kms_key_id = aws_kms_key.opensearch.arn  # TODO: Create KMS key
  # }

  node_to_node_encryption {
    enabled = false  # LocalStack: false, Production: true
  }

  # ========== Domain Endpoint ==========
  # LocalStack (Development)
  domain_endpoint_options {
    enforce_https       = false
    tls_security_policy = "Policy-Min-TLS-1-0-2019-07"
  }

  # Production (주석 해제)
  # domain_endpoint_options {
  #   enforce_https       = true
  #   tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  # }

  # ========== Access Policy ==========
  # LocalStack (Development) - 모두 허용
  access_policies = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "*"
        }
        Action   = "es:*"
        Resource = "arn:aws:es:${var.aws_region}:000000000000:domain/xflow-catalog-search/*"
      }
    ]
  })

  # Production (주석 해제) - 특정 Role만 허용
  # access_policies = jsonencode({
  #   Version = "2012-10-17"
  #   Statement = [{
  #     Effect = "Allow"
  #     Principal = {
  #       AWS = "arn:aws:iam::YOUR_ACCOUNT_ID:role/xflow-backend-role"  # TODO: Replace
  #     }
  #     Action   = "es:*"
  #     Resource = "arn:aws:es:${var.aws_region}:YOUR_ACCOUNT_ID:domain/xflow-catalog-search/*"
  #   }]
  # })

  # ========== CloudWatch Logs (Production recommended) ==========
  # log_publishing_options {
  #   cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch.arn
  #   log_type                 = "INDEX_SLOW_LOGS"
  # }
  #
  # log_publishing_options {
  #   cloudwatch_log_group_arn = aws_cloudwatch_log_group.opensearch.arn
  #   log_type                 = "SEARCH_SLOW_LOGS"
  # }

  tags = {
    Name        = "xflow-catalog-search"
    Environment = var.environment
    Purpose     = "data-catalog-search"
  }
}

# OpenSearch 도메인 엔드포인트 출력
output "opensearch_endpoint" {
  value       = aws_opensearch_domain.catalog_search.endpoint
  description = "OpenSearch domain endpoint"
}

output "opensearch_domain_name" {
  value       = aws_opensearch_domain.catalog_search.domain_name
  description = "OpenSearch domain name"
}
