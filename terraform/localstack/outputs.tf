output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

output "public_subnet_id" {
  description = "Public subnet ID"
  value       = aws_subnet.public.id
}

output "private_subnet_id" {
  description = "Private subnet ID"
  value       = aws_subnet.private.id
}

output "internet_gateway_id" {
  description = "Internet Gateway ID"
  value       = aws_internet_gateway.main.id
}

output "security_group_id" {
  description = "Default security group ID"
  value       = aws_security_group.default.id
}

output "route_table_id" {
  description = "Public route table ID"
  value       = aws_route_table.public.id
}

output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_instance_id" {
  description = "RDS instance ID"
  value       = aws_db_instance.postgres.id
}

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

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.main.name
}

output "glue_crawler_raw_data" {
  description = "Glue crawler name for raw data"
  value       = aws_glue_crawler.raw_data.name
}

output "glue_crawler_processed_data" {
  description = "Glue crawler name for processed data"
  value       = aws_glue_crawler.processed_data.name
}

output "glue_crawler_data_lake" {
  description = "Glue crawler name for data lake"
  value       = aws_glue_crawler.data_lake.name
}

# output "athena_workgroup_name" {
#   description = "Athena workgroup name"
#   value       = aws_athena_workgroup.main.name
# }

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.athena_results.bucket
}
