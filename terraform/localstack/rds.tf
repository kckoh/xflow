# RDS Subnet Group
resource "aws_db_subnet_group" "main" {
  name       = "xflow-db-subnet-group"
  subnet_ids = [aws_subnet.private.id, aws_subnet.private_2.id]

  tags = {
    Name        = "xflow-db-subnet-group"
    Environment = var.environment
  }
}

# RDS Instance
resource "aws_db_instance" "postgres" {
  identifier             = "xflow-postgres"
  engine                 = "postgres"
  engine_version         = "14.7"
  instance_class         = "db.t3.micro"
  allocated_storage      = 20
  storage_type           = "gp2"
  db_name                = var.rds_database_name
  username               = var.rds_username
  password               = var.rds_password
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.default.id]
  skip_final_snapshot    = true
  publicly_accessible    = false

  tags = {
    Name        = "xflow-postgres"
    Environment = var.environment
  }
}
