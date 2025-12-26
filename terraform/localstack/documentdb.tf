# DocumentDB Cluster Parameter Group
resource "aws_docdb_cluster_parameter_group" "main" {
  family      = "docdb5.0"
  name        = "xflow-docdb-params"
  description = "XFlow DocumentDB cluster parameter group"

  parameter {
    name  = "tls"
    value = "disabled"
  }

  tags = {
    Name        = "xflow-docdb-params"
    Environment = var.environment
  }
}

# DocumentDB Subnet Group
resource "aws_docdb_subnet_group" "main" {
  name       = "xflow-docdb-subnet-group"
  subnet_ids = [aws_subnet.private.id, aws_subnet.private_2.id]

  tags = {
    Name        = "xflow-docdb-subnet-group"
    Environment = var.environment
  }
}

# DocumentDB Cluster
resource "aws_docdb_cluster" "main" {
  cluster_identifier              = "xflow-docdb-cluster"
  engine                          = "docdb"
  master_username                 = var.documentdb_username
  master_password                 = var.documentdb_password
  backup_retention_period         = 1
  preferred_backup_window         = "07:00-09:00"
  skip_final_snapshot             = true
  db_subnet_group_name            = aws_docdb_subnet_group.main.name
  db_cluster_parameter_group_name = aws_docdb_cluster_parameter_group.main.name
  vpc_security_group_ids          = [aws_security_group.default.id]

  tags = {
    Name        = "xflow-docdb-cluster"
    Environment = var.environment
  }
}

# DocumentDB Cluster Instance
resource "aws_docdb_cluster_instance" "main" {
  identifier         = "xflow-docdb-instance"
  cluster_identifier = aws_docdb_cluster.main.id
  instance_class     = "db.t3.medium"

  tags = {
    Name        = "xflow-docdb-instance"
    Environment = var.environment
  }
}
