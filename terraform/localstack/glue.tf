# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name        = "xflow_db"
  description = "XFlow main Glue catalog database"

  tags = {
    Name        = "xflow-glue-db"
    Environment = var.environment
  }
}
