resource "aws_redshiftserverless_namespace" "this" {
  namespace_name      = var.namespace_name
  db_name             = var.database_name
  admin_username      = var.admin_username
  admin_user_password = var.admin_password
  tags                = var.tags
}

resource "aws_redshiftserverless_workgroup" "this" {
  workgroup_name      = var.workgroup_name
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  base_capacity       = var.base_capacity
  publicly_accessible = var.publicly_accessible
  tags                = var.tags
}
