output "namespace_name" { value = aws_redshiftserverless_namespace.this.namespace_name }
output "workgroup_name" { value = aws_redshiftserverless_workgroup.this.workgroup_name }
output "endpoint" { value = aws_redshiftserverless_workgroup.this.endpoint }
