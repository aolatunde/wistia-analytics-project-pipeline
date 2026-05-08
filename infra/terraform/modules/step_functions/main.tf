resource "aws_cloudwatch_log_group" "sfn" {
  name              = var.log_group_name
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_sfn_state_machine" "this" {
  name     = var.state_machine_name
  role_arn = var.role_arn
  type     = "STANDARD"

  definition = file(var.definition_path)

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tags = var.tags
}
