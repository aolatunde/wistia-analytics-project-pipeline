resource "aws_scheduler_schedule" "this" {
  name                         = var.schedule_name
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.timezone
  state                        = var.enabled ? "ENABLED" : "DISABLED"

  flexible_time_window { mode = "OFF" }

  target {
    arn      = var.state_machine_arn
    role_arn = var.scheduler_role_arn
    input    = var.input

    retry_policy {
      maximum_event_age_in_seconds = 3600
      maximum_retry_attempts       = 2
    }
  }
}
