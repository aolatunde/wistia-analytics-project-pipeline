variable "schedule_name" {
  type = string
}

variable "state_machine_arn" {
  type = string
}

variable "scheduler_role_arn" {
  type = string
}

variable "schedule_expression" {
  type = string
}

variable "timezone" {
  type    = string
  default = "America/Los_Angeles"
}

variable "input" {
  type    = string
  default = "{}"
}

variable "enabled" {
  type    = bool
  default = true
}

variable "tags" {
  type    = map(string)
  default = {}
}
