variable "state_machine_name" {
  type = string
}

variable "role_arn" {
  type = string
}

variable "definition_path" {
  type = string
}

variable "log_group_name" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
