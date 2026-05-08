variable "project_name" {
  type = string
}

variable "data_bucket_arn" {
  type = string
}

variable "watermark_table_arn" {
  type = string
}

variable "wistia_secret_arn" {
  type = string
}

variable "state_machine_arn" {
  type    = string
  default = "*"
}

variable "tags" {
  type    = map(string)
  default = {}
}
