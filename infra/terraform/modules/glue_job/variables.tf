variable "job_name" {
  type = string
}

variable "role_arn" {
  type = string
}

variable "script_location" {
  type = string
}

variable "glue_version" {
  type    = string
  default = "5.0"
}

variable "worker_type" {
  type    = string
  default = "G.1X"
}

variable "number_of_workers" {
  type    = number
  default = 2
}

variable "timeout" {
  type    = number
  default = 480
}

variable "default_arguments" {
  type    = map(string)
  default = {}
}

variable "tags" {
  type    = map(string)
  default = {}
}
