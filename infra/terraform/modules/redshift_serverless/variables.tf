variable "namespace_name" {
  type = string
}

variable "workgroup_name" {
  type = string
}

variable "database_name" {
  type = string
}

variable "admin_username" {
  type = string
}

variable "admin_password" {
  type      = string
  sensitive = true
}

variable "base_capacity" {
  type    = number
  default = 8
}

variable "publicly_accessible" {
  type    = bool
  default = false
}

variable "tags" {
  type    = map(string)
  default = {}
}
