variable "project_id" {
  type = string
}

variable "name" {
  description = "Prefix for the service account IDs."
  type        = string
  default     = "hydrocouple"
}

variable "bucket_name" {
  type = string
}

variable "create_bucket" {
  type    = bool
  default = true
}

variable "location" {
  description = "The bucket's location, in the cluster's region."
  type        = string
}

variable "prefix" {
  description = "The store's object prefix, ending in a slash."
  type        = string
  default     = "hydrocouple/"
  validation {
    condition     = can(regex("^([^/]+/)+$", var.prefix))
    error_message = "The prefix must end in a slash and have no empty segments."
  }
}

variable "namespace" {
  description = "The agent's namespace (deploy/kustomize)."
  type        = string
  default     = "hydrocouple"
}

variable "noncurrent_version_days" {
  type    = number
  default = 30
}

variable "labels" {
  type    = map(string)
  default = {}
}
