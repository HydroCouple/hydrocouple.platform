variable "name" {
  description = "Prefix for the IAM role names."
  type        = string
  default     = "hydrocouple"
}

variable "bucket_name" {
  description = "The store's bucket."
  type        = string
}

variable "create_bucket" {
  description = "Create the bucket, or use an existing one."
  type        = bool
  default     = true
}

variable "prefix" {
  description = "The store's key prefix in the bucket, ending in a slash."
  type        = string
  default     = "hydrocouple/"
  validation {
    condition     = can(regex("^([^/]+/)+$", var.prefix))
    error_message = "The prefix must end in a slash and have no empty segments."
  }
}

variable "oidc_provider_arn" {
  description = "The EKS cluster's IAM OIDC provider ARN."
  type        = string
}

variable "oidc_provider_url" {
  description = "The EKS cluster's OIDC issuer, without https:// (oidc.eks.REGION.amazonaws.com/id/...)."
  type        = string
}

variable "namespace" {
  description = "The agent's namespace (deploy/kustomize)."
  type        = string
  default     = "hydrocouple"
}

variable "noncurrent_version_days" {
  description = "Days before an overwritten or deleted object's old version expires."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags for every resource (plan §3: org, project)."
  type        = map(string)
  default     = {}
}
