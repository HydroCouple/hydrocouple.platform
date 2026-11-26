variable "name" {
  description = "Prefix for the identity names."
  type        = string
  default     = "hydrocouple"
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "create_storage" {
  description = "Create the storage account, or use storage_account_id."
  type        = bool
  default     = true
}

variable "storage_account_name" {
  description = "The account to create (3 to 24 lowercase letters and digits)."
  type        = string
  default     = null
}

variable "storage_account_id" {
  description = "An existing account, when create_storage is false."
  type        = string
  default     = null
}

variable "container_name" {
  type    = string
  default = "hydrocouple"
}

variable "replication" {
  type    = string
  default = "ZRS"
}

variable "public_network_access" {
  description = "Leave false and reach the account through a private endpoint (deploy/kustomize: hcp-runner-storage)."
  type        = bool
  default     = false
}

variable "deleted_blob_days" {
  description = "Days a deleted blob stays recoverable."
  type        = number
  default     = 7
}

variable "oidc_issuer_url" {
  description = "The AKS cluster's OIDC issuer URL (az aks show --query oidcIssuerProfile.issuerUrl)."
  type        = string
}

variable "namespace" {
  description = "The agent's namespace (deploy/kustomize)."
  type        = string
  default     = "hydrocouple"
}

variable "tags" {
  type    = map(string)
  default = {}
}
