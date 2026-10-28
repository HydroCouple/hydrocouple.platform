# The Azure side of a Kubernetes target on AKS: the store (a Blob Storage container) and
# two user-assigned identities federated with the cluster's OIDC issuer, one for the
# agent and one for the runners. The account allows no shared keys: the agent signs
# grants with user-delegation keys. The cluster, its node pools and Kueue are the site's
# own; this module needs the cluster's OIDC issuer URL (workload identity enabled).

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.0"
    }
  }
}

locals {
  accounts = {
    agent  = "hcp-agent"
    runner = "hcp-runner"
  }
  account_id = var.create_storage ? azurerm_storage_account.store[0].id : var.storage_account_id
}

resource "azurerm_storage_account" "store" {
  count                           = var.create_storage ? 1 : 0
  name                            = var.storage_account_name
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = var.replication
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = false
  allow_nested_items_to_be_public = false
  public_network_access_enabled   = var.public_network_access
  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = var.deleted_blob_days
    }
  }
  tags = var.tags
}

resource "azurerm_storage_container" "store" {
  name                  = var.container_name
  storage_account_id    = local.account_id
  container_access_type = "private"
}

resource "azurerm_user_assigned_identity" "this" {
  for_each            = local.accounts
  name                = "${var.name}-${each.key}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags
}

resource "azurerm_federated_identity_credential" "this" {
  for_each            = local.accounts
  name                = "${var.name}-${each.key}"
  resource_group_name = var.resource_group_name
  parent_id           = azurerm_user_assigned_identity.this[each.key].id
  issuer              = var.oidc_issuer_url
  subject             = "system:serviceaccount:${var.namespace}:${each.value}"
  audience            = ["api://AzureADTokenExchange"]
}

# Both identities use the container. The agent also needs to obtain user-delegation keys
# for grants, which is an account-level action.
resource "azurerm_role_assignment" "data" {
  for_each             = local.accounts
  scope                = azurerm_storage_container.store.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.this[each.key].principal_id
}

resource "azurerm_role_assignment" "delegator" {
  scope                = local.account_id
  role_definition_name = "Storage Blob Delegator"
  principal_id         = azurerm_user_assigned_identity.this["agent"].principal_id
}
