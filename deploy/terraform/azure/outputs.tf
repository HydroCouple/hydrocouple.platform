output "store_uri" {
  description = "agent.json \"store\"."
  value       = "azure://${element(split("/", local.account_id), length(split("/", local.account_id)) - 1)}/${azurerm_storage_container.store.name}/hydrocouple"
}

output "agent_client_id" {
  description = "The hcp-agent ServiceAccount's azure.workload.identity/client-id."
  value       = azurerm_user_assigned_identity.this["agent"].client_id
}

output "runner_client_id" {
  description = "The hcp-runner ServiceAccount's azure.workload.identity/client-id."
  value       = azurerm_user_assigned_identity.this["runner"].client_id
}
