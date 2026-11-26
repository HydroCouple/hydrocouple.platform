output "store_uri" {
  description = "agent.json \"store\"."
  value       = "gs://${local.bucket}/${trimsuffix(var.prefix, "/")}"
}

output "agent_service_account" {
  description = "The hcp-agent ServiceAccount's iam.gke.io/gcp-service-account."
  value       = google_service_account.this["agent"].email
}

output "runner_service_account" {
  description = "The hcp-runner ServiceAccount's iam.gke.io/gcp-service-account."
  value       = google_service_account.this["runner"].email
}
