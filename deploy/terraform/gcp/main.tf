# The Google Cloud side of a Kubernetes target on GKE: the store (a Cloud Storage bucket
# and prefix) and two service accounts bound to the agent's and the runners' Kubernetes
# ServiceAccounts through Workload Identity Federation for GKE. IAM conditions keep each
# to the prefix. The agent may sign URLs as itself, for grants. The cluster (with
# Workload Identity and Dataplane V2), its node pools and Kueue are the site's own.

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

locals {
  bucket = var.create_bucket ? google_storage_bucket.store[0].name : var.bucket_name
  accounts = {
    agent  = "hcp-agent"
    runner = "hcp-runner"
  }
  objects = "projects/_/buckets/${local.bucket}/objects/${var.prefix}"
}

resource "google_storage_bucket" "store" {
  count                       = var.create_bucket ? 1 : 0
  project                     = var.project_id
  name                        = var.bucket_name
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  versioning {
    enabled = true
  }
  lifecycle_rule {
    # Deletion (plan §12) removes live objects; noncurrent versions expire after this.
    condition {
      days_since_noncurrent_time = var.noncurrent_version_days
    }
    action {
      type = "Delete"
    }
  }
  labels = var.labels
}

resource "google_service_account" "this" {
  for_each     = local.accounts
  project      = var.project_id
  account_id   = "${var.name}-${each.key}"
  display_name = "HydroCouple ${each.key} (${each.value})"
}

resource "google_service_account_iam_member" "workload_identity" {
  for_each           = local.accounts
  service_account_id = google_service_account.this[each.key].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.namespace}/${each.value}]"
}

# Signed URLs under workload identity are signed through the IAM Credentials API.
resource "google_service_account_iam_member" "agent_signs_urls" {
  service_account_id = google_service_account.this["agent"].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.this["agent"].email}"
}

resource "google_storage_bucket_iam_member" "agent" {
  bucket = local.bucket
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.this["agent"].email}"
  # Listing (for erasure) is checked on the bucket, and a condition can't see the listed
  # prefix, so the agent may list the bucket's object names; it reads and writes only
  # the prefix.
  condition {
    title      = "hydrocouple-prefix"
    expression = "resource.name.startsWith(\"${local.objects}\") || resource.name == \"projects/_/buckets/${local.bucket}\""
  }
}

resource "google_storage_bucket_iam_member" "runner_reads_revisions" {
  bucket = local.bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.this["runner"].email}"
  condition {
    title      = "hydrocouple-revisions"
    expression = "resource.name.startsWith(\"${local.objects}revisions/\")"
  }
}

resource "google_storage_bucket_iam_member" "runner_writes_runs" {
  bucket = local.bucket
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.this["runner"].email}"
  condition {
    title      = "hydrocouple-runs"
    expression = "resource.name.startsWith(\"${local.objects}runs/\")"
  }
}
