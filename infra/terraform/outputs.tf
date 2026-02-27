output "cloud_run_url" {
  description = "Cloud Run service URL"
  value       = google_cloud_run_v2_service.processor.uri
}

output "bucket_name" {
  description = "GCS bucket for log uploads"
  value       = google_storage_bucket.pinpad_logs.name
}

output "pubsub_topic" {
  description = "Pub/Sub topic receiving GCS notifications"
  value       = google_pubsub_topic.log_uploads.name
}

output "artifact_registry_repo" {
  description = "Artifact Registry repository path"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pinpad.repository_id}"
}

output "wif_provider" {
  description = "Workload Identity Federation provider for GitHub Actions"
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "github_sa_email" {
  description = "Service account email for GitHub Actions"
  value       = google_service_account.github_actions.email
}
