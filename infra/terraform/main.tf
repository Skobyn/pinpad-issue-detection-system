terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    # Configured via -backend-config at init time:
    #   terraform init -backend-config="bucket=<PROJECT_ID>-tfstate"
    prefix = "terraform/pinpad"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------- Artifact Registry ----------

resource "google_artifact_registry_repository" "pinpad" {
  location      = var.region
  repository_id = "pinpad"
  format        = "DOCKER"
  description   = "Pinpad log processor container images"
}

# ---------- GCS Bucket ----------

resource "google_storage_bucket" "pinpad_logs" {
  name     = var.bucket_name
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ---------- Pub/Sub ----------

resource "google_pubsub_topic" "log_uploads" {
  name = "pinpad-log-uploads"
}

resource "google_pubsub_topic" "dead_letter" {
  name = "pinpad-dead-letter"
}

# GCS notification -> Pub/Sub
resource "google_storage_notification" "log_upload_notification" {
  bucket         = google_storage_bucket.pinpad_logs.name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.log_uploads.id
  event_types    = ["OBJECT_FINALIZE"]

  depends_on = [google_pubsub_topic_iam_member.gcs_publisher]
}

# Push subscription -> Cloud Run
resource "google_pubsub_subscription" "processor_push" {
  name  = "pinpad-processor-push"
  topic = google_pubsub_topic.log_uploads.id

  ack_deadline_seconds = 300

  push_config {
    push_endpoint = "${google_cloud_run_v2_service.processor.uri}/process"

    oidc_token {
      service_account_email = google_service_account.pubsub_invoker.email
    }
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  depends_on = [google_cloud_run_v2_service.processor]
}

# ---------- Secret Manager ----------

resource "google_secret_manager_secret" "motherduck_token" {
  secret_id = "motherduck-token"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "motherduck_token" {
  secret      = google_secret_manager_secret.motherduck_token.id
  secret_data = var.motherduck_token
}

# ---------- Service Accounts ----------

resource "google_service_account" "cloud_run" {
  account_id   = "pinpad-processor"
  display_name = "Pinpad Cloud Run Processor"
}

resource "google_service_account" "pubsub_invoker" {
  account_id   = "pinpad-pubsub-invoker"
  display_name = "Pub/Sub -> Cloud Run Invoker"
}

# ---------- Cloud Run ----------

resource "google_cloud_run_v2_service" "processor" {
  name     = "pinpad-processor"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }

    timeout = "900s"

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/pinpad/processor:latest"

      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
      }

      env {
        name  = "PINPAD_DB"
        value = var.pinpad_db
      }

      env {
        name = "MOTHERDUCK_TOKEN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.motherduck_token.secret_id
            version = "latest"
          }
        }
      }

      env {
        name  = "LOG_LEVEL"
        value = "INFO"
      }

      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 5
        period_seconds        = 10
        failure_threshold     = 3
      }
    }

    service_account = google_service_account.cloud_run.email
  }
}

# ---------- IAM Bindings ----------

# GCS -> Pub/Sub publisher
data "google_storage_project_service_account" "gcs_sa" {
}

resource "google_pubsub_topic_iam_member" "gcs_publisher" {
  topic  = google_pubsub_topic.log_uploads.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}

# Cloud Run SA -> GCS object read/write (download files, move to processed)
resource "google_storage_bucket_iam_member" "cloud_run_gcs_access" {
  bucket = google_storage_bucket.pinpad_logs.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cloud_run.email}"
}

# Pub/Sub invoker -> Cloud Run
resource "google_cloud_run_v2_service_iam_member" "pubsub_invoker" {
  name     = google_cloud_run_v2_service.processor.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}

# Cloud Run SA -> Secret Manager accessor
resource "google_secret_manager_secret_iam_member" "cloud_run_secret_access" {
  secret_id = google_secret_manager_secret.motherduck_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.cloud_run.email}"
}

# Dead letter topic - Pub/Sub SA needs publisher role
resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  topic  = google_pubsub_topic.dead_letter.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Dead letter subscription - Pub/Sub SA needs subscriber role on source
resource "google_pubsub_subscription_iam_member" "dead_letter_subscriber" {
  subscription = google_pubsub_subscription.processor_push.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

data "google_project" "current" {
}

# ---------- Workload Identity Federation (GitHub Actions) ----------

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions Pool"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub Actions Provider"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "assertion.repository == '${var.github_repo}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "github_actions" {
  account_id   = "github-actions-deploy"
  display_name = "GitHub Actions CI/CD"
}

resource "google_service_account_iam_member" "github_wif" {
  service_account_id = google_service_account.github_actions.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

# GitHub Actions SA roles
resource "google_project_iam_member" "github_ar_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.github_actions.email}"
}

resource "google_project_iam_member" "github_run_developer" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.github_actions.email}"
}

resource "google_project_iam_member" "github_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.github_actions.email}"
}
