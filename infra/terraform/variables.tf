variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "GCS bucket name for journal log uploads"
  type        = string
  default     = "pinpad-logs"
}

variable "pinpad_db" {
  description = "Database connection string (MotherDuck URI, e.g. md:pinpad_analyzer)"
  type        = string
  default     = "md:pinpad_analyzer"
}

variable "motherduck_token" {
  description = "MotherDuck authentication token"
  type        = string
  sensitive   = true
}

variable "github_repo" {
  description = "GitHub repository (owner/repo) for Workload Identity Federation"
  type        = string
}
