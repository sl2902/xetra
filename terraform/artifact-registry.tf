resource "google_artifact_registry_repository" "my-project" {
  repository_id = "xetra"
  location      = var.GCP_REGION
  format        = "docker"
}