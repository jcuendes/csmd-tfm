resource "google_artifact_registry_repository" "repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_naming
  description   = var.artifact_registry_description
  format        = var.artifact_format

}
