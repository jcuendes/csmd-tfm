output "artifact-registry-name" {
  description = "Artifact Registry name"
  value       = google_artifact_registry_repository.repo.name
}