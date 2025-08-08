resource "google_cloudbuild_trigger" "trigger_dataflow_template_builder" {
  project     = var.project_id
  location    = var.region
  name        = var.cloud_build_trigger_name
  description = var.cloud_build_trigger_description

  substitutions  = var.cloud_build_trigger_substitutions
  filename       = var.cloud_build_trigger_filename

  source_to_build {
    ref       = "refs/heads/${var.cloud_build_trigger_source_ref}"
    repo_type = "GITHUB"
    uri       = "${var.cloud_build_trigger_repository_owner}/${var.cloud_build_trigger_repository_name}"
  }

  github {
    owner = var.cloud_build_trigger_repository_owner
    name  = var.cloud_build_trigger_repository_name
    push {
      branch = var.cloud_build_trigger_regex_branch
    }
  }
}