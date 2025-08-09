resource "google_cloudbuild_trigger" "cb_dataflow_template_builder" {
  project     = var.project_id
  location    = var.region
  name        = var.cloud_build_trigger_name
  description = var.cloud_build_trigger_description

  service_account = var.cloud_build_service_account_email

  filename       = var.cloud_build_trigger_filename
  substitutions  = var.cloud_build_trigger_substitutions

  github {
    owner = var.cloud_build_trigger_repository_owner
    name  = var.cloud_build_trigger_repository_name
    push {
      branch = var.cloud_build_trigger_regex_branch
    }
  }
}