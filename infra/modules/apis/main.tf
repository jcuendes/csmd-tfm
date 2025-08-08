resource "google_project_service" "apis_to_enable" {
  for_each = toset(var.apis)
  
  project  = var.project_id
  service  = "${each.value}.googleapis.com"

  disable_on_destroy = true
  disable_dependent_services=true
}