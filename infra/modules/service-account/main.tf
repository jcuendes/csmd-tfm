resource "google_service_account" "service_account" {
  project      = var.project_id
  account_id   = var.sa_account_id
  display_name = var.sa_display_name
  description  = var.sa_description
  disabled     = var.sa_disabled
}

resource "google_project_iam_member" "roles" {
  project  = var.project_id
  for_each = toset(var.sa_roles)
  role     = each.value
  member   = "serviceAccount:${google_service_account.service_account.email}"
}