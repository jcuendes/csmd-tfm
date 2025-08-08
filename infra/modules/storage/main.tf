resource "google_storage_bucket" "bucket" {
  project                  = var.project_id
  name                     = var.bucket_name
  location                 = var.bucket_location
  force_destroy            = var.bucket_force_destroy
  storage_class            = var.bucket_class
  public_access_prevention = var.bucket_public_access_prevention

  uniform_bucket_level_access = var.bucket_uniform_bucket_level_access

  dynamic "soft_delete_policy" {
    for_each = var.bucket_with_soft_delete ? [1] : []
    content {
      retention_duration_seconds = var.bucket_retention_duration_seconds
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.bucket_with_lifecycle_rule ? [1] : []
    content {
      action {
        type = var.bucket_lifecycle_rule_action_type
      }
      condition {
        age = var.bucket_lifecycle_rule_age
        matches_prefix = var.bucket_lifecycle_rule_matches_prefix
      }
    }
  }
}

resource "google_storage_bucket_object" "file" {
  count = var.bucket_with_object ? 1 : 0

  bucket = google_storage_bucket.bucket.name
  name   = var.bucket_object_name
  source = var.bucket_object_source
}