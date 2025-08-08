output "dataset" {
  description = "Dataset name"
  value       = google_bigquery_dataset.dataset.dataset_id
}

output "table" {
  description = "Table name"
  value = google_bigquery_table.table.id
}