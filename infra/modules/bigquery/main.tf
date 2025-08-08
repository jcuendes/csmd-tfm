resource "google_bigquery_dataset" "dataset" {
  dataset_id                      = var.bq_dataset_id
  location                        = var.bq_dataset_location
  description                     = var.bq_dataset_description
  friendly_name                   = var.bq_dataset_id
  labels                          = var.bq_dataset_labels
  delete_contents_on_destroy      = var.bq_dataset_delete_contents_on_destroy
}

resource "google_bigquery_table" "table" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = var.bq_table_id
  expiration_time     = var.bq_table_expiration_time
  friendly_name       = var.bq_table_friendly_name
  clustering          = var.bq_table_clustering
  deletion_protection = var.bq_table_deletion_protection
  schema = <<EOF
  [
    {
      "name": "make",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "make"
    },
    {
      "name": "model",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "model"
    },
    {
      "name": "version",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "version"
    },
    {
      "name": "price",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "price"
    },
    {
      "name": "fuel",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "fuel"
    },
    {
      "name": "year",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "year"
    },

    {
      "name": "kms",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "kms"
    },

    {
      "name": "power",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "power"
    },

    {
      "name": "doors",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "doors"
    },
    {
      "name": "shift",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "shift"
    },
    {
      "name": "color",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "color"
    },
    {
      "name": "province",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "province"
    },
    {
      "name": "country",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "country"
    },
    {
      "name": "publish_date",
      "type": "DATETIME",
      "mode": "NULLABLE",
      "description": "publish_date"
    }
  ]
  EOF
}
