###################
#### REQUIRED #####
###################

# Global
variable "project_id" {
  description = "The ID of the Google Cloud project to use."
  type = string
}

# Dataset
variable "bq_dataset_id" {
    description = "The ID of the BigQuery dataset to create. This must be unique across all datasets in the project."
    type        = string
}

variable "bq_table_id" {
  description = "A unique ID for the resource. Changing this forces a new resource to be created."
  type        = string
}

###################
#### OPTIONAL #####
###################

# Dataset
variable "bq_dataset_location" {
  type    = string
  default = "EU"
}

variable "bq_dataset_description" {
  type    = string
  default = "BigQuery dataset created by Terraform" // Descripción del dataset, si no se especifica se usa un valor por defecto
}

variable "bq_dataset_labels" {
  type    = map(string)
  default = null
}

variable "bq_dataset_delete_contents_on_destroy" {
  type    = bool
  default = true // Si se establece en true, se eliminarán los contenidos del dataset al destruir el recurso. Por defecto es true.
}

# Table
variable "bq_table_expiration_time" {
  description = "The time when this table expires, in milliseconds since the epoch. If not present, the table will persist indefinitely. Expired tables will be deleted and their storage reclaimed."
  default     = null

}
variable "bq_table_friendly_name" {
  description = "A descriptive name for the table"
  default     = ""
}
variable "bq_table_clustering" {
  description = "Specifies column names to use for data clustering. Up to four top_level columns are allowed, and should be specified in descending priority order."
  type        = list(any)
  default     = null
}
variable "bq_table_deletion_protection" {
  description = "Whether or not to allow Terraform to destroy the instance. Unless this field is set to false in Terraform state, a terraform destroy or terraform apply that would delete the instance will fail."
  type        = bool
  default     = true
}