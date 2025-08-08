###################
#### REQUIRED #####
###################

# Global
variable "project_id" {
  description = "The ID of the Google Cloud project to use."
  type        = string
}

# API
variable "apis" {
  description = "List of APIs to enable in the project."
  type        = list(string)
  default     = [
    "artifactregistry",
    "cloudbuild",
    "compute",
    "iam"
  ]
}