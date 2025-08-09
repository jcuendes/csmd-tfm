###################
#### REQUIRED #####
###################

variable "project_id" {
  description = ") The ID of the project in which the resource belongs. If it is not provided, the provider project is used."
  type        = string
}

variable "region" {
  description = "The region to deploy resources in. This is used for regional resources."
  type = string
}

variable "cloud_build_trigger_name" {
  description = "The name of the Cloud Build trigger."
  type        = string
}

variable "cloud_build_trigger_filename" {
  description = "Path, from the source root, to a file whose contents is used for the template."
  type        = string
}

variable "cloud_build_trigger_repository_owner" {
  description = "The owner of the repository"
  type        = string
}

variable "cloud_build_trigger_repository_name" {
  description = "The name of the repository"
  type        = string
}

variable "cloud_build_service_account_email" {
  description = "The service account email"
  type        = string
}


###################
#### OPTIONAL #####
###################

variable "cloud_build_trigger_description" {
  description = "The description of the Cloud Build trigger."
  type        = string
  default     = "Construye la plantilla de Dataflow al hacer push a la rama main en GitHub."
}

variable "cloud_build_trigger_regex_branch" {
  description = "Regex branch"
  type        = string
  default     = "^main$"
}

variable "cloud_build_trigger_substitutions" {
  description = "Substitutions data for Build resource."
  type        = map(string)
  default     = {}
}

