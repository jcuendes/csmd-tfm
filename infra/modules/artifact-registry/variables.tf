###################
#### REQUIRED #####
###################

variable "project_id" {
  description = ") The ID of the project in which the resource belongs. If it is not provided, the provider project is used."
  type        = string
}

variable "region" {
  description = "The name of the location this repository is located in."
  type        = string
}

variable "artifact_registry_naming" {
  description = "The last part of the repository name"
  type        = string
}


###################
#### OPTIONAL #####
###################

variable "artifact_registry_description" {
  description = "The description of the repository."
  type        = string
  default     = "Repositorio para las imágenes de las plantillas de Dataflow."
}

variable "artifact_format" {
  description = "The format of packages that are stored in the repository."
  type        = string
  default     = "DOCKER"
}
