###################
#### REQUIRED #####
###################

variable "project_id" {
  description = "The ID of the project in which the resource belongs."
  type        = string
}

variable "sa_account_id" {
  description = "The account id that is used to generate the service account email address and a stable unique id."
  type        = string
}

variable "sa_display_name" {
  description = " The display name for the service account. Can be updated without creating a new resource."
  type        = string
}

###################
#### OPTIONAL #####
###################

variable "sa_description" {
  description = " A text description of the service account."
  type        = string
  default     = null
}

variable "sa_disabled" {
  description = "Whether a service account is disabled or not. Defaults to false. This field has no effect during creation. Must be set after creation to disable a service account."
  type        = bool
  default     = false
}

variable "sa_roles" {
  description = "List of roles that should be applied on the Service Account"
  type        = list(string)
  default     = []
}
