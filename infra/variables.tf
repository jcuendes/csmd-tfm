#####################################
##         Global Variables        ##
#####################################
variable "project_id" {
  description = "The ID of the Google Cloud project to use."
  type        = string
  default     = "csmd-tfm-jcuendes-test"
}

variable "region" {
  description = "The region to deploy resources in."
  type        = string
  default     = "europe-west1"
}

variable "zone" {
  description = "The zone to deploy resources in."
  type        = string
  default     = "europe-west1-b"
}