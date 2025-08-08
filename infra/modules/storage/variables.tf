###################
#### REQUIRED #####
###################

variable "project_id" {
  description = "Project ID"
  type        = string
}

variable "bucket_name" {
  description = "The name of the bucket"
  type        = string
}

variable "bucket_location" {
  description = "The GCS location. It can be Regional, global, etc."
  type        = string
}


###################
#### OPTIONAL #####
###################

variable "bucket_force_destroy" {
  description = "Default false. When deleting a bucket, this boolean option will delete all contained objects. If you try to delete a bucket that contains objects, Terraform will fail that run."
  type        = bool
  default     = true
}

variable "bucket_uniform_bucket_level_access" {
  description = "Default: true. Enables Uniform bucket-level access access to a bucket."
  type        = bool
  default     = true
}

variable "bucket_public_access_prevention" {
  description = "Prevents public access to a bucket. Acceptable values are inherited or enforced. If inherited, the bucket uses public access prevention. only if the bucket is subject to the public access prevention organization policy constraint. Defaults to inherited."
  type        = string
  default     = "inherited"
}

variable "bucket_class" {
  description = "Default: 'STANDARD'. The Storage Class of the new bucket. Supported values include: STANDARD, MULTI_REGIONAL, REGIONAL, NEARLINE, COLDLINE, ARCHIVE."
  type        = string
  default     = "STANDARD"
}

variable "bucket_with_soft_delete" {
  description = "Default: false. Enables Soft delete access to a bucket."
  type    = bool
  default = false
}

variable "bucket_retention_duration_seconds" {
  description = "Duration in seconds of the retention policy."
  type    = number
  default = 0
}

variable "bucket_with_lifecycle_rule" {
  description = "Default: false. Enables Lifecycle Rule access to a bucket."
  type    = bool
  default = false
}

variable "bucket_lifecycle_rule_action_type" {
  description = "A lifecycle rule action type"
  type    = string
  default = ""
}

variable "bucket_lifecycle_rule_age" {
  description = "Age in days"
  type    = number
  default = 0
}

variable "bucket_lifecycle_rule_matches_prefix" {
  description = "Regex expression to match objects"
  type    = list(string)
  default = []
}

variable "bucket_with_object" {
  description = "Default: false. Enables upload object to bucket"
  type        = bool
  default = false
}

variable "bucket_object_name" {
  description = "The name of the object"
  type        = string
  default     = ""
}

variable "bucket_object_source" {
  description = "The source of the object"
  type        = string
  default     = ""
}
