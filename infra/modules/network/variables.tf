###################
#### REQUIRED #####
###################

variable "project_id" {
  description = "The ID of the Google Cloud project to use."
  type = string
}

variable "region" {
  description = "The region to deploy resources in. This is used for regional resources."
  type = string
}

variable "vpc_name" {
  description = "Name of the vpc."
  type        = string
}

variable "subnetwork_name" {
  description = "Name of the subnetwork."
  type        = string
}

###################
#### OPTIONAL #####
###################

variable "vpc_description" {
  description = "An optional description of this resource. The resource must be recreated to modify this field."
  type        = string
  default     = "Main custom VPC"
}

variable "vpc_auto_create_subnetworks" {
  description = "When set to true, the network is created in auto subnet mode and it will create a subnet for each region automatically across the 10.128.0.0/9 address range. When set to false, the network is created in custom subnet mode so the user can explicitly connect subnetwork resources. Default false"
  type        = bool
  default     = false
}

variable "vpc_routing_mode" {
  description = "The network-wide routing mode to use. If set to REGIONAL, this network's cloud routers will only advertise routes with subnetworks of this network in the same region as the router. If set to GLOBAL, this network's cloud routers will advertise routes with all subnetworks of this network, across regions. Possible values are REGIONAL and GLOBAL."
  type        = string
  default     = "REGIONAL"
}

variable "vpc_delete_default_routes_on_create" {
  description = " If set to true, default routes (0.0.0.0/0) will be deleted immediately after network creation. Defaults to false."
  type        = bool
  default     = false
}

variable "network_ip_cidr_range_main" {
  description = "IP range for main subnet. Default value is 10.0.1.0/24"
  type        = string
  default     = "10.0.1.0/24"
}

variable "subnetwork_description" {
  description = "An optional description of this resource. The resource must be recreated to modify this field."
  type        = string
  default     = "Main custom subnetwork"
}
