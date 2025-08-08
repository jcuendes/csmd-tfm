output "network" {
  description = "VPC Network object"
  value       = google_compute_network.custom-network
}

output "subnetwork" {
  description = "VPC Subnetwork object"
  value       = google_compute_subnetwork.custom-subnetwork
}

