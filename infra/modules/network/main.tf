resource "google_compute_network" "custom-network" {
  name                            = var.vpc_name
  project                         = var.project_id
  description                     = var.vpc_description
  auto_create_subnetworks         = var.vpc_auto_create_subnetworks
  routing_mode                    = var.vpc_routing_mode
  delete_default_routes_on_create = var.vpc_delete_default_routes_on_create
}

resource "google_compute_subnetwork" "custom-subnetwork" {
  network                    = google_compute_network.custom-network.self_link
  region                     = var.region
  project                    = var.project_id
  name                       = var.subnetwork_name
  ip_cidr_range              = var.network_ip_cidr_range_main
  description                = var.subnetwork_description
}