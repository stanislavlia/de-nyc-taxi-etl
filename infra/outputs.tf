output "cluster_id" {
  description = "PostgreSQL cluster ID"
  value       = digitalocean_database_cluster.postgres_cluster.id
}

output "cluster_host" {
  description = "PostgreSQL cluster host"
  value       = digitalocean_database_cluster.postgres_cluster.host
}

output "cluster_port" {
  description = "PostgreSQL cluster port"
  value       = digitalocean_database_cluster.postgres_cluster.port
}

output "cluster_user" {
  description = "PostgreSQL cluster default user"
  value       = digitalocean_database_cluster.postgres_cluster.user
  sensitive   = true
}

output "cluster_password" {
  description = "PostgreSQL cluster password"
  value       = digitalocean_database_cluster.postgres_cluster.password
  sensitive   = true
}

output "raw_db_name" {
  description = "Raw database name"
  value       = digitalocean_database_db.raw_db.name
}

output "processed_db_name" {
  description = "Processed database name"
  value       = digitalocean_database_db.processed_db.name
}