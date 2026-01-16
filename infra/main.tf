resource "digitalocean_database_cluster" "postgres_cluster" {
  name       = var.db_cluster_name
  engine     = "pg"
  version    = var.pg_version
  size       = var.db_size
  region     = var.db_region
  node_count = 1
}

resource "digitalocean_database_db" "raw_db" {
  cluster_id = digitalocean_database_cluster.postgres_cluster.id
  name       = var.db_staging
}

resource "digitalocean_database_db" "processed_db" {
  cluster_id = digitalocean_database_cluster.postgres_cluster.id
  name       = var.db_dwh
}

resource "digitalocean_database_db" "airflow" {
  cluster_id = digitalocean_database_cluster.postgres_cluster.id
  name       = var.db_airflow
}

#S3-compatible Bucket
resource "digitalocean_spaces_bucket" "artifacts" {
  name   = "taxi-project-artifacts"
  region = var.bucket_region
}

