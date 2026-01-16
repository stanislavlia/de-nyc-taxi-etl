
variable "do_token" {
  description = "DigitalOcean API Token"
  type        = string
  sensitive   = true
}

variable "spaces_access_id" {
  description = "Spaces Access ID"
  type        = string
  sensitive   = true
}

variable "spaces_secret_key" {
  description = "Spaces secret key"
  type        = string
  sensitive   = true
}


variable "pg_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "17"
}

variable "db_cluster_name" {
  description = "Name of the PostgreSQL cluster"
  type        = string
  default     = "nyc-taxi-postgres-cluster"
}

variable "db_staging" {
  description = "Name of the raw data database"
  type        = string
  default     = "nyc_taxi_staging"
}

variable "db_dwh" {
  description = "Name of the processed data database"
  type        = string
  default     = "nyc_taxi_dwh"
}

variable "db_airflow" {
  description = "Name of the db for Airflow"
  type        = string
  default     = "airflow"
}


variable "db_size" {
  description = "Database cluster size"
  type        = string
  default     = "db-s-1vcpu-1gb"
}

variable "db_region" {
  description = "Database region"
  type        = string
  default     = "sgp1"
}

variable "bucket_region" {
  description = "DO Space Bucket region"
  type = string
  default = "tor1"
  
}