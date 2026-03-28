terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 1.0.0"
    }
  }
}

provider "minio" {
  minio_server   = "minio:9000"
  minio_user     = "minio_admin"
  minio_password = "minio_password"
  minio_ssl      = false
}

resource "minio_s3_bucket" "data_lake" {
  bucket        = "movies-data-lake"
  acl           = "private"
}
