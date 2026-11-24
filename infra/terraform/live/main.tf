# Main stack for Bahn Pulse. Grows step by step (bronze first, then Glue/Athena, compute, ...).

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "bahn-pulse"
      Stack     = "live"
      ManagedBy = "terraform"
    }
  }
}

resource "random_id" "suffix" {
  byte_length = 4
}

# Bronze layer: raw API responses (JSON), immutable, append-only, hourly partitions.
module "bronze_bucket" {
  source = "../modules/s3_bucket"

  name                               = "bahn-pulse-bronze-${random_id.suffix.hex}"
  versioning                         = true
  noncurrent_version_expiration_days = 30
}
