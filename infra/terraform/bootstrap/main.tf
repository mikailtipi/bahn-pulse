# Bootstrap: creates the S3 bucket that holds Terraform state for all stacks.
# Run once with local state, then migrate this stack's own state into the bucket
# (see backend.tf). Locking uses S3 native lock files, so no DynamoDB table.

terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "bahn-pulse"
      Stack     = "bootstrap"
      ManagedBy = "terraform"
    }
  }
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-central-1"
}

resource "random_id" "suffix" {
  byte_length = 4
}

module "state_bucket" {
  source = "../modules/s3_bucket"

  name                               = "bahn-pulse-tfstate-${random_id.suffix.hex}"
  versioning                         = true
  noncurrent_version_expiration_days = 90
}

output "state_bucket" {
  description = "Name of the Terraform state bucket (use it in backend blocks)."
  value       = module.state_bucket.id
}
