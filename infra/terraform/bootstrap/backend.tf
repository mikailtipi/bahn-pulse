# The bootstrap stack keeps its own state in the bucket it created.
terraform {
  backend "s3" {
    bucket       = "bahn-pulse-tfstate-530ea66e"
    key          = "bootstrap/terraform.tfstate"
    region       = "eu-central-1"
    encrypt      = true
    use_lockfile = true
  }
}
