variable "name" {
  description = "Globally unique bucket name."
  type        = string
}

variable "versioning" {
  description = "Keep old object versions (protects against accidental overwrite or delete)."
  type        = bool
  default     = true
}

variable "noncurrent_version_expiration_days" {
  description = "Days after which old (noncurrent) object versions are deleted."
  type        = number
  default     = 30
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket."
  type        = bool
  default     = false
}
