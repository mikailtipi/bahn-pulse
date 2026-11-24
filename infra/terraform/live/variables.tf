variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-central-1"
}

variable "poller_enabled" {
  description = "Switch the OpenBoards poller schedules on or off without destroying anything."
  type        = bool
  default     = true
}
