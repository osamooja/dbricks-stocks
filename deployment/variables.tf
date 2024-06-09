variable "git_repo_url" {
  description = "GitHub repository url"
  type        = string
  default     = "https://github.com/osamooja/dbricks-stocks"
}

variable "git_tag" {
  description = "GitHub tag to deploy (e.g. v1.10.4)"
  type        = string
  default     = null
}

variable "github_token" {
  description = "GitHub personal access token"
  type        = string
  sensitive   = true
}