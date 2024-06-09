terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "databricks" {
}

data "databricks_current_user" "me" {
}

locals {
  repo_name = one(regex("/([-\\w]+$)", var.git_repo_url))
  git_branch = var.git_tag
}

resource "databricks_repo" "git_repo" {
  url    = var.git_repo_url
  branch = local.git_branch
  tag    = var.git_tag
}