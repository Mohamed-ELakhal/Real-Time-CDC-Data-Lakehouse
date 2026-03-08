# ============================================================
#  providers.tf — CDC Data Lakehouse
#
#  AWS credentials are resolved in priority order:
#    1. Environment variables  (recommended — no file changes needed)
#         export AWS_ACCESS_KEY_ID="AKIA..."
#         export AWS_SECRET_ACCESS_KEY="wJalr..."
#
#    2. Named profile via var.aws_profile  (set in terraform.tfvars)
#         aws configure --profile cdc-lakehouse
#         then set: aws_profile = "cdc-lakehouse"
#
#    3. Default profile  (~/.aws/credentials [default] section)
#         aws configure
#
#    4. EC2 / ECS instance role  (if Terraform itself runs on AWS)
#
#  Verify your credentials work before applying:
#    aws sts get-caller-identity
# ============================================================

terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # When aws_profile is an empty string the provider ignores it and falls back
  # to environment variables or the default credential chain.
  # Set aws_profile = "" in terraform.tfvars to use env-var credentials.
  profile = var.aws_profile != "" ? var.aws_profile : null
}
