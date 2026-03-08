# ============================================================
#  variables.tf — CDC Data Lakehouse
#
#  All values have safe defaults. The only required variables
#  (no default) are ssh_public_key_path and ssh_private_key_path.
#  Copy terraform.tfvars.example → terraform.tfvars and fill those in.
# ============================================================

# ── AWS credentials ───────────────────────────────────────────
# Credentials are never defined here — see providers.tf for the
# three supported methods (env vars, named profile, default profile).

variable "aws_profile" {
  description = "AWS CLI named profile to use. Set to \"\" to use environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) instead."
  type        = string
  default     = "default"
}

variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "eu-west-1"

  # Latency reference:
  #   eu-west-1    Ireland     ~120 ms from EU
  #   eu-central-1 Frankfurt   ~130 ms from EU
  #   me-south-1   Bahrain     ~30 ms  from Gulf/SA  (closest, smaller pool)
  #   me-central-1 UAE         ~40 ms  from Gulf/SA
  #   us-east-1    N. Virginia (lowest global spot price)
}

# ── Instance ──────────────────────────────────────────────────
# The full pipeline requires ~9.4 GB RAM (hard limit) with 12 GB recommended.
# t3.xlarge (4 vCPU / 16 GB) is the minimum class that reliably delivers this.
#
# On-demand cost reference (eu-west-1):
#   t3.xlarge   $0.166 / hr  →  $100 credit ≈ 602 hrs (25 days 24/7)
#   t3a.xlarge  $0.150 / hr  →  AMD variant, ~10 % cheaper, identical specs
#   m6i.xlarge  $0.192 / hr  →  more consistent CPU burst
#
# Tip: stop the instance when not in use — credit stretches to months.

variable "instance_type" {
  description = "EC2 instance type. t3.xlarge is the minimum for this pipeline."
  type        = string
  default     = "t3.xlarge"
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size in GB. 100 GB recommended to cache container images."
  type        = number
  default     = 100
}

# ── SSH key ───────────────────────────────────────────────────

variable "ssh_public_key_path" {
  description = "Path to your SSH public key file (e.g. ~/.ssh/id_ed25519.pub or ~/.ssh/id_rsa.pub). Terraform uploads this to EC2 — the private key never leaves your machine."
  type        = string
}

variable "ssh_private_key_path" {
  description = "Path to your SSH private key. Used only to generate the ready-to-paste ssh_command output — Terraform does not connect via SSH itself."
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR allowed to reach port 22. Restrict to your IP for security: e.g. \"203.0.113.10/32\". Default 0.0.0.0/0 allows connections from any IP."
  type        = string
  default     = "0.0.0.0/0"
}

# ── Tagging ───────────────────────────────────────────────────

variable "project_tag" {
  description = "Tag applied to every AWS resource. Useful for cost tracking and bulk teardown."
  type        = string
  default     = "cdc-lakehouse"
}
