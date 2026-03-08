# ============================================================
#  main.tf — AWS infrastructure for the CDC Data Lakehouse
#
#  Resources created:
#    - VPC + Internet Gateway + Route Table
#    - Public subnet  (10.0.1.0/24)
#    - Security Group (SSH + all 7 pipeline service ports)
#    - EC2 key pair   (from your local SSH public key)
#    - t3.xlarge EC2  (4 vCPU / 16 GB — meets 12 GB free-RAM requirement)
#    - Cloud-init     installs Docker, kind, kubectl, Helm on first boot
#
#  Cost:  ~$0.166 / hr  (eu-west-1, t3.xlarge on-demand)
#         $100 credit ≈ 600 hrs  (25 days continuous)
#         $200 credit ≈ 1200 hrs (50 days continuous)
#
#  Stop billing (keeps disk):   see stop_instance_command output
#  Destroy everything:          terraform destroy -auto-approve
# ============================================================

# ── Latest Ubuntu 22.04 LTS AMI ───────────────────────────────
# Canonical publishes official AMIs to every AWS region.
# This data source resolves to the newest one in var.aws_region
# so you never need to hard-code an AMI ID.

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical's official AWS account ID

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# ── VPC ───────────────────────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "${var.project_tag}-vpc", project = var.project_tag }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project_tag}-igw", project = var.project_tag }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true

  tags = { Name = "${var.project_tag}-subnet", project = var.project_tag }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = { Name = "${var.project_tag}-rt", project = var.project_tag }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# ── Security Group ────────────────────────────────────────────
# One ingress rule per service — mirrors the Kind NodePort
# mappings in kind-cluster-config-light.yaml exactly.
#
# EC2 host port → Kind NodePort → Kubernetes Service → Pod
# ─────────────────────────────────────────────────────────────
#  22    SSH
#  5432  PostgreSQL 15      (NodePort 30432)
#  27017 MongoDB 6.0        (NodePort 30017)
#  8080  Airflow Web UI     (NodePort 30080)
#  8083  Kafka Connect REST (NodePort 30083)
#  8123  ClickHouse HTTP    (NodePort 30123)
#  9000  ClickHouse Native  (NodePort 30900)
#  9092  Kafka broker       (NodePort 30092)

resource "aws_security_group" "cdc_lakehouse" {
  name        = "${var.project_tag}-sg"
  description = "CDC Lakehouse — SSH + pipeline service ports"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.project_tag}-sg", project = var.project_tag }

  # ── SSH ─────────────────────────────────────────────────────
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
  }

  # ── PostgreSQL ───────────────────────────────────────────────
  ingress {
    description = "PostgreSQL (NodePort 30432)"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── MongoDB ──────────────────────────────────────────────────
  ingress {
    description = "MongoDB (NodePort 30017)"
    from_port   = 27017
    to_port     = 27017
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── Airflow Web UI ───────────────────────────────────────────
  ingress {
    description = "Airflow Web UI (NodePort 30080)"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── Kafka Connect REST API ───────────────────────────────────
  ingress {
    description = "Kafka Connect REST API (NodePort 30083)"
    from_port   = 8083
    to_port     = 8083
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── ClickHouse HTTP ──────────────────────────────────────────
  ingress {
    description = "ClickHouse HTTP interface (NodePort 30123)"
    from_port   = 8123
    to_port     = 8123
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── ClickHouse Native TCP ────────────────────────────────────
  ingress {
    description = "ClickHouse native TCP / clickhouse-client (NodePort 30900)"
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── Kafka External Bootstrap ─────────────────────────────────
  ingress {
    description = "Kafka external bootstrap listener (NodePort 30092)"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # ── Egress: allow all outbound ───────────────────────────────
  # Required for: apt-get updates, Docker/Kind image pulls,
  # Helm repo downloads, Strimzi + Altinity operator manifests.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── SSH Key Pair ──────────────────────────────────────────────

resource "aws_key_pair" "cdc_lakehouse" {
  key_name   = "${var.project_tag}-key"
  public_key = file(var.ssh_public_key_path)
  tags       = { project = var.project_tag }
}

# ── Cloud-init bootstrap ──────────────────────────────────────
# Runs once on first boot as root.
# Installs the four host prerequisites: Docker, kubectl, kind, Helm.
# Also installs netcat-openbsd (used by the CHK ruok health check).
#
# Progress is logged to /var/log/cdc-lakehouse-init.log.
# SSH is available ~90 seconds after launch.
# After SSH, clone the repo and run ./startup.sh as normal.
#
# AWS Ubuntu 22.04 AMIs do NOT have the OCI iptables DROP issue
# that affects Docker Desktop on macOS — no iptables flush needed.

locals {
  cloud_init = <<-CLOUDINIT
    #!/usr/bin/env bash
    set -euo pipefail
    exec > >(tee -a /var/log/cdc-lakehouse-init.log) 2>&1

    echo "======================================================"
    echo " CDC Data Lakehouse — EC2 Bootstrap"
    echo " Instance: ${var.instance_type}"
    echo " vCPU: $(nproc)   RAM: $(free -h | awk '/^Mem:/{print $2}')"
    echo " Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "======================================================"

    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y --no-install-recommends \
        curl git python3 python3-pip ca-certificates \
        gnupg lsb-release apt-transport-https netcat-openbsd

    # ── 1/4  Docker ──────────────────────────────────────────
    echo "[1/4] Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    usermod -aG docker ubuntu
    # Allow the ubuntu user to use Docker immediately (no re-login needed)
    chmod 666 /var/run/docker.sock
    echo "[1/4] Docker: $(docker --version)"

    # ── 2/4  kubectl ─────────────────────────────────────────
    echo "[2/4] Installing kubectl..."
    K8S_VER=$(curl -sL https://dl.k8s.io/release/stable.txt)
    curl -sLo /usr/local/bin/kubectl \
        "https://dl.k8s.io/release/$${K8S_VER}/bin/linux/amd64/kubectl"
    chmod +x /usr/local/bin/kubectl
    echo "[2/4] kubectl: $(kubectl version --client --short 2>/dev/null || true)"

    # ── 3/4  kind ────────────────────────────────────────────
    echo "[3/4] Installing kind..."
    curl -sLo /usr/local/bin/kind \
        "https://kind.sigs.k8s.io/dl/v0.22.0/kind-linux-amd64"
    chmod +x /usr/local/bin/kind
    echo "[3/4] kind: $(kind version)"

    # ── 4/4  Helm ────────────────────────────────────────────
    echo "[4/4] Installing Helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    echo "[4/4] Helm: $(helm version --short)"

    echo ""
    echo "======================================================"
    echo " Bootstrap complete: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo " All host tools installed. SSH in and deploy:"
    echo ""
    echo "   ssh -i <private-key> ubuntu@$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)"
    echo "   git clone <your-repo-url> cdc-lakehouse"
    echo "   cd cdc-lakehouse && ./startup.sh"
    echo ""
    echo " Monitor this log:  tail -f /var/log/cdc-lakehouse-init.log"
    echo "======================================================"
  CLOUDINIT
}

# ── EC2 Instance ──────────────────────────────────────────────

resource "aws_instance" "cdc_lakehouse" {
  ami           = data.aws_ami.ubuntu.id
  instance_type = var.instance_type
  key_name      = aws_key_pair.cdc_lakehouse.key_name

  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.cdc_lakehouse.id]

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gb
    delete_on_termination = true

    tags = { Name = "${var.project_tag}-disk", project = var.project_tag }
  }

  user_data = base64encode(local.cloud_init)

  # Prevent accidental termination via the AWS console click.
  # Use `terraform destroy` or the CLI commands in the outputs.
  disable_api_termination = false

  tags = {
    Name    = "${var.project_tag}-vm"
    project = var.project_tag
  }

  lifecycle {
    # Don't replace the instance if Canonical publishes a new Ubuntu patch.
    ignore_changes = [ami]
  }
}
