###############################################################################
# WP6 — RTDB on AWS. One file, by ruling (WORKLOAD §4): no modules, no CDK.
#
# HARD RULES THIS FILE ENFORCES (WORKLOAD §0.7):
#   * region ap-south-1, the EXISTING VPC, read-only on everything already there;
#   * every resource carries Project=rtdb via provider default_tags — that tag is what makes a
#     resource ours to destroy, and nothing here modifies a resource that lacks it;
#   * no secret literal anywhere. Values come from `-var` at apply time and land in SSM; the state
#     file that records them is gitignored.
#
# The account may already run unrelated production workloads of its own. Nothing here reads,
# references or touches any of them — every resource is named `rtdb-*` and every parameter it can
# reach lives under /rtdb/*.
###############################################################################

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region

  # The single point that makes §0.7's ownership rule true rather than aspirational.
  default_tags {
    tags = {
      Project   = "rtdb"
      ManagedBy = "terraform"
    }
  }
}

###############################################################################
# Inputs
###############################################################################

variable "region" {
  type    = string
  default = "ap-south-1"
}

variable "vpc_id" {
  description = "The EXISTING VPC. Read, never modified."
  type        = string
}

variable "subdomain" {
  description = "The public endpoint."
  type        = string
}

# DNS lives entirely at the REGISTRAR and this configuration creates NO DNS resource at all.
#
# Delegating `rtdb` to a Route53 zone was the plan until the registrar's record editor turned out to
# offer no NS record type at all, so the delegation cannot be expressed. And the
# apex is a LIVE production domain serving a website and mail, so moving the whole zone to Route53
# would put those inside this
# work package. It stays where it is.
#
# What is left is two CNAMEs a human pastes once, and the honest cost of that is written down in
# here rather than hidden: an NLB REPLACEMENT changes its dns_name and needs the endpoint
# CNAME updated by hand. The ACM validation CNAME is stable for this domain+account, so renewals
# and re-issues need no second paste.

variable "db_password" {
  description = "RDS master password. Passed at apply time (-var), never defaulted, never committed."
  type        = string
  sensitive   = true
}

variable "jwt_secret" {
  description = "HS256 secret the app backend signs tokens with (§2 v1 auth). Apply-time only."
  type        = string
  sensitive   = true
}

variable "gateway_image_tag" {
  description = <<-EOT
    Image tag a NEWLY BOOTED gateway deploys. Routine deploys do not go through here — they run
    /opt/rtdb/rtdb-deploy.sh <sha> one gateway at a time (ruling Q2), because replacing both
    instances at once would take the fleet down. Bump this default at every gate anyway, so an
    instance replaced months later does not bootstrap a stale image.
  EOT
  type        = string
  default     = "f005a42"
}

variable "ops_image_tag" {
  description = <<-EOT
    Image tag a NEWLY BOOTED ops box deploys — rtdb-prometheus and rtdb-grafana, which are built and
    pushed on their own cadence and are NOT the gateway's sha.

    This was one variable shared with the gateways until WP7, and sharing it was a live fault: the
    gateway sha was bumped at every gate while the monitoring images kept a single immutable tag, so
    the variable named ops images that had never existed. A replaced ops box would have come up with
    no monitoring at all, and nothing in the plan would have said so. Two tags, two cadences.
  EOT
  type        = string
  default     = "b57382c"
}

# Deploy-review numbers (WORKLOAD §2). PROPOSED here; the formal ruling is Gate C's, which is why
# they are variables rather than literals buried in user-data.
variable "prune_ms" {
  type    = number
  default = 300000
}

variable "lock_ttl_ms" {
  type    = number
  default = 3000
}

variable "pg_pool" {
  type    = number
  default = 20
}

###############################################################################
# What is already there (read-only)
###############################################################################

data "aws_caller_identity" "me" {}

data "aws_vpc" "main" {
  id = var.vpc_id
}

data "aws_subnets" "vpc" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
}

# Amazon Linux 2023 on arm64 — t4g is Graviton, and so is the image the gateways run.
data "aws_ami" "al2023_arm64" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-arm64"]
  }
  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

locals {
  subnet_ids = data.aws_subnets.vpc.ids
  account    = data.aws_caller_identity.me.account_id
  registry   = "${data.aws_caller_identity.me.account_id}.dkr.ecr.${var.region}.amazonaws.com"
}

###############################################################################
# Security groups — the whole reachability matrix, and it is short on purpose
#
#   internet ---443---> NLB(sg rtdb-nlb) ---8080---> gateways(sg rtdb-gw)
#   ops(sg rtdb-ops) ---9090--> gateways        (Prometheus scrape; /metrics is never public)
#   gateways ---5432--> RDS(sg rtdb-db)
#   gateways ---6379--> ElastiCache(sg rtdb-cache)
#   nothing  ---22---> anything                 (there are no SSH rules; access is SSM only)
###############################################################################

resource "aws_security_group" "nlb" {
  name        = "rtdb-nlb"
  description = "RTDB public TLS listener"
  vpc_id      = var.vpc_id
  tags        = { Name = "rtdb-nlb" }
}

resource "aws_vpc_security_group_ingress_rule" "nlb_wss" {
  security_group_id = aws_security_group.nlb.id
  description       = "WSS from the internet (TLS terminates here)"
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 443
  to_port           = 443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "nlb_to_gw" {
  security_group_id            = aws_security_group.nlb.id
  description                  = "to the gateways"
  referenced_security_group_id = aws_security_group.gateway.id
  from_port                    = 8080
  to_port                      = 9090
  ip_protocol                  = "tcp"
}

resource "aws_security_group" "gateway" {
  name        = "rtdb-gw"
  description = "RTDB gateway instances"
  vpc_id      = var.vpc_id
  tags        = { Name = "rtdb-gw" }
}

resource "aws_vpc_security_group_ingress_rule" "gw_from_nlb" {
  security_group_id            = aws_security_group.gateway.id
  description                  = "WebSocket traffic, from the load balancer only"
  referenced_security_group_id = aws_security_group.nlb.id
  from_port                    = 8080
  to_port                      = 8080
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "gw_health_from_nlb" {
  security_group_id            = aws_security_group.gateway.id
  description                  = "/healthz: the target group check, on the admin port"
  referenced_security_group_id = aws_security_group.nlb.id
  from_port                    = 9090
  to_port                      = 9090
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "gw_metrics_from_ops" {
  security_group_id            = aws_security_group.gateway.id
  description                  = "/metrics: the ops box scrapes, nothing else may"
  referenced_security_group_id = aws_security_group.ops.id
  from_port                    = 9090
  to_port                      = 9090
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "gw_out" {
  security_group_id = aws_security_group.gateway.id
  description       = "ECR, SSM, and the data stores"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "ops" {
  name        = "rtdb-ops"
  description = "RTDB Prometheus + Grafana"
  vpc_id      = var.vpc_id
  tags        = { Name = "rtdb-ops" }
}

# No ingress rule at all, deliberately: Grafana is reached with `aws ssm start-session
# --document-name AWS-StartPortForwardingSession`, so there is nothing to open and nothing to leak.
resource "aws_vpc_security_group_egress_rule" "ops_out" {
  security_group_id = aws_security_group.ops.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "db" {
  name        = "rtdb-db"
  description = "RTDB RDS Postgres"
  vpc_id      = var.vpc_id
  tags        = { Name = "rtdb-db" }
}

resource "aws_vpc_security_group_ingress_rule" "db_from_gw" {
  security_group_id            = aws_security_group.db.id
  referenced_security_group_id = aws_security_group.gateway.id
  from_port                    = 5432
  to_port                      = 5432
  ip_protocol                  = "tcp"
}

resource "aws_security_group" "cache" {
  name        = "rtdb-cache"
  description = "RTDB ElastiCache Redis"
  vpc_id      = var.vpc_id
  tags        = { Name = "rtdb-cache" }
}

resource "aws_vpc_security_group_ingress_rule" "cache_from_gw" {
  security_group_id            = aws_security_group.cache.id
  referenced_security_group_id = aws_security_group.gateway.id
  from_port                    = 6379
  to_port                      = 6379
  ip_protocol                  = "tcp"
}

###############################################################################
# Data stores
###############################################################################

resource "aws_db_subnet_group" "main" {
  name       = "rtdb"
  subnet_ids = local.subnet_ids
}

resource "aws_db_instance" "main" {
  identifier     = "rtdb"
  engine         = "postgres"
  engine_version = "16"
  instance_class = "db.t4g.medium"

  db_name  = "rtdb"
  username = "rtdb"
  password = var.db_password

  allocated_storage     = 50
  max_allocated_storage = 200
  storage_type          = "gp3"
  storage_encrypted     = true

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.db.id]
  # The subnets are the VPC's existing public ones (there are no private ones), so "private" here is
  # this flag plus the security group: no public address is assigned and nothing outside the VPC can
  # open a socket to it. Stated plainly because §2 says "private" and this is what that means here.
  publicly_accessible = false

  # §8: "PITR on Postgres, restore drill before first production migration." Both halves are
  # non-negotiable; the drill is re-run against THIS instance at Gate C.
  backup_retention_period = 7
  backup_window           = "18:30-19:00" # 00:00-00:30 IST — the trough
  copy_tags_to_snapshot   = true

  auto_minor_version_upgrade = true
  maintenance_window         = "sun:19:30-sun:20:30"
  deletion_protection        = false # iterate by destroy/recreate (WORKLOAD §5)
  skip_final_snapshot        = true
  apply_immediately          = true

  performance_insights_enabled = true
  tags                         = { Name = "rtdb" }
}

resource "aws_elasticache_subnet_group" "main" {
  name       = "rtdb"
  subnet_ids = local.subnet_ids
}

# Cluster mode disabled, one node: §8 uses Redis as a bus, not as a store. Everything on it is
# reconstructible from the oplog — a lost stream is the trim case `readOplogSince` already covers,
# and WP5's chaos suite kills this exact thing on purpose. A replica would buy availability the
# design does not need and does not use.
resource "aws_elasticache_cluster" "main" {
  cluster_id           = "rtdb"
  engine               = "redis"
  engine_version       = "7.1"
  node_type            = "cache.t4g.small"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.main.name
  security_group_ids   = [aws_security_group.cache.id]

  maintenance_window = "sun:20:30-sun:21:30"
  tags               = { Name = "rtdb" }
}

###############################################################################
# Secrets — SSM Parameter Store, read by instance role. Never in a log, image or commit (§0.7).
###############################################################################

# Terraform CREATES this and then lets go of the value. Rotating a signing key is an operational
# act — it happens out of band, at an incident's pace, not at a plan's — and on 2026-08-29 a rotation
# done with put-parameter read as drift and was silently reverted by an unrelated apply that was only
# meant to add two load clients. The fleet and SSM then disagreed about the signing key, and the next
# auth-server cache refresh would have broken every new sign-in. Without this the same landmine sits
# under every future apply.
#
# The cost is that `var.jwt_secret` only matters on first create; after that the parameter is the
# truth. Rotate with put-parameter (or scripts/console-admin-set.ts's sibling path), then redeploy.
resource "aws_ssm_parameter" "jwt_secret" {
  name  = "/rtdb/prod/jwt_secret"
  type  = "SecureString"
  value = var.jwt_secret

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "db_url" {
  name = "/rtdb/prod/db_url"
  type = "SecureString"
  # sslmode=verify-full, not `require`: RDS refuses plaintext (rds.force_ssl), and `require` would
  # encrypt without checking who answered. The CA path is inside the gateway image, which ships
  # Amazon's RDS bundle. This connection carries the database password and every byte of user data.
  value = "postgres://rtdb:${urlencode(var.db_password)}@${aws_db_instance.main.address}:${aws_db_instance.main.port}/rtdb?sslmode=verify-full&sslrootcert=/etc/ssl/rds/global-bundle.pem"
}

resource "aws_ssm_parameter" "redis_url" {
  name = "/rtdb/prod/redis_url"
  type = "String"
  # Not a secret (no auth token on a VPC-private cache), but it belongs with its siblings so the
  # instances have exactly one place to look.
  value = "redis://${aws_elasticache_cluster.main.cache_nodes[0].address}:${aws_elasticache_cluster.main.port}"
}

###############################################################################
# Images
###############################################################################

resource "aws_ecr_repository" "gateway" {
  name                 = "rtdb-gateway"
  image_tag_mutability = "IMMUTABLE" # a tag is a rollback target; it must mean one image forever
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "prometheus" {
  name                 = "rtdb-prometheus"
  image_tag_mutability = "IMMUTABLE"
}

resource "aws_ecr_repository" "grafana" {
  name                 = "rtdb-grafana"
  image_tag_mutability = "IMMUTABLE"
}

# Gate D's load rig. A repository costs nothing until something is pushed to it.
# §5.13: an ARTIFACT repository, not a service. The console is extracted from this image and run
# natively; the container is never started. See tools/console/Dockerfile for why ECR and not
# user_data (16 KB cap) or Parameter Store (4/8 KB cap) — the files are 124 KB.
resource "aws_ecr_repository" "console" {
  name                 = "rtdb-console"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "loadsim" {
  name                 = "rtdb-loadsim"
  image_tag_mutability = "IMMUTABLE"
}

# Keep the last 10 images per repo. Rollback needs history; it does not need all of it.
resource "aws_ecr_lifecycle_policy" "gateway" {
  repository = aws_ecr_repository.gateway.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the last 10"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}

###############################################################################
# Instance roles
###############################################################################

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "gateway" {
  name               = "rtdb-gateway"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

# SSM Session Manager: this is why no instance has an SSH rule and no key pair exists.
resource "aws_iam_role_policy_attachment" "gateway_ssm" {
  role       = aws_iam_role.gateway.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "gateway_ecr" {
  role       = aws_iam_role.gateway.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# Scoped to /rtdb/* — an account will often hold other projects' parameters alongside these, and
# this role must not be able to read them.
data "aws_iam_policy_document" "gateway_params" {
  statement {
    actions   = ["ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath"]
    resources = ["arn:aws:ssm:${var.region}:${local.account}:parameter/rtdb/*"]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "gateway_params" {
  name   = "rtdb-gateway-params"
  role   = aws_iam_role.gateway.id
  policy = data.aws_iam_policy_document.gateway_params.json
}

resource "aws_iam_instance_profile" "gateway" {
  name = "rtdb-gateway"
  role = aws_iam_role.gateway.name
}

resource "aws_iam_role" "ops" {
  name               = "rtdb-ops"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "ops_ssm" {
  role       = aws_iam_role.ops.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "ops_ecr" {
  role       = aws_iam_role.ops.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# Gate A ruling Q4: Prometheus finds the gateways with ec2_sd_configs on tag Project=rtdb, so an
# instance replacement needs no image rebuild and no config edit. DescribeInstances cannot be scoped
# to a tag by resource ARN — the filtering happens in the Prometheus config, not in IAM.
data "aws_iam_policy_document" "ops_discovery" {
  statement {
    actions   = ["ec2:DescribeInstances", "ec2:DescribeAvailabilityZones"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "ops_discovery" {
  name   = "rtdb-ops-discovery"
  role   = aws_iam_role.ops.id
  policy = data.aws_iam_policy_document.ops_discovery.json
}

resource "aws_iam_instance_profile" "ops" {
  name = "rtdb-ops"
  role = aws_iam_role.ops.name
}

###############################################################################
# Instances
#
# Two gateways, each sized to carry 100% of the load (§8): on a death the NLB moves everyone to the
# survivor immediately, and a restart IS a thundering herd. Fixed at two — no autoscaling group, by
# ruling (§2 "OUT of scope").
###############################################################################

# §5.13, and deliberately NOT `ops_image_tag`: b57382c split the ops tag off the gateway's for the
# reason that a shared tag makes one component's deploy another component's surprise. The console
# and the monitoring stack are released independently, so they version independently.
variable "console_image_tag" {
  type        = string
  description = "rtdb-console artifact tag the ops box extracts the console from."
  default     = "f005a42"
}

locals {
  gateway_user_data = templatefile("${path.module}/user-data/gateway.sh", {
    region      = var.region
    registry    = local.registry
    image_tag   = var.gateway_image_tag
    prune_ms    = var.prune_ms
    lock_ttl_ms = var.lock_ttl_ms
    pg_pool     = var.pg_pool
    # One source of truth: the same script the runbook tells you to run for a deploy or a rollback
    # is the one that performs the first deploy at boot.
    deploy_script = file("${path.module}/user-data/rtdb-deploy.sh")
  })

  ops_user_data = templatefile("${path.module}/user-data/ops.sh", {
    region        = var.region
    registry      = local.registry
    ops_image_tag = var.ops_image_tag
    # §5.13: the hardened unit is 816 B and already committed at deploy/console.service, so it fits
    # the user_data budget comfortably. Only the 124 KB of console FILES has to be fetched.
    console_image_tag = var.console_image_tag
    console_unit      = file("${path.module}/console.service")
    # Same one-source-of-truth rule as the gateway: the script the runbook tells you to run for an
    # ops deploy is the script that performs the first one at boot.
    deploy_script = file("${path.module}/user-data/rtdb-ops-deploy.sh")
  })
}

# Fixed at 2 by design (§2: no autoscaling). `gateway_count` exists so Gate D could measure whether
# fanout scales horizontally — the answer decides Phase 7 sizing — and so a third can be added
# deliberately, not automatically.
variable "gateway_count" {
  type    = number
  default = 2
}

# Which subnet a THIRD-or-later gateway lands in. 0 = ap-south-1b, 1 = 1c, 2 = 1a.
variable "extra_gateway_subnet" {
  type    = number
  default = 0
}

resource "aws_instance" "gateway" {
  count = var.gateway_count

  ami           = data.aws_ami.al2023_arm64.id
  instance_type = "t4g.medium"
  # Gateways 1 and 2 keep their subnets exactly (index 0 and 1), so this expression never forces a
  # replacement of a serving instance. Any gateway BEYOND the designed pair is a temporary
  # measurement box, and `extra_gateway_subnet` exists because RunInstances stalled twice — never
  # returning, never erroring — for an instance in ap-south-1a while identical shapes launched in
  # 1b and 1c minutes apart. §8 puts no AZ constraint on a box that exists for one afternoon.
  subnet_id              = count.index < 2 ? local.subnet_ids[count.index] : local.subnet_ids[var.extra_gateway_subnet]
  vpc_security_group_ids = [aws_security_group.gateway.id]
  iam_instance_profile   = aws_iam_instance_profile.gateway.name
  user_data              = local.gateway_user_data
  # The VPC has only public subnets. A public address is how these reach ECR and SSM without a NAT
  # gateway ($0 vs ~$40/month); the security group above is the boundary, and it opens nothing to
  # the internet. There is no SSH rule and no key pair — access is Session Manager only.
  associate_public_ip_address = true

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "rtdb-gw-${count.index + 1}"
    Role = "gateway" # what Prometheus' ec2_sd_configs filters on
  }
}

resource "aws_instance" "ops" {
  ami                         = data.aws_ami.al2023_arm64.id
  instance_type               = "t4g.small"
  subnet_id                   = local.subnet_ids[0]
  vpc_security_group_ids      = [aws_security_group.ops.id]
  iam_instance_profile        = aws_iam_instance_profile.ops.name
  user_data                   = local.ops_user_data
  # §5.13: a user_data change must REPLACE this box, not update the attribute in place.
  #
  # The provider default is false, and false is silently useless here: user_data executes at FIRST
  # BOOT only, so an in-place update rewrites state, reports success, and runs nothing. That is how
  # a console-bootstrap apply came to be a no-op — the plan said "updated in-place" and would have
  # installed nothing while reporting Apply complete.
  #
  # True ties the box to its declared bootstrap: change what boots it and the box is rebuilt from
  # it. The cost is honest and deliberate — ANY commit that touches ops.sh, rtdb-ops-deploy.sh or
  # console.service now replaces this instance on the next apply, whatever that apply was for. Read
  # the plan before applying; that is true of every apply here, and doubly so of this one.
  #
  # Deliberately NOT set on the gateways: replacing those is a fleet event and stays a decision,
  # not a side effect of editing a template. They move by rtdb-deploy.sh, one at a time (RUNBOOK §3).
  user_data_replace_on_change = true
  associate_public_ip_address = true

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 30 # Prometheus TSDB
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "rtdb-ops"
    Role = "ops"
  }
}

###############################################################################
# Gate D load clients — SPOT, and OFF by default.
#
# `load_clients` is 0 so that a routine `terraform apply` can never create them: they are the only
# billable thing in this configuration that is meant to exist for an afternoon rather than for the
# life of the system. Gate D runs `-var load_clients=2` with the user's yes and terminates them the
# same day (§2 ruling).
###############################################################################

variable "load_clients" {
  description = "Number of spot load-sim clients. 0 except during a load pass."
  type        = number
  default     = 0
}

resource "aws_instance" "load_client" {
  count = var.load_clients

  ami           = data.aws_ami.al2023_arm64.id
  instance_type = "c7g.large"
  # Spread across AZs so one AZ's spot capacity cannot take the whole rig, and so the measurement
  # is not accidentally single-AZ.
  subnet_id                   = local.subnet_ids[count.index % length(local.subnet_ids)]
  vpc_security_group_ids      = [aws_security_group.load_client.id]
  iam_instance_profile        = aws_iam_instance_profile.gateway.name
  associate_public_ip_address = true

  instance_market_options {
    market_type = "spot"
    spot_options {
      # `terminate` + one-time: these are cattle for an afternoon. A stopped spot instance still
      # bills for its EBS and is exactly the leftover §5 forbids.
      instance_interruption_behavior = "terminate"
      spot_instance_type             = "one-time"
    }
  }

  user_data = templatefile("${path.module}/user-data/load-client.sh", {
    region   = var.region
    registry = local.registry
  })

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "rtdb-load-${count.index + 1}"
    Role = "loadclient" # NOT `gateway`, so Prometheus' ec2_sd never scrapes these
  }
}

# Outbound only: these dial the public endpoint and pull from ECR. Nothing ever connects TO them.
resource "aws_security_group" "load_client" {
  name        = "rtdb-load"
  description = "RTDB Gate D load clients"
  vpc_id      = var.vpc_id
  tags        = { Name = "rtdb-load" }
}

resource "aws_vpc_security_group_egress_rule" "load_client_out" {
  security_group_id = aws_security_group.load_client.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

###############################################################################
# The public endpoint: NLB + ACM + TLS, from day one (§4 ruling)
###############################################################################

variable "console_subdomain" {
  description = "Host for the console. Its own certificate in us-east-1, fronted by CloudFront."
  type        = string
}

# Unchanged, and deliberately so: CloudFront reaches the origin AS `var.subdomain`, which this
# certificate already covers, so the console needs no SAN and this certificate is never replaced.
resource "aws_acm_certificate" "endpoint" {
  domain_name       = var.subdomain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

# Waits until ACM reports the certificate ISSUED. No `validation_record_fqdns`: Terraform does not
# own the record, a human pastes it at the REGISTRAR, and this resource's job is only to stop the TLS
# listener being created against a certificate that is still pending — which would surface at Gate C
# as a TLS error rather than here as a failed apply.
#
# It BLOCKS while the paste is outstanding, which is why the apply is staged: request the
# certificate, print the CNAME, wait for the human, then run the rest.
resource "aws_acm_certificate_validation" "endpoint" {
  certificate_arn = aws_acm_certificate.endpoint.arn

  timeouts {
    create = "20m" # long enough for a paste plus propagation, short enough to fail a forgotten one
  }
}

resource "aws_lb" "main" {
  name                             = "rtdb"
  load_balancer_type               = "network"
  internal                         = false
  subnets                          = local.subnet_ids
  security_groups                  = [aws_security_group.nlb.id]
  enable_cross_zone_load_balancing = true
  tags                             = { Name = "rtdb" }
}

resource "aws_lb_target_group" "gateway" {
  name        = "rtdb-gw"
  port        = 8080
  protocol    = "TCP"
  vpc_id      = var.vpc_id
  target_type = "instance"

  # Client IP preservation OFF, so traffic reaches the instances from the NLB's own addresses and
  # the gateway security group can reference the NLB's group instead of opening 8080 to the world.
  preserve_client_ip = false

  # §2: /healthz is the target-group check, and it is a REAL check — it reaches storage on every
  # call, so a gateway that cannot see RDS is drained instead of being handed connections.
  health_check {
    protocol            = "HTTP"
    port                = "9090"
    path                = "/healthz"
    interval            = 10
    timeout             = 6
    healthy_threshold   = 2
    unhealthy_threshold = 2
    matcher             = "200"
  }

  # §8's ops note: a gateway death moves everyone to the survivor immediately. 30s is long enough
  # for in-flight frames to settle and short enough that a rolling deploy is not a coffee break.
  deregistration_delay = 30

  stickiness {
    type    = "source_ip"
    enabled = false
  }
}

resource "aws_lb_target_group_attachment" "gateway" {
  count            = length(aws_instance.gateway)
  target_group_arn = aws_lb_target_group.gateway.arn
  target_id        = aws_instance.gateway[count.index].id
  port             = 8080
}

resource "aws_lb_listener" "tls" {
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "TLS"
  certificate_arn   = aws_acm_certificate_validation.endpoint.certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.gateway.arn
  }
}

###############################################################################
# The console's front door: NLB :8443 -> auth-server on the ops box.
#
# It rides the EXISTING NLB rather than an ALB, which is why this costs nothing: a second listener
# on a load balancer already paid for. The price is the port in the URL, which the user chose over
# ~$18/month for a clean one.
###############################################################################

variable "console_port" {
  description = "Port auth-server.mjs listens on, on the ops box."
  type        = number
  default     = 8080
}

resource "aws_lb_target_group" "console" {
  name        = "rtdb-console"
  port        = var.console_port
  protocol    = "TCP"
  vpc_id      = var.vpc_id
  target_type = "instance"

  preserve_client_ip = false

  # The service's own /healthz. It answers only when it can serve the page.
  health_check {
    protocol            = "HTTP"
    port                = tostring(var.console_port)
    path                = "/healthz"
    interval            = 30
    timeout             = 6
    healthy_threshold   = 2
    unhealthy_threshold = 2
    matcher             = "200"
  }
}

resource "aws_lb_target_group_attachment" "console" {
  target_group_arn = aws_lb_target_group.console.arn
  target_id        = aws_instance.ops.id
  port             = var.console_port
}

resource "aws_lb_listener" "console" {
  load_balancer_arn = aws_lb.main.arn
  port              = 8443
  protocol          = "TLS"
  certificate_arn   = aws_acm_certificate_validation.endpoint.certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.console.arn
  }
}

###############################################################################
# CloudFront — a clean https URL for the console, with no port and no third party
#
# A tempting alternative is an existing host that is a CNAME to a third-party platform,
# which means that platform terminates its TLS. We are not doing that here: this service holds the
# production JWT secret, and the keys to the database do not go to a third party. CloudFront is the
# same clean result with AWS terminating instead — no fixed monthly fee, pennies at admin traffic.
###############################################################################

# CloudFront viewer certificates MUST live in us-east-1, whatever region everything else is in.
provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"

  default_tags {
    tags = { Project = "rtdb" }
  }
}

resource "aws_acm_certificate" "console" {
  provider          = aws.us_east_1
  domain_name       = var.console_subdomain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate_validation" "console" {
  provider        = aws.us_east_1
  certificate_arn = aws_acm_certificate.console.arn
}

resource "aws_cloudfront_distribution" "console" {
  enabled         = true
  comment         = "rtdb console sign-in"
  aliases         = [var.console_subdomain]
  is_ipv6_enabled = true
  price_class     = "PriceClass_100"

  origin {
    origin_id = "nlb"
    # The NLB reached by a name its certificate actually covers. Pointing this at the raw
    # *.elb.amazonaws.com name would fail the origin TLS handshake — CloudFront validates the
    # origin's certificate against the origin domain, and no ACM cert covers an ELB hostname.
    domain_name = var.subdomain

    custom_origin_config {
      https_port             = 8443
      http_port              = 80
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    target_origin_id       = "nlb"
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods         = ["GET", "HEAD"]

    # A login page and a POST that mints a token. There is nothing here worth caching, and caching
    # any of it would be a bug rather than a saving.
    cache_policy_id = data.aws_cloudfront_cache_policy.disabled.id
    # Forwards everything EXCEPT Host, which must stay the origin's name so the origin TLS handshake
    # and this distribution's SNI agree.
    origin_request_policy_id = data.aws_cloudfront_origin_request_policy.all_except_host.id
  }

  restrictions {
    geo_restriction { restriction_type = "none" }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.console.certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

data "aws_cloudfront_cache_policy" "disabled" {
  name = "Managed-CachingDisabled"
}

data "aws_cloudfront_origin_request_policy" "all_except_host" {
  name = "Managed-AllViewerExceptHostHeader"
}

# The ops box gains exactly one inbound rule, and only from the load balancer. Grafana and
# Prometheus stay on 127.0.0.1 behind the SSM tunnel — this listener exposes auth-server alone.
resource "aws_vpc_security_group_ingress_rule" "ops_console_from_nlb" {
  security_group_id            = aws_security_group.ops.id
  description                  = "console sign-in page, from the load balancer only"
  referenced_security_group_id = aws_security_group.nlb.id
  from_port                    = var.console_port
  to_port                      = var.console_port
  ip_protocol                  = "tcp"
}

# Not 0.0.0.0/0. Only CloudFront's origin-facing ranges reach :8443, so the endpoint host on :8443
# is not a second, uglier front door to the same login page — the distribution is the only way in.
data "aws_ec2_managed_prefix_list" "cloudfront_origins" {
  name = "com.amazonaws.global.cloudfront.origin-facing"
}

resource "aws_vpc_security_group_ingress_rule" "nlb_console" {
  security_group_id = aws_security_group.nlb.id
  description       = "console TLS, from CloudFront only"
  prefix_list_id    = data.aws_ec2_managed_prefix_list.cloudfront_origins.id
  from_port         = 8443
  to_port           = 8443
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "nlb_to_ops" {
  security_group_id            = aws_security_group.nlb.id
  description                  = "to the console service"
  referenced_security_group_id = aws_security_group.ops.id
  from_port                    = var.console_port
  to_port                      = var.console_port
  ip_protocol                  = "tcp"
}

# The ops role may now read the console credential and the shard secret it mints tokens against —
# scoped to those two paths, not to /rtdb/* at large.
data "aws_iam_policy_document" "ops_console_params" {
  statement {
    actions = ["ssm:GetParameter", "ssm:GetParameters"]
    resources = [
      "arn:aws:ssm:${var.region}:${local.account}:parameter/rtdb/console/*",
      "arn:aws:ssm:${var.region}:${local.account}:parameter/rtdb/prod/jwt_secret",
    ]
  }
  # The console's owner adds and removes users from the page, so the service must WRITE exactly one
  # parameter: the user store. Not /rtdb/console/*, which would let it rewrite its own admin
  # credential and the shadow key — one ARN, and the read scope above is untouched.
  statement {
    actions   = ["ssm:PutParameter"]
    resources = ["arn:aws:ssm:${var.region}:${local.account}:parameter/rtdb/console/users"]
  }
  statement {
    # Encrypt/GenerateDataKey are the write half of the SecureString above; Decrypt was already here
    # for the read. KMS actions cannot be scoped to a parameter, so ViaService stays the boundary:
    # this role can use the key through SSM and through nothing else.
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["ssm.${var.region}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "ops_console_params" {
  name   = "rtdb-ops-console-params"
  role   = aws_iam_role.ops.id
  policy = data.aws_iam_policy_document.ops_console_params.json
}

###############################################################################
# Outputs — everything the runbook, the SDK and the human with the DNS console need
###############################################################################

output "endpoint" {
  value = "wss://${var.subdomain}"
}

output "nlb_dns_name" {
  value = aws_lb.main.dns_name
}

# MANUAL STEP 1 of 2, at the REGISTRAR: one CNAME, before the rest of the stack is applied.
# Stable for this domain+account, so renewals and re-issues never need a second paste.
output "dns_step_1_acm_validation_cname" {
  description = "Add at the REGISTRAR as a CNAME, then wait for ACM to report ISSUED. Stage 2 blocks until it does."
  value = [
    for o in aws_acm_certificate.endpoint.domain_validation_options : {
      type  = o.resource_record_type
      name  = o.resource_record_name
      value = o.resource_record_value
    }
  ]
}

# MANUAL STEP 2 of 2, at the REGISTRAR: the endpoint itself, once the NLB exists.
# `rtdb` is not the apex, so a CNAME is legal here where an ALIAS is not available.
# This value changes if the NLB is ever REPLACED, and then it must be re-pasted by hand.
output "console_url" {
  value = "https://${var.console_subdomain}"
}

output "dns_step_2_endpoint_cname" {
  description = "Add at the REGISTRAR as a CNAME on `rtdb` pointing at this value."
  value       = { type = "CNAME", name = var.subdomain, value = aws_lb.main.dns_name }
}

# The console's name points at CloudFront, not at the load balancer.
output "dns_step_3_console_cname" {
  description = "Add at the REGISTRAR as a CNAME on `console` pointing at this value."
  value       = { type = "CNAME", name = var.console_subdomain, value = aws_cloudfront_distribution.console.domain_name }
}

# One validation record, in us-east-1, for the CloudFront viewer certificate.
output "dns_step_3a_console_cert_validation" {
  description = "Add at the REGISTRAR before the apply can finish: the console certificate's validation CNAME."
  value = [for o in aws_acm_certificate.console.domain_validation_options :
  { type = o.resource_record_type, name = o.resource_record_name, value = o.resource_record_value }]
}

output "gateway_instance_ids" {
  value = aws_instance.gateway[*].id
}

output "load_client_instance_ids" {
  value = aws_instance.load_client[*].id
}

output "gateway_private_ips" {
  value = aws_instance.gateway[*].private_ip
}

output "ops_instance_id" {
  value = aws_instance.ops.id
}

output "rds_endpoint" {
  value = aws_db_instance.main.address
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.main.cache_nodes[0].address
}

output "ecr_gateway_repo" {
  value = aws_ecr_repository.gateway.repository_url
}

output "grafana_port_forward" {
  description = "How to reach Grafana: no ingress rule exists, by design."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.ops.id} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"3000\"],\"localPortNumber\":[\"3000\"]}'"
}
