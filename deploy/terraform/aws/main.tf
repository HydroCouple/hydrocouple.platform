# The AWS side of a Kubernetes target on EKS: the store (an S3 bucket and prefix) and two
# IAM roles for IRSA, one for the agent and one for the runners. Each role can reach only
# what its pods need under the prefix. The cluster, its node groups and Kueue are the
# site's own; this module needs only the cluster's OIDC provider.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.70"
    }
  }
}

data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  bucket     = var.create_bucket ? aws_s3_bucket.store[0].id : var.bucket_name
  bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::${local.bucket}"
  objects    = "${local.bucket_arn}/${var.prefix}"
  accounts = {
    agent  = "hcp-agent"
    runner = "hcp-runner"
  }
}

resource "aws_s3_bucket" "store" {
  count  = var.create_bucket ? 1 : 0
  bucket = var.bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_public_access_block" "store" {
  count                   = var.create_bucket ? 1 : 0
  bucket                  = aws_s3_bucket.store[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "store" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.store[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "store" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.store[0].id
  rule {
    id     = "noncurrent-versions"
    status = "Enabled"
    filter {}
    # Deletion (plan §12) removes current objects; old versions expire after this.
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

data "aws_iam_policy_document" "trust" {
  for_each = local.accounts
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.namespace}:${each.value}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  for_each           = local.accounts
  name               = "${var.name}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.trust[each.key].json
  tags               = var.tags
}

# The agent: writes and presigns uploads of revisions, verifies and presigns downloads
# of results, and deletes runs (plan §12). Presigned URLs carry the agent's permissions.
data "aws_iam_policy_document" "agent" {
  statement {
    actions = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject",
    "s3:DeleteObjectVersion", "s3:AbortMultipartUpload"]
    resources = ["${local.objects}revisions/*", "${local.objects}runs/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:ListBucketVersions"]
    resources = [local.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.prefix}*"]
    }
  }
}

# The runners: read revisions, read and write their own outputs. Nothing else.
data "aws_iam_policy_document" "runner" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${local.objects}revisions/*"]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${local.objects}runs/*"]
  }
}

resource "aws_iam_role_policy" "agent" {
  name   = "store"
  role   = aws_iam_role.this["agent"].id
  policy = data.aws_iam_policy_document.agent.json
}

resource "aws_iam_role_policy" "runner" {
  name   = "store"
  role   = aws_iam_role.this["runner"].id
  policy = data.aws_iam_policy_document.runner.json
}
