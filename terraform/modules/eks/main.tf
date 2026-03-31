###############################################################################
# modules/eks/main.tf
#
# Provisions an EKS cluster for the Flink-FASC project.
#
# Two managed node groups are created:
#   • flink-app          — hosts Flink JobManager and TaskManager pods
#   • fasc-coordinator   — hosts the lightweight FASC coordinator process
#
# IRSA (IAM Roles for Service Accounts) is enabled so that individual
# Kubernetes Service Accounts can assume scoped IAM roles without needing
# EC2 instance profile credentials.
###############################################################################

# ── EKS cluster ──────────────────────────────────────────────────────────────
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.29"

  # Networking
  vpc_id     = var.vpc_id
  subnet_ids = var.private_subnet_ids

  # Keep the API server endpoint inside the VPC; disable public endpoint so
  # that the control plane is never reachable from the internet.
  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = false

  # IRSA — creates an OpenID Connect provider for the cluster, which lets
  # service accounts assume IAM roles via federated identity.
  enable_irsa = true

  # ── Managed node groups ─────────────────────────────────────────────────
  eks_managed_node_groups = {

    # ── flink-app ──────────────────────────────────────────────────────────
    # m5.xlarge gives 4 vCPU / 16 GiB per node; Flink TaskManagers are
    # memory-hungry, so this balances cost and performance for production.
    flink-app = {
      instance_types = ["m5.xlarge"]

      min_size     = 2
      max_size     = 10
      desired_size = 4

      # Cluster autoscaler uses these labels as node selectors.
      labels = {
        role = "flink-app"
      }

      # Propagate common tags so cost-allocation reports include the project.
      tags = var.common_tags
    }

    # ── fasc-coordinator ──────────────────────────────────────────────────
    # The FASC coordinator is a Go/JVM microservice with modest resource
    # needs (leader election, DynamoDB polling, savepoint coordination).
    # t3.small is sufficient; 2 nodes for HA across AZs.
    fasc-coordinator = {
      instance_types = ["t3.small"]

      min_size     = 1
      max_size     = 2
      desired_size = 2

      labels = {
        role = "fasc-coordinator"
      }

      tags = var.common_tags
    }
  }

  # ── EKS managed add-ons ─────────────────────────────────────────────────
  # Pinning versions here avoids unintended upgrades during plan runs.
  # Update these when the cluster Kubernetes version is upgraded.
  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      # vpc-cni must use IRSA to manage ENIs on behalf of pods.
      most_recent    = true
      before_compute = true # install before nodes join so pods get IPs
    }
    aws-ebs-csi-driver = {
      # Required for PersistentVolumeClaims backed by EBS (e.g. RocksDB state
      # directories if local storage is preferred over S3 checkpoints).
      most_recent              = true
      service_account_role_arn = aws_iam_role.ebs_csi_driver.arn
    }
  }

  tags = var.common_tags
}

# ── IAM role for the EBS CSI driver add-on ───────────────────────────────────
# The aws-ebs-csi-driver add-on needs its own service-account-scoped role to
# create/attach EBS volumes.  The AmazonEBSCSIDriverPolicy is AWS-managed.

data "aws_iam_policy_document" "ebs_csi_driver_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:sub"
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
    }

    condition {
      test     = "StringEquals"
      variable = "${module.eks.oidc_provider}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ebs_csi_driver" {
  name               = "${var.cluster_name}-ebs-csi-driver"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_driver_assume.json

  tags = merge(var.common_tags, {
    Name = "${var.cluster_name}-ebs-csi-driver"
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi_driver" {
  role       = aws_iam_role.ebs_csi_driver.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
