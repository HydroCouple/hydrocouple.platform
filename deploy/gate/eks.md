# The gate on Amazon EKS

This runbook prepares an EKS cluster, an S3 store and ECR for the gate, and ends with a filled-in overlay. The common steps are in [README.md](README.md).

```bash
export AWS_REGION=us-east-1 CLUSTER=hcp-gate BUCKET=<a new bucket name>
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
```

## 1. The cluster

Kubernetes 1.31 or later, with two nodes of 4 vCPUs and 16 GiB (for example `m6i.xlarge`). With eksctl:

```bash
eksctl create cluster --name "$CLUSTER" --region "$AWS_REGION" --version 1.33 \
    --nodegroup-name gate --node-type m6i.xlarge --nodes 2 --node-private-networking --with-oidc
```

**NetworkPolicy.** EKS enforces it through the VPC CNI's network policy agent. It needs VPC CNI v1.14 or later as a managed add-on, and nodes with Linux kernel 5.10 or later:

```bash
aws eks update-addon --cluster-name "$CLUSTER" --addon-name vpc-cni \
    --resolve-conflicts PRESERVE --configuration-values '{"enableNetworkPolicy": "true"}'
```

**IRSA.** `--with-oidc` created the cluster's IAM OIDC provider. The store module needs its ARN and URL:

```bash
ISSUER=$(aws eks describe-cluster --name "$CLUSTER" --query cluster.identity.oidc.issuer --output text)
OIDC_URL=${ISSUER#https://}
OIDC_ARN=arn:aws:iam::$ACCOUNT:oidc-provider/$OIDC_URL
VPC=$(aws eks describe-cluster --name "$CLUSTER" --query cluster.resourcesVpcConfig.vpcId --output text)
CLUSTER_SG=$(aws eks describe-cluster --name "$CLUSTER" \
    --query cluster.resourcesVpcConfig.clusterSecurityGroupId --output text)
```

## 2. The gate machine

An EC2 instance in one of the cluster's private subnets, with the tools in [README.md](README.md). With the VPC CNI, pods use VPC addresses and their node's security groups, which for managed node groups include the cluster security group. So one rule opens the gate's port to them (or use `--cidr` with the node subnets' CIDRs):

```bash
GATE_SG=<the gate machine's security group>
aws ec2 authorize-security-group-ingress --group-id "$GATE_SG" \
    --protocol tcp --port 18080 --source-group "$CLUSTER_SG"
```

On the gate machine, sign in with the identity that created the cluster (`aws sso login`, or its keys), so that it has the cluster's admin access entry, and set the same variables as above. Then:

```bash
aws eks update-kubeconfig --name "$CLUSTER" --region "$AWS_REGION"
kubectl get nodes
```

## 3. The store

**S3 gateway endpoint.** Runners reach S3 through it, and NetworkPolicy names its addresses:

```bash
ROUTE_TABLES=$(aws ec2 describe-route-tables --filters Name=vpc-id,Values="$VPC" \
    --query 'RouteTables[].RouteTableId' --output text)
S3_ENDPOINT=$(aws ec2 create-vpc-endpoint --vpc-id "$VPC" --vpc-endpoint-type Gateway \
    --service-name "com.amazonaws.$AWS_REGION.s3" --route-table-ids $ROUTE_TABLES \
    --query VpcEndpoint.VpcEndpointId --output text)
PL=$(aws ec2 describe-managed-prefix-lists \
    --filters Name=prefix-list-name,Values="com.amazonaws.$AWS_REGION.s3" \
    --query 'PrefixLists[0].PrefixListId' --output text)
aws ec2 get-managed-prefix-list-entries --prefix-list-id "$PL" --query 'Entries[].Cidr' --output text
```

**STS interface endpoint.** With IRSA, a runner exchanges its token at STS before it can use S3. NetworkPolicy can name STS only through an interface endpoint's addresses. Its private DNS makes the regional STS name, which the IRSA webhook has pods use, resolve to them:

```bash
NODE_SUBNETS=$(aws eks describe-nodegroup --cluster-name "$CLUSTER" --nodegroup-name gate \
    --query 'nodegroup.subnets' --output text)
STS_ENDPOINT=$(aws ec2 create-vpc-endpoint --vpc-id "$VPC" --vpc-endpoint-type Interface \
    --service-name "com.amazonaws.$AWS_REGION.sts" --subnet-ids $NODE_SUBNETS \
    --security-group-ids "$CLUSTER_SG" --private-dns-enabled \
    --query VpcEndpoint.VpcEndpointId --output text)
ENIS=$(aws ec2 describe-vpc-endpoints --vpc-endpoint-ids "$STS_ENDPOINT" \
    --query 'VpcEndpoints[0].NetworkInterfaceIds' --output text)
aws ec2 describe-network-interfaces --network-interface-ids $ENIS \
    --query 'NetworkInterfaces[].PrivateIpAddress' --output text
```

The endpoint has one address in each node subnet. It takes a few minutes to become available, which `aws ec2 describe-vpc-endpoints --vpc-endpoint-ids "$STS_ENDPOINT"` shows. The cluster security group admits traffic from its own members, so the endpoint accepts the pods' connections.

**Bucket and identities.**

```bash
cd deploy/terraform/aws
tofu init && tofu validate
tofu apply -var bucket_name="$BUCKET" -var oidc_provider_arn="$OIDC_ARN" -var oidc_provider_url="$OIDC_URL"
tofu output
cd -
```

## 4. The registry

```bash
for r in base runner agent; do aws ecr create-repository --repository-name "hydrocouple/$r"; done
REGISTRY=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY"
```

**Pulls and signatures.** Nodes pull with their node role. eksctl's node groups include `AmazonEC2ContainerRegistryReadOnly`. The agent reads image signatures with its own role:

```bash
aws iam attach-role-policy --role-name hydrocouple-agent \
    --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
```

Now build the images on the gate machine (README sections C and D) with this `REGISTRY`.

## 5. The overlay

```bash
cp -r deploy/kustomize/overlays/eks deploy/kustomize/overlays/site-eks
cp cosign.pub deploy/kustomize/overlays/site-eks/release.pub
```

**In `kustomization.yaml`:**
- the agent image: `newName` is `$REGISTRY/hydrocouple/agent`, and `digest` is the digest in `$AGENT`;
- the two role ARNs: `tofu output agent_role_arn` and `runner_role_arn`;
- in `hcp-runner-storage`, one `ipBlock` for each CIDR of the prefix list, and one `<address>/32` for each address of the STS endpoint.

**In `agent.json`:**
- `store`: `tofu output store_uri`;
- `kubernetes.max_cpu` and `max_memory_gib`: at most a node's size, here 4 and 16.

Leave `control_plane` as it is; the gate sets it.

Then continue with README section E.

## Cleanup

The bucket is versioned and the module doesn't force its deletion, so empty it first (in the console, **Empty** removes every version). Then, in this order:

```bash
aws iam detach-role-policy --role-name hydrocouple-agent \
    --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
(cd deploy/terraform/aws && tofu destroy -var bucket_name="$BUCKET" \
    -var oidc_provider_arn="$OIDC_ARN" -var oidc_provider_url="$OIDC_URL")
for r in base runner agent; do aws ecr delete-repository --force --repository-name "hydrocouple/$r"; done
aws ec2 delete-vpc-endpoints --vpc-endpoint-ids "$S3_ENDPOINT" "$STS_ENDPOINT"
aws ec2 revoke-security-group-ingress --group-id "$GATE_SG" \
    --protocol tcp --port 18080 --source-group "$CLUSTER_SG"
aws ec2 terminate-instances --instance-ids <the gate machine>
eksctl delete cluster --name "$CLUSTER" --region "$AWS_REGION"
```

The role's hand-attached policy is detached first because `tofu destroy` can't delete a role that still has one. eksctl can delete the VPC only once the endpoints and the gate machine are gone.
