# The gate on Google GKE

This runbook prepares a GKE cluster, a Cloud Storage store and Artifact Registry for the gate, and ends with a filled-in overlay. The common steps are in [README.md](README.md).

```bash
export PROJECT=<your project> REGION=us-central1 CLUSTER=hcp-gate BUCKET=<a new bucket name>
export NETWORK=default SUBNET=default
gcloud config set project "$PROJECT"
```

## 1. The cluster

A Standard cluster, Kubernetes 1.31 or later, with two nodes of 4 vCPUs and 16 GiB. **Dataplane V2** enforces NetworkPolicy, and **Workload Identity Federation for GKE** gives the agent and runners their identities.

```bash
gcloud container clusters create "$CLUSTER" --region "$REGION" --num-nodes 1 \
    --node-locations "$REGION-a,$REGION-b" --machine-type e2-standard-4 \
    --network "$NETWORK" --subnetwork "$SUBNET" --enable-ip-alias \
    --enable-dataplane-v2 --workload-pool "$PROJECT.svc.id.goog"
gcloud container clusters get-credentials "$CLUSTER" --region "$REGION"
POD_RANGE=$(gcloud container clusters describe "$CLUSTER" --region "$REGION" \
    --format 'value(clusterIpv4Cidr)')
```

`--num-nodes 1` with two node locations makes two nodes.

## 2. The gate machine

A Compute Engine VM in the same network and region, with the tools in [README.md](README.md). Pods reach it from their pod addresses, so a firewall rule opens the gate's port to the pod range:

```bash
gcloud compute instances create hcp-gate --zone "$REGION-a" --machine-type e2-standard-4 \
    --network "$NETWORK" --subnet "$SUBNET" --image-family ubuntu-2204-lts \
    --image-project ubuntu-os-cloud --tags hcp-gate --scopes cloud-platform
gcloud compute firewall-rules create hcp-gate --network "$NETWORK" --direction INGRESS \
    --allow tcp:18080 --source-ranges "$POD_RANGE" --target-tags hcp-gate
```

On the gate machine, sign in with your account (`gcloud auth login`), set the same variables as above, install kubectl's GKE plugin, and get the cluster's credentials:

```bash
sudo apt-get install -y google-cloud-cli-gke-gcloud-auth-plugin kubectl
gcloud container clusters get-credentials "$CLUSTER" --region "$REGION"
kubectl get nodes
```

Run the Artifact Registry steps below on the gate machine too, so that Docker pushes with your account.
## 3. The store

**Private Google Access.** Runners reach Cloud Storage at `private.googleapis.com` (`199.36.153.8/30`), which NetworkPolicy can name. That needs Private Google Access on the subnet, and a private DNS zone that sends `*.googleapis.com` there. The zone applies to every VM and pod in the network.

```bash
gcloud compute networks subnets update "$SUBNET" --region "$REGION" --enable-private-ip-google-access
gcloud dns managed-zones create googleapis --dns-name googleapis.com. --visibility private \
    --networks "$NETWORK" --description "Private Google Access"
gcloud dns record-sets create private.googleapis.com. --zone googleapis --type A --ttl 300 \
    --rrdatas 199.36.153.8,199.36.153.9,199.36.153.10,199.36.153.11
gcloud dns record-sets create '*.googleapis.com.' --zone googleapis --type CNAME --ttl 300 \
    --rrdatas private.googleapis.com.
```

The network needs a route to `199.36.153.8/30` through the default internet gateway. The `default` network's `0.0.0.0/0` route is one.

**Bucket and identities.**

```bash
cd deploy/terraform/gcp
tofu init && tofu validate
tofu apply -var project_id="$PROJECT" -var bucket_name="$BUCKET" -var location="$REGION"
tofu output
cd -
```

## 4. The registry

```bash
gcloud artifacts repositories create hydrocouple --repository-format docker --location "$REGION"
REGISTRY=$REGION-docker.pkg.dev/$PROJECT/hydrocouple
gcloud auth configure-docker "$REGION-docker.pkg.dev"
```

**Pulls and signatures.** Nodes pull with the node pool's service account; the agent reads image signatures with its own. Both need `roles/artifactregistry.reader`:

```bash
NODE_SA=$(gcloud container node-pools describe default-pool --cluster "$CLUSTER" --region "$REGION" \
    --format 'value(config.serviceAccount)')
[ "$NODE_SA" = default ] && NODE_SA=$(gcloud iam service-accounts list \
    --filter 'displayName:Compute Engine default service account' --format 'value(email)')
for sa in "$NODE_SA" "$(cd deploy/terraform/gcp && tofu output -raw agent_service_account)"; do
  gcloud artifacts repositories add-iam-policy-binding hydrocouple --location "$REGION" \
      --member "serviceAccount:$sa" --role roles/artifactregistry.reader
done
```

Now build the images on the gate machine (README sections C and D) with this `REGISTRY`.

## 5. The overlay

```bash
cp -r deploy/kustomize/overlays/gke deploy/kustomize/overlays/site-gke
cp cosign.pub deploy/kustomize/overlays/site-gke/release.pub
```

**In `kustomization.yaml`:**
- the agent image: `newName` is `$REGISTRY/hydrocouple/agent`, and `digest` is the digest in `$AGENT`;
- the two service accounts: `tofu output agent_service_account` and `runner_service_account`.

`hcp-runner-storage` already names `199.36.153.8/30`.

**In `agent.json`:**
- `store`: `tofu output store_uri`;
- `kubernetes.max_cpu` and `max_memory_gib`: at most a node's size, here 4 and 16.

Leave `control_plane` as it is; the gate sets it.

Then continue with README section E.

## Cleanup

```bash
(cd deploy/terraform/gcp && tofu destroy -var project_id="$PROJECT" -var bucket_name="$BUCKET" \
    -var location="$REGION")
gcloud artifacts repositories delete hydrocouple --location "$REGION"
gcloud compute instances delete hcp-gate --zone "$REGION-a"
gcloud compute firewall-rules delete hcp-gate
gcloud container clusters delete "$CLUSTER" --region "$REGION"
```

The bucket is versioned and the module doesn't force its deletion, so empty it first (`gcloud storage rm --all-versions "gs://$BUCKET/**"`). Delete the `googleapis` DNS zone too, unless the network should keep using Private Google Access: first its two record sets, then `gcloud dns managed-zones delete googleapis`.
