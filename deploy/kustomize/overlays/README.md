# Overlays

Each overlay installs the agent for one kind of cluster. Copy the one that fits and replace every `REPLACE` value before applying it:

| Overlay | Cluster | Store | Workload identity |
| --- | --- | --- | --- |
| `eks` | Amazon EKS | S3 | IRSA (`eks.amazonaws.com/role-arn`), or EKS Pod Identity with the annotations removed |
| `aks` | Azure AKS | Blob Storage | Microsoft Entra Workload ID (`azure.workload.identity/*`) |
| `gke` | Google GKE | Cloud Storage | Workload Identity Federation for GKE (`iam.gke.io/gcp-service-account`) |
| `onprem` | k3s, RKE2, OpenShift or any other cluster | an S3-compatible store (MinIO, Ceph) | credentials from a Secret |

`deploy/terraform/<cloud>` creates the store, the two identities (agent and runner) and their permissions, and prints the values these overlays need.

## Onboarding

1. **Register the target** as a person with the organization's admin role. This prints the agent credential once, as a Secret:

   ```bash
   export HCP_TOKEN=...   # your sign-in token
   hcp target register --control-plane https://platform.example.org --org org_... \
       --name utility-eks --adapter kubernetes --storage-uri s3://bucket/hydrocouple \
       --kubernetes-secret hydrocouple/hcp-agent-credential > credential.json
   ```

2. **Install the agent**, then the credential:

   ```bash
   kubectl apply -k deploy/kustomize/overlays/eks
   kubectl apply -f credential.json && rm credential.json
   ```

3. **Verify** from inside the agent pod. This runs a runner pod from the signed image and writes to the store with the runner's own identity:

   ```bash
   kubectl -n hydrocouple exec deploy/hcp-agent -- \
       hcp agent --config /etc/hydrocouple/config/agent.json --verify --image <runner@sha256:...>
   ```

## Checks before applying

- The cluster enforces NetworkPolicy. On EKS this needs the VPC CNI's network policy agent (or Calico); on AKS, Azure, Calico or Cilium network policy; on GKE, Dataplane V2.
- `hcp-runner-storage` names the CIDRs of the store's private endpoint:
  - an S3 gateway endpoint's prefix list;
  - an Azure private endpoint's address;
  - `199.36.153.8/30` (private.googleapis.com) for Private Google Access.

  On a CNI with DNS-aware policies (Cilium), an FQDN rule can replace it.
- Runners also reach the endpoint where their workload identity gets its token:
  - on EKS, an STS interface endpoint's addresses, in the same `hcp-runner-storage` list;
  - on AKS, Microsoft Entra ID, in `entra-id.yaml`;
  - on GKE, the metadata server, which the overlay already allows.
- With Kueue (`components/kueue`), set the ClusterQueue's quota to what this target may use.
- The agent's identity can read the registry, as the nodes can. The cloud overlays set `k8s_keychain`, so cosign reads image signatures with that identity. `deploy/gate/<cloud>.md` shows the grant for each cloud.
