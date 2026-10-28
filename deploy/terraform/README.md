# Cloud modules

OpenTofu (or Terraform 1.6+) modules for the cloud side of a Kubernetes target. Each creates the store and two workload identities, one for the agent and one for the runners, and prints the values the matching `deploy/kustomize/overlays/<cloud>` needs.

| Module | Store | Identities | Needs from the cluster |
| --- | --- | --- | --- |
| `aws` | S3 bucket and prefix, versioned, with public access blocked | IAM roles for IRSA | The OIDC provider ARN and URL |
| `azure` | Storage account (no shared keys) and container | User-assigned identities with federated credentials | The OIDC issuer URL |
| `gcp` | Cloud Storage bucket and prefix, versioned, with uniform access | Service accounts bound through Workload Identity Federation for GKE | Workload Identity enabled |

**Permissions.** The runners' identity can read revisions and write runs, and nothing else. The agent can also:
- write revisions;
- sign grants (presigned URLs are signed with the agent's own permissions);
- delete runs.

On Google Cloud the agent can also list the names of every object in the bucket, because erasure lists and an IAM condition can't limit a listing to a prefix. Azure scopes both identities to the container rather than the prefix. Add an ABAC condition on the role assignment to narrow the runners to `runs/` and `revisions/`.

**What the modules leave to the site.** The cluster, its node pools, Kueue, the private endpoint for the store and the NetworkPolicy-capable CNI. Use the endpoint's CIDRs in `hcp-runner-storage`.

```bash
cd deploy/terraform/aws
tofu init && tofu validate
tofu apply -var bucket_name=utility-hydrocouple \
           -var oidc_provider_arn=arn:aws:iam::123456789012:oidc-provider/oidc.eks... \
           -var oidc_provider_url=oidc.eks.us-east-1.amazonaws.com/id/...
```

These modules are format-checked in the repository. `tofu validate` needs the providers, so it runs where they can be downloaded.
