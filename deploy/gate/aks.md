# The gate on Azure AKS

This runbook prepares an AKS cluster, a Blob Storage store and ACR for the gate, and ends with a filled-in overlay. The common steps are in [README.md](README.md).

```bash
export RG=hcp-gate LOCATION=eastus CLUSTER=hcp-gate ACCOUNT=<a new storage account name> ACR=<a new registry name>
az group create -n "$RG" -l "$LOCATION"
```

## 1. The cluster

Kubernetes 1.31 or later, with two nodes of 4 vCPUs and 16 GiB. **Azure CNI powered by Cilium** enforces NetworkPolicy; the OIDC issuer and workload identity give the agent and runners their identities.

```bash
az aks create -g "$RG" -n "$CLUSTER" -l "$LOCATION" --node-count 2 --node-vm-size Standard_D4s_v5 \
    --network-plugin azure --network-plugin-mode overlay --pod-cidr 192.168.0.0/16 \
    --network-dataplane cilium --enable-oidc-issuer --enable-workload-identity --generate-ssh-keys
az aks get-credentials -g "$RG" -n "$CLUSTER"
ISSUER=$(az aks show -g "$RG" -n "$CLUSTER" --query oidcIssuerProfile.issuerUrl -o tsv)
NODE_RG=$(az aks show -g "$RG" -n "$CLUSTER" --query nodeResourceGroup -o tsv)
VNET_ID=$(az network vnet list -g "$NODE_RG" --query '[0].id' -o tsv)
VNET_NAME=$(az network vnet list -g "$NODE_RG" --query '[0].name' -o tsv)
```

With Cilium, a NetworkPolicy `ipBlock` can't select node or pod addresses. That doesn't affect the gate: the store's private endpoint is neither.

## 2. The gate machine

A VM in a new subnet of the cluster's virtual network, with the tools in [README.md](README.md). With overlay networking, pods reach addresses in the virtual network from their node's address, which the default `AllowVnetInBound` rule admits. No firewall rule is needed for the gate's port.

```bash
az network vnet subnet create -g "$NODE_RG" --vnet-name "$VNET_NAME" -n gate \
    --address-prefixes 10.225.0.0/24
GATE_SUBNET=$(az network vnet subnet show -g "$NODE_RG" --vnet-name "$VNET_NAME" -n gate --query id -o tsv)
az vm create -g "$RG" -n hcp-gate --image Ubuntu2204 --size Standard_D4s_v5 \
    --subnet "$GATE_SUBNET" --generate-ssh-keys
```

`10.225.0.0/24` lies inside the AKS-managed network's default `10.224.0.0/12` and outside its node subnet. Check with `az network vnet show --ids "$VNET_ID" --query addressSpace` first.

On the gate machine, sign in with your account (`az login --use-device-code`), set the same variables as above, and get the cluster's credentials:

```bash
az aks get-credentials -g "$RG" -n "$CLUSTER"
kubectl get nodes
```

## 3. The store

**Account, container and identities.** The module turns off public network access, so the store is reached through a private endpoint.

```bash
cd deploy/terraform/azure
tofu init && tofu validate
tofu apply -var resource_group_name="$RG" -var location="$LOCATION" \
    -var storage_account_name="$ACCOUNT" -var oidc_issuer_url="$ISSUER"
tofu output
cd -
```

**Private endpoint**, with the private DNS zone that makes `$ACCOUNT.blob.core.windows.net` resolve to it inside the network:

```bash
STORAGE_ID=$(az storage account show -n "$ACCOUNT" -g "$RG" --query id -o tsv)
az network private-endpoint create -g "$RG" -n hcp-blob -l "$LOCATION" --subnet "$GATE_SUBNET" \
    --private-connection-resource-id "$STORAGE_ID" --group-id blob --connection-name hcp-blob
az network private-dns zone create -g "$RG" -n privatelink.blob.core.windows.net
az network private-dns link vnet create -g "$RG" -n hcp-blob -z privatelink.blob.core.windows.net \
    -v "$VNET_ID" -e false
az network private-endpoint dns-zone-group create -g "$RG" --endpoint-name hcp-blob -n default \
    --private-dns-zone privatelink.blob.core.windows.net --zone-name blob
NIC=$(az network private-endpoint show -g "$RG" -n hcp-blob --query 'networkInterfaces[0].id' -o tsv)
az network nic show --ids "$NIC" --query 'ipConfigurations[0].privateIPAddress' -o tsv
```

The last command prints the endpoint's address, for `hcp-runner-storage`.

## 4. The registry

```bash
az acr create -g "$RG" -n "$ACR" --sku Standard
az aks update -g "$RG" -n "$CLUSTER" --attach-acr "$ACR"      # nodes pull the images
REGISTRY=$ACR.azurecr.io
az acr login -n "$ACR"
```

**Signatures.** The agent reads image signatures with its own identity:

```bash
az role assignment create --role AcrPull \
    --assignee "$(cd deploy/terraform/azure && tofu output -raw agent_client_id)" \
    --scope "$(az acr show -n "$ACR" --query id -o tsv)"
```

cosign reaches ACR through its Kubernetes keychain (`k8s_keychain` in `agent.json`), whose ACR helper uses the agent's workload identity.

Now build the images on the gate machine (README sections C and D) with this `REGISTRY`.

## 5. The overlay

```bash
cp -r deploy/kustomize/overlays/aks deploy/kustomize/overlays/site-aks
cp cosign.pub deploy/kustomize/overlays/site-aks/release.pub
```

**Microsoft Entra ID.** A runner's workload identity exchanges its token at `login.microsoftonline.com` before it can use the store. NetworkPolicy names it by the IPv4 prefixes of the `AzureActiveDirectory` service tag. This writes them into the overlay's `entra-id.yaml`:

```bash
az network list-service-tags --location "$LOCATION" \
    --query "values[?name=='AzureActiveDirectory'].properties.addressPrefixes[]" -o tsv \
  | grep -v : | python3 -c '
import sys
print("- op: add\n  path: /spec/egress/-\n  value:\n    to:")
for cidr in sys.stdin.read().split():
    print(f"      - ipBlock: {{cidr: {cidr}}}")
print("    ports:\n      - {protocol: TCP, port: 443}")' > deploy/kustomize/overlays/site-aks/entra-id.yaml
```

Microsoft changes the prefixes from time to time; a long-lived installation refreshes the file the same way.

**In `kustomization.yaml`:**
- the agent image: `newName` is `$REGISTRY/hydrocouple/agent`, and `digest` is the digest in `$AGENT`;
- the two client IDs: `tofu output agent_client_id` and `runner_client_id`;
- in `hcp-runner-storage`, the private endpoint's address as `<address>/32`.

**In `agent.json`:**
- `store`: `tofu output store_uri`;
- `kubernetes.max_cpu` and `max_memory_gib`: at most a node's size, here 4 and 16.

Leave `control_plane` as it is; the gate sets it.

Then continue with README section E.

## Cleanup

```bash
az group delete -n "$RG" --yes
```

This deletes the cluster, its node resource group, the store, the registry and the gate machine. Run `tofu destroy` first if you keep the resource group.
