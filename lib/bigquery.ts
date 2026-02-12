import { BigQuery } from '@google-cloud/bigquery';
import { ExternalAccountClient } from 'google-auth-library';
import { getVercelOidcToken } from '@vercel/oidc';

// Uses Vercel OIDC + Google Workload Identity Federation.
// No service account key file required — avoids iam.disableServiceAccountKeyCreation org policy.
// Each invocation exchanges a short-lived Vercel OIDC token for GCP credentials.
// No service_account_impersonation_url — the federated identity accesses BigQuery
// directly via the STS token. BigQuery roles are granted to the principal at project level.
const authClient = ExternalAccountClient.fromJSON({
  type: 'external_account',
  audience: `//iam.googleapis.com/projects/${process.env.GCP_PROJECT_NUMBER}/locations/global/workloadIdentityPools/${process.env.GCP_WORKLOAD_IDENTITY_POOL_ID}/providers/${process.env.GCP_WORKLOAD_IDENTITY_POOL_PROVIDER_ID}`,
  subject_token_type: 'urn:ietf:params:oauth:token-type:jwt',
  token_url: 'https://sts.googleapis.com/v1/token',
  subject_token_supplier: {
    getSubjectToken: () => getVercelOidcToken(),
  },
});

if (!authClient) {
  throw new Error('Failed to initialise ExternalAccountClient — check GCP_* env vars');
}

export const bigquery = new BigQuery({
  projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  authClient: authClient as any,
});

export const dataset = bigquery.dataset(process.env.BIGQUERY_DATASET_ID!);
