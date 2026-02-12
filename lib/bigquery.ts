import { BigQuery } from '@google-cloud/bigquery';
import { Readable } from 'stream';
import { getVercelOidcToken } from '@vercel/oidc';

// Manual Workload Identity Federation auth — bypasses google-auth-library
// ExternalAccountClient which doesn't correctly handle subject_token_supplier.
// Implements the same two-step flow confirmed working in the debug endpoint:
//   1. Exchange Vercel OIDC token for GCP STS access token
//   2. Use STS token to call generateAccessToken on the service account
//   3. Pass the resulting short-lived SA token to BigQuery

async function getServiceAccountToken(): Promise<string> {
  const projectNumber = process.env.GCP_PROJECT_NUMBER!;
  const poolId        = process.env.GCP_WORKLOAD_IDENTITY_POOL_ID!;
  const providerId    = process.env.GCP_WORKLOAD_IDENTITY_POOL_PROVIDER_ID!;
  const saEmail       = process.env.GCP_SERVICE_ACCOUNT_EMAIL!;

  // Step 1: Exchange Vercel OIDC token for STS access token
  const oidcToken = await getVercelOidcToken();
  const stsRes = await fetch('https://sts.googleapis.com/v1/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type:           'urn:ietf:params:oauth:grant-type:token-exchange',
      audience:             `//iam.googleapis.com/projects/${projectNumber}/locations/global/workloadIdentityPools/${poolId}/providers/${providerId}`,
      scope:                'https://www.googleapis.com/auth/cloud-platform',
      requested_token_type: 'urn:ietf:params:oauth:token-type:access_token',
      subject_token:        oidcToken,
      subject_token_type:   'urn:ietf:params:oauth:token-type:jwt',
    }),
  });
  const stsData = await stsRes.json() as { access_token?: string; error_description?: string };
  if (!stsData.access_token) {
    throw new Error(`STS exchange failed: ${stsData.error_description}`);
  }

  // Step 2: Use STS token to impersonate service account and get a BigQuery-scoped token
  const saRes = await fetch(
    `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${saEmail}:generateAccessToken`,
    {
      method:  'POST',
      headers: {
        Authorization:  `Bearer ${stsData.access_token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ scope: ['https://www.googleapis.com/auth/bigquery'] }),
    }
  );
  const saData = await saRes.json() as { accessToken?: string; error?: { message: string } };
  if (!saData.accessToken) {
    throw new Error(`generateAccessToken failed: ${saData.error?.message}`);
  }

  return saData.accessToken;
}

// Custom auth client that satisfies the google-auth-library AuthClient interface
class VercelWifAuthClient {
  private tokenPromise: Promise<string> | null = null;
  private tokenExpiry = 0;

  async getAccessToken(): Promise<{ token: string | null | undefined }> {
    // Cache token until 5 minutes before expiry (SA tokens last 1 hour)
    if (!this.tokenPromise || Date.now() > this.tokenExpiry - 300_000) {
      this.tokenExpiry = Date.now() + 3600_000;
      this.tokenPromise = getServiceAccountToken();
    }
    const token = await this.tokenPromise;
    return { token };
  }

  async getRequestHeaders(): Promise<Record<string, string>> {
    const { token } = await this.getAccessToken();
    return { Authorization: `Bearer ${token}` };
  }
}

export const bigquery = new BigQuery({
  projectId: process.env.GOOGLE_CLOUD_PROJECT_ID,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  authClient: new VercelWifAuthClient() as any,
});

export const dataset = bigquery.dataset(process.env.BIGQUERY_DATASET_ID!);

// ---------------------------------------------------------------------------
// loadIntoBigQuery — free-tier compatible batch insert via load job.
// Uses NEWLINE_DELIMITED_JSON source format streamed from an in-memory buffer.
// This is equivalent to the old `table.insert()` streaming API but works on
// the BigQuery free tier (streaming inserts are a paid feature).
// ---------------------------------------------------------------------------
export async function loadIntoBigQuery(
  tableName: string,
  records: Record<string, unknown>[],
): Promise<void> {
  const ndjson = records.map((r) => JSON.stringify(r)).join('\n');
  const stream = Readable.from([ndjson]);

  const table = dataset.table(tableName);
  const [job] = await table.load(stream, {
    sourceFormat:      'NEWLINE_DELIMITED_JSON',
    writeDisposition:  'WRITE_APPEND',
    createDisposition: 'CREATE_NEVER', // table must already exist
  });

  const errors = job.status?.errors;
  if (errors && errors.length > 0) {
    throw new Error(`BigQuery load job errors: ${JSON.stringify(errors)}`);
  }
}
