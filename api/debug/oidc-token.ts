// TEMPORARY DEBUG ENDPOINT — DELETE AFTER USE
// Tests the full STS token exchange and generateAccessToken call step by step.
// Protected by x-ingest-secret header.

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateIngestSecret } from '../../lib/auth';
import { getVercelOidcToken } from '@vercel/oidc';

const PROJECT_NUMBER = process.env.GCP_PROJECT_NUMBER!;
const POOL_ID = process.env.GCP_WORKLOAD_IDENTITY_POOL_ID!;
const PROVIDER_ID = process.env.GCP_WORKLOAD_IDENTITY_POOL_PROVIDER_ID!;
const SA_EMAIL = process.env.GCP_SERVICE_ACCOUNT_EMAIL!;

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (!validateIngestSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // Step 1: Get Vercel OIDC token
    const oidcToken = await getVercelOidcToken();
    const oidcPayload = JSON.parse(Buffer.from(oidcToken.split('.')[1], 'base64url').toString());

    // Step 2: Exchange OIDC token for GCP STS token
    const stsRes = await fetch('https://sts.googleapis.com/v1/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'urn:ietf:params:oauth:grant-type:token-exchange',
        audience: `//iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}`,
        scope: 'https://www.googleapis.com/auth/cloud-platform',
        requested_token_type: 'urn:ietf:params:oauth:token-type:access_token',
        subject_token: oidcToken,
        subject_token_type: 'urn:ietf:params:oauth:token-type:jwt',
      }),
    });
    const stsData = await stsRes.json() as { access_token?: string; error?: string; error_description?: string };

    if (!stsData.access_token) {
      return res.status(500).json({ step: 'sts_exchange', error: stsData });
    }

    // Step 3: Use STS token to call generateAccessToken on service account
    const saRes = await fetch(
      `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/${SA_EMAIL}:generateAccessToken`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${stsData.access_token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ scope: ['https://www.googleapis.com/auth/bigquery'] }),
      }
    );
    const saData = await saRes.json() as { accessToken?: string; error?: { message: string } };

    return res.status(200).json({
      oidc_sub: oidcPayload.sub,
      oidc_iss: oidcPayload.iss,
      sts_ok: !!stsData.access_token,
      sa_ok: !!saData.accessToken,
      sa_error: saData.error ?? null,
    });
  } catch (err) {
    return res.status(500).json({ error: String(err) });
  }
}
