// TEMPORARY DEBUG ENDPOINT — DELETE AFTER USE
// Returns the decoded Vercel OIDC token claims so we can confirm
// the exact `sub` value to use in the GCP IAM binding.
// Protected by x-ingest-secret header.

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateIngestSecret } from '../../lib/auth';
import { getVercelOidcToken } from '@vercel/oidc';

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (!validateIngestSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const token = await getVercelOidcToken();
  const payload = JSON.parse(
    Buffer.from(token.split('.')[1], 'base64url').toString('utf8')
  );

  return res.status(200).json({
    sub: payload.sub,
    iss: payload.iss,
    aud: payload.aud,
  });
}
