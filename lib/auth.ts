import type { VercelRequest } from '@vercel/node';

export function validateIngestSecret(req: VercelRequest): boolean {
  return req.headers['x-ingest-secret'] === process.env.INGEST_SECRET;
}

export function validateCronSecret(req: VercelRequest): boolean {
  return req.headers.authorization === `Bearer ${process.env.CRON_SECRET}`;
}
