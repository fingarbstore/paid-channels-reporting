// ============================================================
// POST /api/ingest/google-ads
// Called by the Google Ads Script (daily-ingest.js and backfill.js).
// Receives an array of rows and streams them into BigQuery.
// Protected by x-ingest-secret header.
// ============================================================

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateIngestSecret } from '../../lib/auth';
import { dataset } from '../../lib/bigquery';

interface GoogleAdsRow {
  date: string;
  campaign_name: string;
  asset_group_name?: string;
  clicks: number;
  impressions: number;
  cost: number;
  conversions: number;
  conv_value: number;
  currency_code: string;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  if (!validateIngestSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { rows } = req.body as { rows: GoogleAdsRow[] };

  if (!Array.isArray(rows) || rows.length === 0) {
    return res.status(400).json({ error: 'No rows provided' });
  }

  const records = rows.map((r) => ({
    date:             r.date,
    campaign_name:    r.campaign_name,
    asset_group_name: r.asset_group_name ?? null,
    clicks:           r.clicks           ?? 0,
    impressions:      r.impressions       ?? 0,
    currency_code:    r.currency_code     ?? 'GBP',
    cost:             r.cost              ?? 0,
    conversions:      r.conversions       ?? 0,
    conv_value:       r.conv_value        ?? 0,
  }));

  try {
    await dataset.table('raw_google_ads').insert(records);
  } catch (err: unknown) {
    const details = (err as { errors?: unknown }).errors ?? err;
    console.error('BigQuery insert error:', details);
    return res.status(500).json({ error: String(err) });
  }

  return res.status(200).json({ ok: true, inserted: records.length });
}
