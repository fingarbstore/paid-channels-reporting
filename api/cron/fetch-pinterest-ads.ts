// ============================================================
// GET /api/cron/fetch-pinterest-ads
// Vercel Cron: runs daily at 02:00 UTC
// Fetches yesterday's data from Pinterest Ads API and inserts
// it into BigQuery raw_pinterest_ads table via load job.
// ============================================================

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateCronSecret } from '../../lib/auth';
import { loadIntoBigQuery } from '../../lib/bigquery';

const AD_ACCOUNT_ID = process.env.PINTEREST_AD_ACCOUNT_ID!;
const APP_ID        = process.env.PINTEREST_APP_ID!;
const APP_SECRET    = process.env.PINTEREST_APP_SECRET!;

async function getAccessToken(): Promise<string> {
  const res = await fetch('https://api.pinterest.com/v5/oauth/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Authorization: `Basic ${Buffer.from(`${APP_ID}:${APP_SECRET}`).toString('base64')}`,
    },
    body: new URLSearchParams({
      grant_type:    'refresh_token',
      refresh_token: process.env.PINTEREST_REFRESH_TOKEN!,
    }),
  });
  const data = await res.json() as { access_token?: string; error?: string };
  if (!data.access_token) throw new Error(`Pinterest token refresh failed: ${data.error}`);
  return data.access_token;
}

interface PinterestAdGroup { id: string; name: string; }

interface PinterestRow {
  DATE?:                 string;
  AD_GROUP_NAME:         string;
  CAMPAIGN_NAME:         string;
  SPEND_IN_MICRO_DOLLAR: string | number;
  CLICKTHROUGH_1:        string | number;
  IMPRESSION_1:          string | number;
  TOTAL_CHECKOUT:        string | number;
}

async function listAdGroupIds(token: string): Promise<string[]> {
  const url = new URL(
    `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups`
  );
  url.searchParams.set('page_size', '250');
  const res = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${token}` },
  });
  const data = await res.json() as { items?: PinterestAdGroup[] };
  return (data.items ?? []).map((g) => g.id);
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  // Vercel Hobby plan doesn't send CRON_SECRET automatically — skip check if not set
  if (process.env.CRON_SECRET && !validateCronSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const dateStr = yesterday.toISOString().split('T')[0];

  const token = await getAccessToken();

  // Step 1: list all ad group IDs (analytics endpoint requires explicit IDs)
  const adGroupIds = await listAdGroupIds(token);
  if (adGroupIds.length === 0) {
    return res.status(200).json({ ok: true, inserted: 0 });
  }

  const url = new URL(
    `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups/analytics`
  );
  url.searchParams.set('start_date',    dateStr);
  url.searchParams.set('end_date',      dateStr);
  url.searchParams.set('ad_group_ids',  adGroupIds.join(','));
  url.searchParams.set('columns',       'AD_GROUP_NAME,CAMPAIGN_NAME,SPEND_IN_MICRO_DOLLAR,CLICKTHROUGH_1,IMPRESSION_1,TOTAL_CHECKOUT');
  url.searchParams.set('granularity',   'DAY');

  const response = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${token}` },
  });
  // The ad_groups/analytics endpoint returns a raw array, not { items: [] }
  const json = await response.json() as PinterestRow[] | { code?: number; message?: string };

  if (!response.ok) {
    console.error('Pinterest API error:', json);
    return res.status(502).json({ error: (json as { message?: string }).message ?? 'Pinterest API error' });
  }

  const items: PinterestRow[] = Array.isArray(json) ? json : [];

  if (items.length === 0) {
    return res.status(200).json({ ok: true, inserted: 0 });
  }

  const records = items.map((r) => ({
    date:          r.DATE ?? dateStr,
    campaign_name: r.CAMPAIGN_NAME  ?? '',
    ad_group_name: r.AD_GROUP_NAME  ?? null,
    spend:         (Number(r.SPEND_IN_MICRO_DOLLAR) || 0) / 1_000_000,
    clicks:        Number(r.CLICKTHROUGH_1)       || 0,
    impressions:   Number(r.IMPRESSION_1)         || 0,
    orders:        Number(r.TOTAL_CHECKOUT)       || 0,
    revenue:       0,
  }));

  try {
    await loadIntoBigQuery('raw_pinterest_ads', records);
  } catch (err: unknown) {
    console.error('BigQuery load job error:', err);
    return res.status(500).json({ error: String(err) });
  }

  return res.status(200).json({ ok: true, inserted: records.length });
}
