// ============================================================
// GET /api/backfill/pinterest?from=2024-10-01
// Manually triggered backfill for Pinterest Ads.
// Pinterest API has a ~90 day rolling lookback limit on analytics.
// Protected by x-ingest-secret header.
// ============================================================

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateIngestSecret } from '../../lib/auth';
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

interface PinterestRow {
  DATE?:                  string;
  AD_GROUP_NAME:          string;
  CAMPAIGN_NAME:          string;
  SPEND_IN_MICRO_DOLLAR:  string | number;
  CLICK_1:                string | number;
  IMPRESSION_1:           string | number;
  TOTAL_ORDER_QUANTITY:   string | number;
}

function getWeeklyChunks(from: string, to: string) {
  const chunks: { start_date: string; end_date: string }[] = [];
  const end = new Date(to);
  let cursor = new Date(from);

  while (cursor <= end) {
    const start_date = cursor.toISOString().split('T')[0];
    const weekEnd = new Date(cursor);
    weekEnd.setDate(weekEnd.getDate() + 6);
    const end_date = (weekEnd < end ? weekEnd : end).toISOString().split('T')[0];
    chunks.push({ start_date, end_date });
    cursor.setDate(cursor.getDate() + 7);
  }
  return chunks;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (!validateIngestSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const from = (req.query.from as string) ?? (() => {
    const d = new Date();
    d.setDate(d.getDate() - 89);
    return d.toISOString().split('T')[0];
  })();
  const to = (req.query.to as string) ?? new Date().toISOString().split('T')[0];

  const token = await getAccessToken();
  const chunks = getWeeklyChunks(from, to);
  let total = 0;

  for (const { start_date, end_date } of chunks) {
    const url = new URL(
      `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups/analytics`
    );
    url.searchParams.set('start_date',  start_date);
    url.searchParams.set('end_date',    end_date);
    url.searchParams.set('columns',     'AD_GROUP_NAME,CAMPAIGN_NAME,SPEND_IN_MICRO_DOLLAR,CLICK_1,IMPRESSION_1,TOTAL_ORDER_QUANTITY');
    url.searchParams.set('granularity', 'DAY');

    const response = await fetch(url.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await response.json() as { items?: PinterestRow[]; message?: string };

    if (!response.ok) {
      console.error(`Pinterest error for ${start_date}–${end_date}:`, json);
      continue;
    }

    const rows = (json.items ?? []).map((r) => ({
      date:          r.DATE ?? start_date,
      campaign_name: r.CAMPAIGN_NAME  ?? '',
      ad_group_name: r.AD_GROUP_NAME  ?? null,
      spend:         (Number(r.SPEND_IN_MICRO_DOLLAR) || 0) / 1_000_000,
      clicks:        Number(r.CLICK_1)              || 0,
      impressions:   Number(r.IMPRESSION_1)         || 0,
      orders:        Number(r.TOTAL_ORDER_QUANTITY) || 0,
      revenue:       0,
    }));

    if (rows.length === 0) continue;

    try {
      await loadIntoBigQuery('raw_pinterest_ads', rows);
      total += rows.length;
      console.log(`Loaded ${rows.length} rows for ${start_date}–${end_date}`);
    } catch (err: unknown) {
      console.error(`BigQuery load job error for ${start_date}–${end_date}:`, err);
    }
  }

  return res.status(200).json({ ok: true, total });
}
