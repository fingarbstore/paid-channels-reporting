// ============================================================
// GET /api/backfill/meta?from=2023-01-01&to=2025-01-01
// Manually triggered backfill for Meta Ads.
// Fetches data in weekly chunks and inserts into BigQuery via load jobs.
// Protected by x-ingest-secret header.
// ============================================================

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateIngestSecret } from '../../lib/auth';
import { loadIntoBigQuery } from '../../lib/bigquery';

const AD_ACCOUNT_ID = process.env.META_AD_ACCOUNT_ID!;
const ACCESS_TOKEN  = process.env.META_ACCESS_TOKEN!;

const FIELDS = [
  'date_start',
  'date_stop',
  'adset_name',
  'campaign_name',
  'reach',
  'frequency',
  'spend',
  'impressions',
  'inline_link_clicks',
  'clicks',
  'actions',
  'action_values',
].join(',');

interface MetaInsight {
  date_start:          string;
  date_stop:           string;
  adset_name?:         string;
  campaign_name:       string;
  reach?:              string;
  frequency?:          string;
  spend?:              string;
  impressions?:        string;
  inline_link_clicks?: string;
  clicks?:             string;
  actions?:            Array<{ action_type: string; value: string }>;
  action_values?:      Array<{ action_type: string; value: string }>;
}

function getActionValue(
  actions: Array<{ action_type: string; value: string }> | undefined,
  type: string
): number {
  return parseFloat(actions?.find((a) => a.action_type === type)?.value ?? '0') || 0;
}

function getWeeklyChunks(from: string, to: string) {
  const chunks: { since: string; until: string }[] = [];
  const end = new Date(to);
  let cursor = new Date(from);

  while (cursor <= end) {
    const since = cursor.toISOString().split('T')[0];
    const weekEnd = new Date(cursor);
    weekEnd.setDate(weekEnd.getDate() + 6);
    const until = (weekEnd < end ? weekEnd : end).toISOString().split('T')[0];
    chunks.push({ since, until });
    cursor.setDate(cursor.getDate() + 7);
  }
  return chunks;
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (!validateIngestSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const from = (req.query.from as string) ?? (() => {
    const d = new Date('2023-01-01');
    return d.toISOString().split('T')[0];
  })();
  const to = (req.query.to as string) ?? new Date().toISOString().split('T')[0];

  const chunks = getWeeklyChunks(from, to);
  let total = 0;

  for (const { since, until } of chunks) {
    const url = new URL(
      `https://graph.facebook.com/v19.0/${AD_ACCOUNT_ID}/insights`
    );
    url.searchParams.set('fields',       FIELDS);
    url.searchParams.set('level',        'adset');
    url.searchParams.set('time_range',   JSON.stringify({ since, until }));
    url.searchParams.set('access_token', ACCESS_TOKEN);
    url.searchParams.set('limit',        '500');

    const response = await fetch(url.toString());
    const json = await response.json() as { data?: MetaInsight[]; error?: { message: string } };

    if (!response.ok || json.error) {
      console.error(`Meta API error for ${since}–${until}:`, json.error);
      continue;
    }

    const insights: MetaInsight[] = json.data ?? [];
    if (insights.length === 0) continue;

    const records = insights.map((r) => ({
      date:                         r.date_start,
      reporting_starts:             r.date_start,
      reporting_ends:               r.date_stop,
      adset_name:                   r.adset_name       ?? null,
      campaign_name:                r.campaign_name,
      results:                      0,
      result_indicator:             null,
      reach:                        parseInt(r.reach   ?? '0', 10) || 0,
      frequency:                    parseFloat(r.frequency ?? '0') || 0,
      amount_spent:                 parseFloat(r.spend ?? '0')     || 0,
      impressions:                  parseInt(r.impressions ?? '0', 10) || 0,
      link_clicks:                  parseInt(r.inline_link_clicks ?? '0', 10) || 0,
      clicks_all:                   parseInt(r.clicks ?? '0', 10)  || 0,
      purchases:                    Math.round(getActionValue(r.actions, 'purchase')),
      purchases_conversion_value:   getActionValue(r.action_values, 'purchase'),
      adds_to_cart:                 Math.round(getActionValue(r.actions, 'add_to_cart')),
    }));

    try {
      await loadIntoBigQuery('raw_meta_ads', records);
      total += records.length;
      console.log(`Loaded ${records.length} rows for ${since}–${until}`);
    } catch (err: unknown) {
      console.error(`BigQuery load job error for ${since}–${until}:`, err);
    }
  }

  return res.status(200).json({ ok: true, total });
}
