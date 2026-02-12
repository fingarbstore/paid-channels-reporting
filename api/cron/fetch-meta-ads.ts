// ============================================================
// GET /api/cron/fetch-meta-ads
// Vercel Cron: runs daily at 02:00 UTC
// Fetches yesterday's data from Meta Marketing API and streams
// it into BigQuery raw_meta_ads table.
// ============================================================

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateCronSecret } from '../../lib/auth';
import { dataset } from '../../lib/bigquery';

const AD_ACCOUNT_ID  = process.env.META_AD_ACCOUNT_ID!;
const ACCESS_TOKEN   = process.env.META_ACCESS_TOKEN!;

const FIELDS = [
  'date_start',
  'date_stop',
  'adset_name',
  'campaign_name',
  'results',
  'result_indicator',
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
  results?:            number;
  result_indicator?:   string;
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

export default async function handler(req: VercelRequest, res: VercelResponse) {
  // Vercel Hobby plan doesn't send CRON_SECRET automatically — skip check if not set
  if (process.env.CRON_SECRET && !validateCronSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const dateStr = yesterday.toISOString().split('T')[0];

  const url = new URL(
    `https://graph.facebook.com/v19.0/${AD_ACCOUNT_ID}/insights`
  );
  url.searchParams.set('fields',       FIELDS);
  url.searchParams.set('level',        'adset');
  url.searchParams.set('time_range',   JSON.stringify({ since: dateStr, until: dateStr }));
  url.searchParams.set('access_token', ACCESS_TOKEN);
  url.searchParams.set('limit',        '500');

  const response = await fetch(url.toString());
  const json = await response.json() as { data?: MetaInsight[]; error?: { message: string } };

  if (!response.ok || json.error) {
    console.error('Meta API error:', json.error);
    return res.status(502).json({ error: json.error?.message ?? 'Meta API error' });
  }

  const insights: MetaInsight[] = json.data ?? [];

  if (insights.length === 0) {
    return res.status(200).json({ ok: true, inserted: 0 });
  }

  const records = insights.map((r) => ({
    date:                         r.date_start,
    reporting_starts:             r.date_start,
    reporting_ends:               r.date_stop,
    adset_name:                   r.adset_name       ?? null,
    campaign_name:                r.campaign_name,
    results:                      r.results          ?? 0,
    result_indicator:             r.result_indicator ?? null,
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
    await dataset.table('raw_meta_ads').insert(records);
  } catch (err: unknown) {
    const details = (err as { errors?: unknown }).errors ?? err;
    console.error('BigQuery insert error:', details);
    return res.status(500).json({ error: String(err) });
  }

  return res.status(200).json({ ok: true, inserted: records.length });
}
