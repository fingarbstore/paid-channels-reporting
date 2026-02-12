// TEMPORARY DEBUG ENDPOINT — DELETE AFTER USE
// Diagnoses why Pinterest analytics returns 0 rows.
// Shows raw listAdGroupIds response and raw analytics response for a given date.
// Protected by x-ingest-secret header.
// Usage: GET /api/debug/pinterest?date=2025-01-15
//        GET /api/debug/pinterest  (defaults to 7 days ago)

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { validateIngestSecret } from '../../lib/auth';

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

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (!validateIngestSecret(req)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  // Default to 7 days ago (likely to have data)
  const dateParam = req.query.date as string | undefined;
  const dateStr = dateParam ?? (() => {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    return d.toISOString().split('T')[0];
  })();

  try {
    // Step 1: get token
    const token = await getAccessToken();

    // Step 2: list ad groups (raw response)
    const adGroupsUrl = new URL(
      `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups`
    );
    adGroupsUrl.searchParams.set('page_size', '250');
    const adGroupsRes = await fetch(adGroupsUrl.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const adGroupsJson = await adGroupsRes.json() as {
      items?: Array<{ id: string; name: string; status?: string; campaign_id?: string }>;
      message?: string;
    };

    const adGroupIds = (adGroupsJson.items ?? []).map((g) => g.id);

    if (adGroupIds.length === 0) {
      return res.status(200).json({
        ad_account_id: AD_ACCOUNT_ID,
        ad_groups_status: adGroupsRes.status,
        ad_groups_raw: adGroupsJson,
        analytics: null,
        note: 'No ad group IDs returned — cannot call analytics',
      });
    }

    // Step 3: analytics for the target date
    const analyticsUrl = new URL(
      `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups/analytics`
    );
    analyticsUrl.searchParams.set('start_date',   dateStr);
    analyticsUrl.searchParams.set('end_date',     dateStr);
    analyticsUrl.searchParams.set('ad_group_ids', adGroupIds.join(','));
    analyticsUrl.searchParams.set('columns',      'AD_GROUP_NAME,CAMPAIGN_NAME,SPEND_IN_MICRO_DOLLAR,CLICKTHROUGH_1,IMPRESSION_1,TOTAL_CHECKOUT');
    analyticsUrl.searchParams.set('granularity',  'DAY');

    const analyticsRes = await fetch(analyticsUrl.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const analyticsJson = await analyticsRes.json();

    return res.status(200).json({
      ad_account_id:      AD_ACCOUNT_ID,
      date_queried:       dateStr,
      ad_groups_count:    adGroupIds.length,
      ad_groups_ids:      adGroupIds,
      ad_groups_names:    (adGroupsJson.items ?? []).map((g) => ({ id: g.id, name: g.name, status: g.status })),
      analytics_status:   analyticsRes.status,
      analytics_raw:      analyticsJson,
    });

  } catch (err) {
    return res.status(500).json({ error: String(err) });
  }
}
