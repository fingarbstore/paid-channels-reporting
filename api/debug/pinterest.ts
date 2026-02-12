// TEMPORARY DEBUG ENDPOINT — DELETE AFTER USE
// Diagnoses why Pinterest analytics returns 0 rows.
// Shows raw ad groups list, analytics response for single date, and
// also tries fetching ALL ad groups including archived ones.
// Protected by x-ingest-secret header.
// Usage: GET /api/debug/pinterest?date=2025-12-01
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

    // Step 2a: list ad groups — default (active/paused only)
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

    // Step 2b: also try with entity_status=ACTIVE,PAUSED,ARCHIVED to catch all
    const adGroupsAllUrl = new URL(
      `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups`
    );
    adGroupsAllUrl.searchParams.set('page_size', '250');
    adGroupsAllUrl.searchParams.set('entity_statuses', 'ACTIVE,PAUSED,ARCHIVED');
    const adGroupsAllRes = await fetch(adGroupsAllUrl.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const adGroupsAllJson = await adGroupsAllRes.json() as {
      items?: Array<{ id: string; name: string; status?: string }>;
      message?: string;
    };

    const adGroupIds = (adGroupsJson.items ?? []).map((g) => g.id);
    const adGroupIdsAll = (adGroupsAllJson.items ?? []).map((g) => g.id);

    // Use the larger set for analytics
    const idsToUse = adGroupIdsAll.length > adGroupIds.length ? adGroupIdsAll : adGroupIds;

    if (idsToUse.length === 0) {
      return res.status(200).json({
        ad_account_id:    AD_ACCOUNT_ID,
        ad_groups_raw:    adGroupsJson,
        ad_groups_all_raw: adGroupsAllJson,
        analytics:        null,
        note:             'No ad group IDs returned from either query — check AD_ACCOUNT_ID env var',
      });
    }

    // Step 3: analytics for the target date
    const analyticsUrl = new URL(
      `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/ad_groups/analytics`
    );
    analyticsUrl.searchParams.set('start_date',   dateStr);
    analyticsUrl.searchParams.set('end_date',     dateStr);
    analyticsUrl.searchParams.set('ad_group_ids', idsToUse.join(','));
    analyticsUrl.searchParams.set('columns',      'AD_GROUP_NAME,CAMPAIGN_NAME,SPEND_IN_MICRO_DOLLAR,CLICKTHROUGH_1,IMPRESSION_1,TOTAL_CHECKOUT');
    analyticsUrl.searchParams.set('granularity',  'DAY');

    const analyticsRes = await fetch(analyticsUrl.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const analyticsJson = await analyticsRes.json();

    // Step 4: also try the campaigns analytics endpoint directly
    const campaignsAnalyticsUrl = new URL(
      `https://api.pinterest.com/v5/ad_accounts/${AD_ACCOUNT_ID}/campaigns/analytics`
    );
    campaignsAnalyticsUrl.searchParams.set('start_date',  dateStr);
    campaignsAnalyticsUrl.searchParams.set('end_date',    dateStr);
    campaignsAnalyticsUrl.searchParams.set('columns',     'CAMPAIGN_NAME,SPEND_IN_MICRO_DOLLAR,CLICKTHROUGH_1,IMPRESSION_1,TOTAL_CHECKOUT');
    campaignsAnalyticsUrl.searchParams.set('granularity', 'DAY');

    const campaignsRes = await fetch(campaignsAnalyticsUrl.toString(), {
      headers: { Authorization: `Bearer ${token}` },
    });
    const campaignsJson = await campaignsRes.json();

    return res.status(200).json({
      ad_account_id:            AD_ACCOUNT_ID,
      date_queried:             dateStr,
      // default ad groups query
      ad_groups_default_count:  adGroupIds.length,
      ad_groups_default_names:  (adGroupsJson.items ?? []).map((g) => ({ id: g.id, name: g.name, status: g.status })),
      // all-status ad groups query
      ad_groups_all_count:      adGroupIdsAll.length,
      ad_groups_all_names:      (adGroupsAllJson.items ?? []).map((g) => ({ id: g.id, name: g.name, status: g.status })),
      // analytics using the larger set
      analytics_ids_used:       idsToUse.length,
      analytics_status:         analyticsRes.status,
      analytics_raw:            analyticsJson,
      // campaigns-level analytics (no IDs required)
      campaigns_analytics_status: campaignsRes.status,
      campaigns_analytics_raw:    campaignsJson,
    });

  } catch (err) {
    return res.status(500).json({ error: String(err) });
  }
}
