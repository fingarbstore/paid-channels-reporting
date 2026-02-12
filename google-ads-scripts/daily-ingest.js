// ============================================================
// Google Ads Script — Daily Ingest
// Paste into: Google Ads → Tools → Bulk Actions → Scripts
// Schedule:   Daily (set time to e.g. 01:00)
// ============================================================

var INGEST_URL    = 'https://YOUR-NEW-PROJECT.vercel.app/api/ingest/google-ads'; // update after Vercel deploy
var INGEST_SECRET = 'REPLACE_WITH_YOUR_INGEST_SECRET'; // must match INGEST_SECRET env var in Vercel

function main() {
  var yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  var dateStr = Utilities.formatDate(yesterday, AdsApp.currentAccount().getTimeZone(), 'yyyy-MM-dd');

  var query = [
    'SELECT',
    '  campaign.name,',
    '  ad_group.name,',
    '  metrics.clicks,',
    '  metrics.impressions,',
    '  metrics.cost_micros,',
    '  metrics.conversions,',
    '  metrics.conversions_value,',
    '  customer.currency_code,',
    '  segments.date',
    'FROM ad_group',
    'WHERE segments.date = "' + dateStr + '"',
    '  AND campaign.status != "REMOVED"',
    '  AND ad_group.status != "REMOVED"',
  ].join(' ');

  var rows = [];
  var result = AdsApp.search(query);

  while (result.hasNext()) {
    var row = result.next();
    rows.push({
      date:             row['segments']['date'],
      campaign_name:    row['campaign']['name'],
      asset_group_name: row['adGroup']['name'],
      clicks:           parseInt(row['metrics']['clicks'], 10)      || 0,
      impressions:      parseInt(row['metrics']['impressions'], 10)  || 0,
      cost:             (parseInt(row['metrics']['costMicros'], 10)  || 0) / 1000000,
      conversions:      parseFloat(row['metrics']['conversions'])    || 0,
      conv_value:       parseFloat(row['metrics']['conversionsValue']) || 0,
      currency_code:    row['customer']['currencyCode'],
    });
  }

  if (rows.length === 0) {
    Logger.log('No rows found for ' + dateStr);
    return;
  }

  var response = UrlFetchApp.fetch(INGEST_URL, {
    method:      'post',
    contentType: 'application/json',
    headers:     { 'x-ingest-secret': INGEST_SECRET },
    payload:     JSON.stringify({ rows: rows }),
    muteHttpExceptions: true,
  });

  Logger.log('Status: ' + response.getResponseCode());
  Logger.log('Response: ' + response.getContentText());
}
