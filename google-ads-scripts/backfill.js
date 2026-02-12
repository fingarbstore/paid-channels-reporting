// ============================================================
// Google Ads Script — Historical Backfill
// Run ONCE manually to populate Jan 2023 → yesterday
// Paste into Google Ads Scripts, run manually (do NOT schedule)
// ============================================================

var INGEST_URL    = 'https://YOUR-NEW-PROJECT.vercel.app/api/ingest/google-ads'; // update after Vercel deploy
var INGEST_SECRET = 'REPLACE_WITH_YOUR_INGEST_SECRET';

var BACKFILL_FROM = '2023-01-01'; // adjust as needed
var BATCH_DAYS    = 30;           // days per batch (keep under script execution limit)

function main() {
  var from    = new Date(BACKFILL_FROM);
  var today   = new Date();
  var cursor  = new Date(from);
  var batches = 0;

  while (cursor < today) {
    var batchStart = formatDate(cursor);
    cursor.setDate(cursor.getDate() + BATCH_DAYS - 1);
    if (cursor >= today) cursor = new Date(today.getTime() - 86400000); // cap at yesterday
    var batchEnd = formatDate(cursor);
    cursor.setDate(cursor.getDate() + 1); // advance past batch end

    Logger.log('Fetching ' + batchStart + ' → ' + batchEnd);

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
      'WHERE segments.date BETWEEN "' + batchStart + '" AND "' + batchEnd + '"',
      '  AND campaign.status != "REMOVED"',
    ].join(' ');

    var rows = [];
    var result = AdsApp.search(query);

    while (result.hasNext()) {
      var row = result.next();
      rows.push({
        date:             row['segments']['date'],
        campaign_name:    row['campaign']['name'],
        asset_group_name: row['adGroup']['name'],
        clicks:           parseInt(row['metrics']['clicks'], 10)       || 0,
        impressions:      parseInt(row['metrics']['impressions'], 10)   || 0,
        cost:             (parseInt(row['metrics']['costMicros'], 10)   || 0) / 1000000,
        conversions:      parseFloat(row['metrics']['conversions'])     || 0,
        conv_value:       parseFloat(row['metrics']['conversionsValue']) || 0,
        currency_code:    row['customer']['currencyCode'],
      });
    }

    if (rows.length > 0) {
      var response = UrlFetchApp.fetch(INGEST_URL, {
        method:      'post',
        contentType: 'application/json',
        headers:     { 'x-ingest-secret': INGEST_SECRET },
        payload:     JSON.stringify({ rows: rows }),
        muteHttpExceptions: true,
      });
      Logger.log('Batch ' + (++batches) + ': ' + rows.length + ' rows — HTTP ' + response.getResponseCode());
    } else {
      Logger.log('No data for ' + batchStart + ' → ' + batchEnd);
    }

    // Pause between batches to avoid rate limits
    Utilities.sleep(1000);
  }

  Logger.log('Backfill complete. ' + batches + ' batches sent.');
}

function formatDate(d) {
  var y = d.getFullYear();
  var m = String(d.getMonth() + 1).padStart(2, '0');
  var day = String(d.getDate()).padStart(2, '0');
  return y + '-' + m + '-' + day;
}
