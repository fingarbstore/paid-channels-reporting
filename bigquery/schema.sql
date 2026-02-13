-- ============================================================
-- Paid Channels Reporting — BigQuery Schema
-- Run each statement in the BigQuery console (query editor)
-- Replace {project} with your GCP project ID throughout
-- Dataset: paid_channels
-- ============================================================

-- ─── Raw tables ──────────────────────────────────────────────
-- BigQuery is append-only: no UNIQUE constraints, no UPSERT.
-- Deduplication is handled in the views below via ROW_NUMBER().

-- Google Ads
-- Sheet columns: Campaign | Asset Group | Month | Year | Clicks | Impr. | Currency code |
--                Cost | Conversions | Conv. value | Store | Agency | Campaign Type | Month
CREATE TABLE IF NOT EXISTS `{project}.paid_channels.raw_google_ads` (
  date             DATE      NOT NULL,
  campaign_name    STRING    NOT NULL,
  asset_group_name STRING,
  clicks           INT64     DEFAULT 0,
  impressions      INT64     DEFAULT 0,
  currency_code    STRING    DEFAULT 'GBP',
  cost             FLOAT64   DEFAULT 0,
  conversions      FLOAT64   DEFAULT 0,
  conv_value       FLOAT64   DEFAULT 0,
  fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Meta Ads
-- Sheet columns: Reporting starts | Reporting ends | Ad Set Name | Campaign name | Results |
--                Result indicator | Reach | Frequency | Amount spent (GBP) | Impressions |
--                Link clicks | Clicks (all) | Purchases | Purchases conversion value |
--                Adds to cart | Agency | Store | CampaignType | Audience Type |
--                Corrected Date | Month | Year
CREATE TABLE IF NOT EXISTS `{project}.paid_channels.raw_meta_ads` (
  date                         DATE      NOT NULL,
  reporting_starts             DATE,
  reporting_ends               DATE,
  adset_name                   STRING,
  campaign_name                STRING    NOT NULL,
  results                      FLOAT64   DEFAULT 0,
  result_indicator             STRING,
  reach                        INT64     DEFAULT 0,
  frequency                    FLOAT64   DEFAULT 0,
  amount_spent                 FLOAT64   DEFAULT 0,
  impressions                  INT64     DEFAULT 0,
  link_clicks                  INT64     DEFAULT 0,
  clicks_all                   INT64     DEFAULT 0,
  purchases                    INT64     DEFAULT 0,
  purchases_conversion_value   FLOAT64   DEFAULT 0,
  adds_to_cart                 INT64     DEFAULT 0,
  fetched_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Pinterest Ads
-- Sheet columns: Date | Campaign | Ad Group | Spend | Clicks | Impressions |
--                Orders | Revenue | Store | Agency | Campaign Type | Audience Type | Month | Year
CREATE TABLE IF NOT EXISTS `{project}.paid_channels.raw_pinterest_ads` (
  date             DATE      NOT NULL,
  campaign_name    STRING    NOT NULL,
  ad_group_name    STRING,
  spend            FLOAT64   DEFAULT 0,
  clicks           INT64     DEFAULT 0,
  impressions      INT64     DEFAULT 0,
  orders           INT64     DEFAULT 0,
  revenue          FLOAT64   DEFAULT 0,
  fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ─── Categorisation rules ────────────────────────────────────
-- Keyword matched against campaign_name (case-insensitive) using REGEXP_CONTAINS.
-- Higher priority = evaluated first. NULL fields = no override for that field.

CREATE TABLE IF NOT EXISTS `{project}.paid_channels.campaign_category_rules` (
  platform       STRING NOT NULL,   -- 'google' | 'meta' | 'pinterest'
  keyword        STRING,            -- matched against campaign_name (case-insensitive); NULL = no campaign-name match
  adset_keyword  STRING,            -- matched against adset_name (case-insensitive); NULL = no adset-name match (meta only)
  store          STRING,            -- 'couv' | 'garb' | 'both' | 'ignore'
  agency         STRING,            -- 'bravada' | 'shoptimised' | 'other' | 'ignore'
  campaign_type  STRING,            -- 'pmax' | 'search' | 'catalogue_sales' | 'adv_plus'
  audience_type  STRING,            -- 'retargeting' | 'prospecting' | 'mixed'
  priority       INT64 DEFAULT 0    -- higher = evaluated first
);

-- ─── Seed data ───────────────────────────────────────────
-- Run ALTER TABLE first if the table already exists without adset_keyword:
--   ALTER TABLE `{project}.paid_channels.campaign_category_rules`
--   ADD COLUMN adset_keyword STRING;
--
-- Then run this INSERT:
INSERT INTO `{project}.paid_channels.campaign_category_rules`
  (platform, keyword, adset_keyword, store, agency, campaign_type, audience_type, priority)
VALUES

-- ── GOOGLE ADS ─────────────────────────────────────────
-- Store
('google', 'couv',              NULL, 'couv',   NULL,        NULL,              NULL, 20),
('google', 'garb',              NULL, 'garb',   NULL,        NULL,              NULL, 20),
('google', 'couverture',        NULL, 'couv',   NULL,        NULL,              NULL, 20),
('google', 'garbstore',         NULL, 'garb',   NULL,        NULL,              NULL, 20),
('google', 'both brands',       NULL, 'both',   NULL,        NULL,              NULL, 15),
('google', 'both stores',       NULL, 'both',   NULL,        NULL,              NULL, 15),
('google', 'brand core',        NULL, 'both',   NULL,        NULL,              NULL, 15),
('google', 'cgs.*brand.*women', NULL, 'couv',   NULL,        NULL,              NULL, 16),
('google', 'porter yoshida',    NULL, 'both',   NULL,        NULL,              NULL, 16),
('google', 'la - brand',        NULL, 'ignore', NULL,        NULL,              NULL, 10),
('google', 'manhattan',         NULL, 'ignore', NULL,        NULL,              NULL, 10),
('google', '^test$',            NULL, 'ignore', NULL,        NULL,              NULL, 10),
-- Agency
('google', '^uk \\|',           NULL, NULL,     'bravada',   NULL,              NULL, 20),
('google', '^cgs',              NULL, NULL,     'bravada',   NULL,              NULL, 20),
('google', 'brand core',        NULL, NULL,     'bravada',   NULL,              NULL, 20),
('google', '^uk - ',            NULL, NULL,     'other',     NULL,              NULL, 20),
('google', '^la - ',            NULL, NULL,     'other',     NULL,              NULL, 20),
('google', '^manhattan',        NULL, NULL,     'other',     NULL,              NULL, 20),
-- Campaign type
('google', 'pmax',              NULL, NULL,     NULL,        'pmax',            NULL, 5),
('google', 'shopping',          NULL, NULL,     NULL,        'pmax',            NULL, 5),
('google', 'search',            NULL, NULL,     NULL,        'search',          NULL, 5),
('google', 'dynamic',           NULL, NULL,     NULL,        'catalogue_sales', NULL, 5),
('google', 'brand',             NULL, NULL,     NULL,        'search',          NULL, 3),
('google', 'generic',           NULL, NULL,     NULL,        'search',          NULL, 3),

-- ── META ADS ───────────────────────────────────────────
-- Store (campaign_name)
('meta', 'couv',               NULL, 'couv', NULL, NULL, NULL, 20),
('meta', 'garb',               NULL, 'garb', NULL, NULL, NULL, 20),
('meta', 'couverture',         NULL, 'couv', NULL, NULL, NULL, 20),
('meta', 'garbstore',          NULL, 'garb', NULL, NULL, NULL, 20),
('meta', 'homeware',           NULL, 'couv', NULL, NULL, NULL, 15),
('meta', 'womenswear',         NULL, 'couv', NULL, NULL, NULL, 15),
('meta', 'menswear',           NULL, 'garb', NULL, NULL, NULL, 15),
('meta', '^cgs',               NULL, 'both', NULL, NULL, NULL, 15),
('meta', '^uk \\| homeware',   NULL, 'couv', NULL, NULL, NULL, 16),
('meta', 'sign up',            NULL, 'both', NULL, NULL, NULL, 5),
-- Campaign type (campaign_name)
('meta', 'catalogue sales',    NULL, NULL, NULL, 'catalogue_sales', NULL, 10),
('meta', 'dynamic ads',        NULL, NULL, NULL, 'catalogue_sales', NULL, 10),
('meta', 'dynamic retarget',   NULL, NULL, NULL, 'catalogue_sales', NULL, 10),
('meta', 'adv\\+',             NULL, NULL, NULL, 'adv_plus',        NULL, 10),
-- Audience type fallback via campaign_name
('meta', 'prospecting & retargeting',   NULL, NULL, NULL, NULL, 'mixed',       9),
('meta', 'retargeting.*prospecting',    NULL, NULL, NULL, NULL, 'mixed',       9),
('meta', 'retargeting \\+ prospecting', NULL, NULL, NULL, NULL, 'mixed',       9),
('meta', '\\| retargeting \\|',         NULL, NULL, NULL, NULL, 'retargeting', 8),
('meta', '\\| prospecting \\|',         NULL, NULL, NULL, NULL, 'prospecting', 8),
('meta', 'dynamic retargeting',         NULL, NULL, NULL, NULL, 'retargeting', 8),
-- Audience type via adset_name (adset_keyword column — primary lookup)
('meta', NULL, '\\|retargeting\\|',     NULL, NULL, NULL, 'retargeting',  22),
('meta', NULL, '\\|prospecting\\|',     NULL, NULL, NULL, 'prospecting',  22),
('meta', NULL, '\\|catalogue sales\\|', NULL, NULL, NULL, 'prospecting',  22),  -- TDR = in-house brand, broad targeting
('meta', NULL, '\\|testing\\|',         NULL, NULL, NULL, 'prospecting',  22),
('meta', NULL, '\\|whitelisting\\|',    NULL, NULL, NULL, 'prospecting',  22),
('meta', NULL, '\\|engagement\\|',      NULL, NULL, NULL, 'prospecting',  22),
('meta', NULL, 'dpa.*retarget',         NULL, NULL, NULL, 'retargeting',  20),
('meta', NULL, 'retarget',              NULL, NULL, NULL, 'retargeting',  18),
('meta', NULL, 'lookalike',             NULL, NULL, NULL, 'prospecting',  18),
('meta', NULL, '\\blal\\b',             NULL, NULL, NULL, 'prospecting',  18),
('meta', NULL, '- la$',                 NULL, NULL, NULL, 'prospecting',  18),
('meta', NULL, '- la -',                NULL, NULL, NULL, 'prospecting',  18),
('meta', NULL, '\\| la \\|',            NULL, NULL, NULL, 'prospecting',  18),
('meta', NULL, 'broad',                 NULL, NULL, NULL, 'prospecting',  15),
('meta', NULL, 'lpv',                   NULL, NULL, NULL, 'prospecting',  15),
('meta', NULL, 'awareness',             NULL, NULL, NULL, 'prospecting',  15),
('meta', NULL, 'interest',              NULL, NULL, NULL, 'prospecting',  12),
('meta', NULL, 'testing',               NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'whitelisting',          NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'video views',           NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'view content',          NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'adjacent',              NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'affluent',              NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'competitor',            NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'soho house',            NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, 'sign up',               NULL, NULL, NULL, 'prospecting',  10),
('meta', NULL, '\\bdpa\\b',             NULL, NULL, NULL, 'retargeting',  10),

-- ── PINTEREST ──────────────────────────────────────────
('pinterest', 'couv',           NULL, 'couv', NULL, NULL,        NULL, 20),
('pinterest', 'couverture',     NULL, 'couv', NULL, NULL,        NULL, 20),
('pinterest', 'garb',           NULL, 'garb', NULL, NULL,        NULL, 20),
('pinterest', 'garbstore',      NULL, 'garb', NULL, NULL,        NULL, 20),
('pinterest', 'consideration',  NULL, NULL,   NULL, NULL, 'prospecting', 10),
('pinterest', 'retarget',       NULL, NULL,   NULL, NULL, 'retargeting', 10),
('pinterest', 'prospect',       NULL, NULL,   NULL, NULL, 'prospecting', 10),
('pinterest', 'performance\\+', NULL, NULL,   NULL, 'adv_plus',   NULL, 10);


-- ─── Deduplication views ─────────────────────────────────────
-- BigQuery is append-only, so re-running a cron creates duplicate rows.
-- These views keep only the most recently inserted row per unique key.

CREATE OR REPLACE VIEW `{project}.paid_channels.v_google_deduped` AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY date, campaign_name, COALESCE(asset_group_name, '')
      ORDER BY fetched_at DESC
    ) AS rn
  FROM `{project}.paid_channels.raw_google_ads`
)
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.paid_channels.v_meta_deduped` AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY date, COALESCE(adset_name, campaign_name)
      ORDER BY fetched_at DESC
    ) AS rn
  FROM `{project}.paid_channels.raw_meta_ads`
)
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.paid_channels.v_pinterest_deduped` AS
SELECT * EXCEPT (rn)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY date, campaign_name, COALESCE(ad_group_name, '')
      ORDER BY fetched_at DESC
    ) AS rn
  FROM `{project}.paid_channels.raw_pinterest_ads`
)
WHERE rn = 1;


-- ─── Sheets views (Connected Sheets queries these directly) ───
-- Column aliases match existing Google Sheet header names exactly.
-- Category lookup uses correlated subquery per field — no stored functions needed.

-- Google: exact column order matching Google RAW Data sheet
CREATE OR REPLACE VIEW `{project}.paid_channels.v_google_for_sheets` AS
SELECT
  g.campaign_name                                                               AS Campaign,
  COALESCE(g.asset_group_name, '')                                              AS `Asset Group`,
  FORMAT_DATE('%b', g.date)                                                     AS Month,
  EXTRACT(YEAR FROM g.date)                                                     AS Year,
  g.clicks                                                                      AS Clicks,
  g.impressions                                                                 AS Impressions,
  g.currency_code                                                               AS `Currency code`,
  g.cost                                                                        AS Cost,
  g.conversions                                                                 AS Conversions,
  g.conv_value                                                                  AS Conv_value,
  COALESCE(
    (SELECT store FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'google' AND store IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(g.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    'ignore'
  )                                                                             AS Store,
  COALESCE(
    (SELECT agency FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'google' AND agency IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(g.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    'other'
  )                                                                             AS Agency,
  COALESCE(
    (SELECT campaign_type FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'google' AND campaign_type IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(g.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    ''
  )                                                                             AS `Campaign Type`,
  FORMAT_DATE('%b', g.date)                                                     AS Month2
FROM `{project}.paid_channels.v_google_deduped` g
ORDER BY g.date DESC, g.campaign_name, g.asset_group_name;

-- Meta: exact column order matching Meta RAW Data sheet
-- Audience Type uses two-tier lookup: adset_keyword vs adset_name first (more specific),
-- then keyword vs campaign_name as fallback.
CREATE OR REPLACE VIEW `{project}.paid_channels.v_meta_for_sheets` AS
SELECT
  m.reporting_starts                                                            AS `Reporting starts`,
  m.reporting_ends                                                              AS `Reporting ends`,
  m.adset_name                                                                  AS `Ad Set Name`,
  m.campaign_name                                                               AS `Campaign name`,
  m.results                                                                     AS Results,
  m.result_indicator                                                            AS `Result indicator`,
  m.reach                                                                       AS Reach,
  m.frequency                                                                   AS Frequency,
  m.amount_spent                                                                AS Amount_spent_GBP,
  m.impressions                                                                 AS Impressions,
  m.link_clicks                                                                 AS `Link clicks`,
  m.clicks_all                                                                  AS Clicks_all,
  m.purchases                                                                   AS Purchases,
  m.purchases_conversion_value                                                  AS `Purchases conversion value`,
  m.adds_to_cart                                                                AS `Adds to cart`,
  COALESCE(
    (SELECT agency FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'meta' AND agency IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(m.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    'bravada'
  )                                                                             AS Agency,
  COALESCE(
    (SELECT store FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'meta' AND store IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(m.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    'ignore'
  )                                                                             AS Store,
  COALESCE(
    (SELECT campaign_type FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'meta' AND campaign_type IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(m.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    ''
  )                                                                             AS CampaignType,
  COALESCE(
    -- Primary: match adset_keyword against adset_name (more specific)
    (SELECT audience_type FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'meta' AND audience_type IS NOT NULL AND adset_keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(m.adset_name), LOWER(adset_keyword))
     ORDER BY priority DESC LIMIT 1),
    -- Fallback: match keyword against campaign_name
    (SELECT audience_type FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'meta' AND audience_type IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(m.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    ''
  )                                                                             AS `Audience Type`,
  m.date                                                                        AS `Corrected Date`,
  FORMAT_DATE('%b', m.date)                                                     AS Month,
  EXTRACT(YEAR FROM m.date)                                                     AS Year
FROM `{project}.paid_channels.v_meta_deduped` m
ORDER BY m.date DESC, m.campaign_name, m.adset_name;

-- Pinterest: column order matching Pinterest Raw Data sheet
CREATE OR REPLACE VIEW `{project}.paid_channels.v_pinterest_for_sheets` AS
SELECT
  p.date                                                                        AS Date,
  p.campaign_name                                                               AS Campaign,
  COALESCE(p.ad_group_name, '')                                                 AS `Ad Group`,
  p.spend                                                                       AS Spend,
  p.clicks                                                                      AS Clicks,
  p.impressions                                                                 AS Impressions,
  p.orders                                                                      AS Orders,
  p.revenue                                                                     AS Revenue,
  COALESCE(
    (SELECT store FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'pinterest' AND store IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(p.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    'ignore'
  )                                                                             AS Store,
  COALESCE(
    (SELECT agency FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'pinterest' AND agency IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(p.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    'other'
  )                                                                             AS Agency,
  COALESCE(
    (SELECT campaign_type FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'pinterest' AND campaign_type IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(p.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    ''
  )                                                                             AS `Campaign Type`,
  COALESCE(
    (SELECT audience_type FROM `{project}.paid_channels.campaign_category_rules`
     WHERE platform = 'pinterest' AND audience_type IS NOT NULL AND keyword IS NOT NULL
       AND REGEXP_CONTAINS(LOWER(p.campaign_name), LOWER(keyword))
     ORDER BY priority DESC LIMIT 1),
    ''
  )                                                                             AS `Audience Type`,
  FORMAT_DATE('%b', p.date)                                                     AS Month,
  EXTRACT(YEAR FROM p.date)                                                     AS Year
FROM `{project}.paid_channels.v_pinterest_deduped` p
ORDER BY p.date DESC, p.campaign_name, p.ad_group_name;
