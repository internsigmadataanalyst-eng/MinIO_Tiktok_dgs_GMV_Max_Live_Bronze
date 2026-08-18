MERGE INTO database-sigma.SILVER_DB.silver_tt_ads_gmvmax_live T
USING (

  WITH latest_raw AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        b.*,
        ROW_NUMBER() OVER (
          PARTITION BY
            UPPER(TRIM(b.toko)),
            UPPER(TRIM(b.id_campaign)),
            UPPER(TRIM(COALESCE(b.nama_live,''))),
            DATE(b.tanggal),
            COALESCE(b.waktu_peluncuran, TIMESTAMP '1900-01-01 00:00:00')
          ORDER BY b.snapshot_ts DESC, b.run_id DESC
        ) rn
      FROM database-sigma.BRONZE_DB.bronze_maxl b
    )
    WHERE rn = 1
  ),

  scaling_rule AS (
    SELECT
      UPPER(TRIM(toko)) AS toko,
      start_date,
      COALESCE(end_date, DATE '9999-12-31') AS end_date,
      scale_factor
    FROM database-sigma.CONFIG_DB.config_gmvmax_scaling
  ),

  base AS (
    SELECT
      DATE(lr.tanggal)                  AS tanggal,
      UPPER(TRIM(lr.toko))              AS toko,
      UPPER(TRIM(lr.nama_live))         AS nama_live,
      lr.waktu_peluncuran               AS waktu_peluncuran,
      UPPER(TRIM(lr.status))            AS status,
      UPPER(TRIM(lr.nama_kampanye))     AS nama_kampanye,
      UPPER(TRIM(lr.id_campaign))       AS id_campaign,
      UPPER(TRIM(lr.mata_uang))         AS mata_uang,

      SAFE_CAST(lr.biaya AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS spend,

      SAFE_CAST(lr.biaya_bersih AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS spend_net,

      SAFE_CAST(lr.pesanan_sku AS INT64) AS orders_sku,

      SAFE_CAST(lr.pesanan_toko_saat_ini AS INT64) AS orders_sku_toko,

      SAFE_CAST(lr.biaya_per_pesanan_toko_saat_ini AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS cpo,

      SAFE_CAST(lr.pendapatan_kotor AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS revenue_gross,

      SAFE_CAST(lr.penghasilan_bruto_toko_saat_ini AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS revenue_net,

      SAFE_CAST(
        REGEXP_REPLACE(SAFE_CAST(lr.roi_toko_saat_ini AS STRING), r'[^0-9.\-]', '')
        AS FLOAT64) AS roi,

      SAFE_CAST(lr.tayangan_live AS INT64) AS views_live,

      SAFE_CAST(lr.biaya_per_tayangan_live AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS cost_per_view,

      SAFE_CAST(lr.tayangan_live_10_detik AS INT64) AS views_live_10s,

      SAFE_CAST(lr.biaya_per_tayangan_live_10_detik AS NUMERIC)
        / COALESCE(sr.scale_factor,1) AS cost_per_view_10s,

      SAFE_CAST(lr.pengikut_saat_live AS INT64) AS followers_live,

      lr.snapshot_ts,
      lr.snapshot_date,
      lr.run_id,
      lr.row_hash_raw

    FROM latest_raw lr
    LEFT JOIN scaling_rule sr
      ON UPPER(TRIM(lr.toko)) = sr.toko
      AND DATE(lr.tanggal) BETWEEN sr.start_date AND sr.end_date
  ),

  with_hash AS (
    SELECT
      b.*,
      TO_HEX(SHA256(
        ARRAY_TO_STRING([
          FORMAT_DATE('%F', b.tanggal),
          b.toko,
          COALESCE(b.nama_live,''),
          CAST(b.waktu_peluncuran AS STRING),
          COALESCE(b.status,''),
          COALESCE(b.nama_kampanye,''),
          COALESCE(b.id_campaign,''),
          COALESCE(b.mata_uang,''),
          CAST(b.spend AS STRING),
          CAST(b.spend_net AS STRING),
          CAST(b.orders_sku AS STRING),
          CAST(b.orders_sku_toko AS STRING),
          CAST(b.cpo AS STRING),
          CAST(b.revenue_gross AS STRING),
          CAST(b.revenue_net AS STRING),
          CAST(b.roi AS STRING),
          CAST(b.views_live AS STRING),
          CAST(b.cost_per_view AS STRING),
          CAST(b.views_live_10s AS STRING),
          CAST(b.cost_per_view_10s AS STRING),
          CAST(b.followers_live AS STRING)
        ], '||')
      )) AS row_hash_clean
    FROM base b
  )

  SELECT * FROM with_hash

) S

ON  T.tanggal = S.tanggal
AND T.toko = S.toko
AND T.id_campaign = S.id_campaign
AND COALESCE(T.nama_live,'') = COALESCE(S.nama_live,'')
AND COALESCE(T.waktu_peluncuran, TIMESTAMP '1900-01-01 00:00:00')
  = COALESCE(S.waktu_peluncuran, TIMESTAMP '1900-01-01 00:00:00')

WHEN MATCHED AND T.row_hash_clean != S.row_hash_clean THEN
  UPDATE SET
    nama_live = S.nama_live,
    status = S.status,
    nama_kampanye = S.nama_kampanye,
    mata_uang = S.mata_uang,
    spend = S.spend,
    spend_net = S.spend_net,
    orders_sku = S.orders_sku,
    orders_sku_toko = S.orders_sku_toko,
    cpo = S.cpo,
    revenue_gross = S.revenue_gross,
    revenue_net = S.revenue_net,
    roi = S.roi,
    views_live = S.views_live,
    cost_per_view = S.cost_per_view,
    views_live_10s = S.views_live_10s,
    cost_per_view_10s = S.cost_per_view_10s,
    followers_live = S.followers_live,
    snapshot_ts = S.snapshot_ts,
    snapshot_date = S.snapshot_date,
    run_id = S.run_id,
    row_hash_raw = S.row_hash_raw,
    row_hash_clean = S.row_hash_clean

WHEN NOT MATCHED THEN
  INSERT ROW;