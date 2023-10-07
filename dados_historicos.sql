WITH
  max_date_data AS (
    SELECT MAX(DATE(ib.date)) AS max_date FROM `acoes-378306.acoes.ibovespa_historico` AS ib
  ),
  cinco_dias AS (
    SELECT
      '5 Dias' AS filter_type,
      CAST(ib.date AS DATE) AS date_index,
      CAST(ROUND(ib.close, 0) AS INTEGER) AS pontos
    FROM
      `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN
      max_date_data
    WHERE
      DATE(ib.date) >= DATE_SUB(max_date_data.max_date, INTERVAL 15 DAY)
    ORDER BY
      ib.date DESC
    LIMIT
      5
  ),
  um_mes AS (
    SELECT
      '1 Mês' AS filter_type,
      CAST(ib.date AS DATE) AS date_index,
      CAST(ROUND(ib.close, 0) AS INTEGER) AS pontos
    FROM
      `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN
      max_date_data
    WHERE
      DATE(ib.date) >= DATE_SUB(max_date_data.max_date, INTERVAL 1 MONTH)
    ORDER BY
      ib.date DESC
  ),
  seis_mes AS (
    SELECT
      '6 Mêses' AS filter_type,
      CAST(ib.date AS DATE) AS date_index,
      CAST(ROUND(ib.close, 0) AS INTEGER) AS pontos
    FROM
      `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN
      max_date_data
    WHERE
      DATE(ib.date) >= DATE_SUB(max_date_data.max_date, INTERVAL 6 MONTH)
  ),
  ano_ytd AS (
    SELECT
      'YTD (2023)' AS filter_type,
      CAST(ib.date AS DATE) AS date_index,
      CAST(ROUND(ib.close, 0) AS INTEGER) AS pontos
    FROM
      `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN
      max_date_data
    WHERE
      DATE(ib.date) >= DATE_TRUNC(max_date_data.max_date, YEAR)
  ),
  um_ano AS (
    SELECT
      '1 Ano' AS filter_type,
      CAST(ib.date AS DATE) AS date_index,
      CAST(ROUND(ib.close, 0) AS INTEGER) AS pontos
    FROM
      `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN
      max_date_data
    WHERE
      DATE(ib.date) >= DATE_SUB(max_date_data.max_date, INTERVAL 1 YEAR)
  ),
  cinco_anos AS (
    SELECT
      '5 Anos' AS filter_type,
      CAST(ib.date AS DATE) AS date_index,
      CAST(ROUND(ib.close, 0) AS INTEGER) AS pontos
    FROM
      `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN
      max_date_data
    WHERE
      DATE(ib.date) >= DATE_SUB(max_date_data.max_date, INTERVAL 5 YEAR)
  )
SELECT *
FROM cinco_dias
UNION ALL
SELECT *
FROM um_mes
UNION ALL
SELECT *
FROM seis_mes
UNION ALL
SELECT *
FROM ano_ytd
UNION ALL
SELECT *
FROM um_ano
UNION ALL
SELECT *
FROM cinco_anos
