WITH
  max_date_data AS (
    SELECT MAX(DATE(ib.date)) AS max_date FROM `acoes-378306.acoes.ibovespa_historico` AS ib
  )
  ,cinco_dias AS (
    select filter_type, pontos from(
        SELECT '5 Dias' AS filter_type, CAST(ib.date AS DATE) AS date_index, cast(round(ib.close,0) as INTEGER) as pontos,
                ROW_NUMBER() OVER (ORDER BY ib.date desc) AS rank_date,
        FROM `acoes-378306.acoes.ibovespa_historico` AS ib
        CROSS JOIN max_date_data 
        WHERE DATE(ib.date) >= DATE_SUB(max_date, INTERVAL 15 DAY)
        ORDER BY ib.date DESC
    ) as r
    where r.rank_date = 5

  )
  ,um_mes AS (
    SELECT '1 Mês' AS filter_type, cast(round(ib.close,0) as INTEGER) as pontos
    FROM `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN max_date_data 
    WHERE DATE(ib.date) >= DATE_SUB(max_date, INTERVAL 1 MONTH)
    ORDER BY ib.date asc
    LIMIT 1
  )
  ,seis_mes AS (
    SELECT '6 Mêses' AS filter_type, cast(round(ib.close,0) as INTEGER) as pontos
    FROM `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN max_date_data 
    WHERE DATE(ib.date) >= DATE_SUB(max_date, INTERVAL 6 MONTH)
    ORDER BY ib.date asc
    limit 1
  )
   ,ano_ytd AS (
    SELECT 'YTD (2023)' AS filter_type, cast(round(ib.close,0) as INTEGER) as pontos
    FROM `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN max_date_data 
    WHERE DATE(ib.date) >= DATE_TRUNC(max_date, YEAR)
    order by ib.date asc
    limit 1
  )
    ,um_ano AS (
    SELECT '1 Ano' AS filter_type, cast(round(ib.close,0) as INTEGER) as pontos
    FROM `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN max_date_data 
    WHERE DATE(ib.date) >= DATE_SUB(max_date, INTERVAL 1 YEAR)
    order by ib.date asc
    limit 1
  )
    ,cinco_anos AS (
    SELECT '5 Anos' AS filter_type, cast(round(ib.close,0) as INTEGER) as pontos
    FROM `acoes-378306.acoes.ibovespa_historico` AS ib
    CROSS JOIN max_date_data 
    WHERE DATE(ib.date) >= DATE_SUB(max_date, INTERVAL 5 YEAR)
    order by ib.date asc
    limit 1
  )
  ,union_data as (
  SELECT t1.filter_type, round(((t2.regularMarketPrice - t1.pontos)/t1.pontos)*100, 2) as variacao
  FROM cinco_dias as t1
  cross join `acoes-378306.acoes.ibovespa_cabecalho` as t2
  UNION ALL
  SELECT t1.filter_type, round(((t2.regularMarketPrice - t1.pontos)/t1.pontos)*100, 2) as variacao
  FROM um_mes as t1
  cross join `acoes-378306.acoes.ibovespa_cabecalho` as t2
  UNION ALL
  SELECT t1.filter_type, round(((t2.regularMarketPrice - t1.pontos)/t1.pontos)*100, 2) as variacao
  FROM seis_mes as t1
  cross join `acoes-378306.acoes.ibovespa_cabecalho` as t2
  UNION ALL
  SELECT t1.filter_type, round(((t2.regularMarketPrice - t1.pontos)/t1.pontos)*100, 2) as variacao
  FROM ano_ytd as t1
  cross join `acoes-378306.acoes.ibovespa_cabecalho` as t2
  UNION ALL
  SELECT t1.filter_type, round(((t2.regularMarketPrice - t1.pontos)/t1.pontos)*100, 2) as variacao
  FROM um_ano as t1
  cross join `acoes-378306.acoes.ibovespa_cabecalho` as t2
  UNION ALL
  SELECT t1.filter_type, round(((t2.regularMarketPrice - t1.pontos)/t1.pontos)*100, 2) as variacao
  FROM cinco_anos as t1
  cross join `acoes-378306.acoes.ibovespa_cabecalho` as t2
  )

  select filter_type,
        case when variacao < 0 then FORMAT('%.2f',variacao) || '%' else '+' || FORMAT('%.2f',variacao) || '%' end as variacao_fomated
  from  union_data