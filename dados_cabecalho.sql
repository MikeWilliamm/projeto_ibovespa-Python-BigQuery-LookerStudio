    select 
            cast(round(ib.regularMarketPrice, 0) as integer) as ibov_pontos
            ,cast(round(ib.regularMarketDayLow,0) as integer) as variacao_min_dia
            ,cast(round(ib.regularMarketDayHigh,0) as integer) as variacao_max_dia
            ,cast(round(ib.regularMarketPreviousClose,0) as integer) as fechamento_anterior
            ,cast(round(ib.regularMarketOpen,0) as integer) as abertura
            ,case when regularMarketChangePercent < 0 then FORMAT('%.2f', cast(round(ib.regularMarketChangePercent,2) as FLOAT64)) || '%' else '+' || FORMAT('%.2f',cast(round(ib.regularMarketChangePercent,2) as FLOAT64)) || '%' end as variacao_dia
            ,cast(round(ib.fiftyTwoWeekLow,2) as FLOAT64) as min_variacao_52_week
            ,cast(round(ib.fiftyTwoWeekHigh,2) as FLOAT64) as max_variacao_52_week
    from `acoes-378306.acoes.ibovespa_cabecalho` as ib