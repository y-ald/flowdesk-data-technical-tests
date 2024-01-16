WITH OrderedIndexes AS (
    SELECT
        t.transaction_id,
        i.currency_1,
        i.currency_2,
        i.value,
        i.updated_at,
        i.exchange,
        i.exchange_type,
        ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY ABS(extract(epoch from (t.executed_at - i.updated_at)))) AS row_num
    FROM
        trades t
    JOIN indexes i ON t.currency_1 = i.currency_1
                   AND t.currency_2 = i.currency_2
                   AND t.exchange = i.exchange
                   AND t.exchange_type = i.exchange_type
)
SELECT
    t.*,
    r.value AS index_value,
    r.updated_at AS index_updated_at
FROM
    trades t
JOIN OrderedIndexes r ON t.transaction_id = r.transaction_id AND r.row_num = 1;
