{% macro classify_edge_risk() %}

CASE
    -- dormant: hasn't transacted in the last 3 months
    WHEN days_since_last_tnx > 90
    THEN 'dormant'

    -- whale: high average transaction value, actively transacting
    WHEN avg_eth_value > 10 AND days_since_last_tnx < 30
    THEN 'whale'

    -- high frequency: many small transactions (mixer pattern)
    WHEN tnx_count > 100 AND avg_eth_value < 0.2
    THEN 'high_frequency_small'

    -- accumulator: high total value, low transaction count
    WHEN total_eth_value > 50 AND tnx_count < 10
    THEN 'accumulator'

    -- active normal
    WHEN days_since_last_tnx < 30
    THEN 'active'

    ELSE 'inactive'
END

{% endmacro %}