-- Q1: Check if campaigns attracted purchases, and include social support signal.
WITH campaign_user_stats AS (
    SELECT
        m.campaign_id,
        m.message_type,
        c.channel,
        c.topic,
        m.user_id,
        bool_or(COALESCE(m.is_purchased, false)) AS did_purchase
    FROM messages m
    JOIN campaigns c
      ON c.campaign_id = m.campaign_id
     AND c.campaign_type = m.message_type
    WHERE m.user_id IS NOT NULL
    GROUP BY m.campaign_id, m.message_type, c.channel, c.topic, m.user_id
),
friend_edges AS (
    SELECT user_id, friend_id FROM friends
    UNION ALL
    SELECT friend_id AS user_id, user_id AS friend_id FROM friends
),
social_purchasers AS (
    SELECT DISTINCT
        s.campaign_id,
        s.message_type,
        s.user_id
    FROM campaign_user_stats s
    JOIN friend_edges fe
      ON fe.user_id = s.user_id
    JOIN campaign_user_stats sf
      ON sf.campaign_id = s.campaign_id
     AND sf.message_type = s.message_type
     AND sf.user_id = fe.friend_id
    WHERE s.did_purchase = true
      AND sf.did_purchase = true
)
SELECT
    s.campaign_id,
    s.message_type,
    s.channel,
    s.topic,
    COUNT(*) AS recipients,
    COUNT(*) FILTER (WHERE s.did_purchase) AS purchasers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE s.did_purchase) / NULLIF(COUNT(*), 0), 2) AS conversion_rate_pct,
    COUNT(*) FILTER (
        WHERE EXISTS (
            SELECT 1
            FROM social_purchasers sp
            WHERE sp.campaign_id = s.campaign_id
              AND sp.message_type = s.message_type
              AND sp.user_id = s.user_id
        )
    ) AS social_purchasers,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE EXISTS (
                SELECT 1
                FROM social_purchasers sp
                WHERE sp.campaign_id = s.campaign_id
                  AND sp.message_type = s.message_type
                  AND sp.user_id = s.user_id
            )
        ) / NULLIF(COUNT(*) FILTER (WHERE s.did_purchase), 0),
        2
    ) AS social_support_share_pct
FROM campaign_user_stats s
GROUP BY s.campaign_id, s.message_type, s.channel, s.topic
ORDER BY conversion_rate_pct DESC NULLS LAST, purchasers DESC
LIMIT 20;
