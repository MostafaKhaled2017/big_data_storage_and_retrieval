-- Q2: Personalized top products per user using friends behavior logs.
WITH target_users AS (
    SELECT e.user_id
    FROM events e
    GROUP BY e.user_id
    ORDER BY COUNT(*) DESC
    LIMIT 100
),
friend_edges AS (
    SELECT user_id, friend_id FROM friends
    UNION ALL
    SELECT friend_id AS user_id, user_id AS friend_id FROM friends
),
candidate_scores AS (
    SELECT
        fe.user_id AS target_user_id,
        e.product_id,
        SUM(
            CASE e.event_type
                WHEN 'purchase' THEN 3
                WHEN 'cart' THEN 2
                WHEN 'view' THEN 1
                ELSE 0
            END
        ) AS weighted_score,
        COUNT(*) AS interactions
    FROM friend_edges fe
    JOIN target_users tu
      ON tu.user_id = fe.user_id
    JOIN events e
      ON e.user_id = fe.friend_id
    WHERE e.event_type IN ('view', 'cart', 'purchase')
    GROUP BY fe.user_id, e.product_id
),
filtered_scores AS (
    SELECT cs.*
    FROM candidate_scores cs
    WHERE NOT EXISTS (
        SELECT 1
        FROM events own
        WHERE own.user_id = cs.target_user_id
          AND own.product_id = cs.product_id
    )
),
ranked AS (
    SELECT
        fs.*,
        ROW_NUMBER() OVER (
            PARTITION BY fs.target_user_id
            ORDER BY fs.weighted_score DESC, fs.interactions DESC, fs.product_id
        ) AS rank_in_user
    FROM filtered_scores fs
)
SELECT
    r.target_user_id,
    r.product_id,
    r.weighted_score,
    r.interactions,
    p.category_code,
    p.brand,
    r.rank_in_user
FROM ranked r
LEFT JOIN products p
  ON p.product_id = r.product_id
WHERE r.rank_in_user <= 5
ORDER BY r.target_user_id, r.rank_in_user;
