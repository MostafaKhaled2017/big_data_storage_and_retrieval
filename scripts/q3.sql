-- Q3: Full-text product search by keywords extracted from Q2 top products.
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
),
q2_products AS (
    SELECT DISTINCT r.product_id
    FROM ranked r
    WHERE r.rank_in_user <= 5
),
keywords AS (
    SELECT
        token AS keyword,
        COUNT(*) AS frequency
    FROM (
        SELECT
            regexp_split_to_table(lower(COALESCE(p.category_code, '')), E'[^a-z0-9]+') AS token
        FROM q2_products qp
        JOIN products p
          ON p.product_id = qp.product_id
    ) t
    WHERE token <> ''
      AND LENGTH(token) > 2
    GROUP BY token
    ORDER BY frequency DESC, token
    LIMIT 3
)
SELECT
    k.keyword,
    p.product_id,
    p.category_code,
    p.brand,
    ts_rank(
        to_tsvector('simple', COALESCE(p.category_code, '')),
        plainto_tsquery('simple', k.keyword)
    ) AS text_score
FROM keywords k
JOIN products p
  ON to_tsvector('simple', COALESCE(p.category_code, ''))
  @@ plainto_tsquery('simple', k.keyword)
ORDER BY k.keyword, text_score DESC, p.product_id
LIMIT 60;
