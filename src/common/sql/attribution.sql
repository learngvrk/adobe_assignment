-- Session-aware first-touch attribution of e-commerce revenue
-- to external search engine keywords.
--
-- Shared SQL: runs on DuckDB (Lambda/CLI) and Spark SQL (EMR).
-- No UDFs required — expects pre-computed columns:
--   _domain       : parsed search engine domain (NULL if not a search referrer)
--   _keyword      : parsed search keyword (NULL if none)
--   _is_purchase  : boolean flag for purchase events
--   _revenue      : extracted revenue from product_list
--
-- Session rules:
--   Session key   : ip + user_agent
--   Session break : inactivity gap > 30 minutes (1800 seconds)
--   First-touch   : first external search referrer per session

WITH gaps AS (
    SELECT *,
        CAST(hit_time_gmt AS BIGINT) AS hit_ts,
        CAST(hit_time_gmt AS BIGINT) - LAG(CAST(hit_time_gmt AS BIGINT)) OVER (
            PARTITION BY ip, user_agent
            ORDER BY CAST(hit_time_gmt AS BIGINT)
        ) AS gap
    FROM hits
),
sessions AS (
    SELECT *,
        SUM(CASE WHEN gap > 1800 OR gap IS NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY ip, user_agent
            ORDER BY hit_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_id
    FROM gaps
),
first_touch AS (
    SELECT
        ip, user_agent, session_id,
        FIRST(_domain) AS search_engine_domain,
        FIRST(_keyword) AS search_keyword
    FROM sessions
    WHERE _domain != '' AND _keyword != ''
    GROUP BY ip, user_agent, session_id
),
purchases AS (
    SELECT
        ip, user_agent, session_id,
        _revenue AS revenue
    FROM sessions
    WHERE _is_purchase
)
SELECT
    ft.search_engine_domain AS "Search Engine Domain",
    ft.search_keyword AS "Search Keyword",
    SUM(p.revenue) AS "Revenue"
FROM purchases p
JOIN first_touch ft
    ON p.ip = ft.ip
    AND p.user_agent = ft.user_agent
    AND p.session_id = ft.session_id
WHERE p.revenue > 0
GROUP BY ft.search_engine_domain, ft.search_keyword
ORDER BY "Revenue" DESC
