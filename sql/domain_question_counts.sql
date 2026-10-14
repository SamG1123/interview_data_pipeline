
SELECT
    d.domain,
    COUNT(DISTINCT f.question_id) AS question_count
FROM fact_questions AS f
JOIN dim_domain AS d
    ON f.domain_id = d.domain_id
GROUP BY d.domain
ORDER BY question_count DESC, d.domain ASC;
