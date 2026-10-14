
SELECT
    c.company_name,
    COUNT(DISTINCT f.question_id) AS question_count
FROM fact_questions AS f
JOIN dim_company AS c
    ON f.company_id = c.company_id
GROUP BY c.company_name
ORDER BY question_count DESC, c.company_name ASC;
