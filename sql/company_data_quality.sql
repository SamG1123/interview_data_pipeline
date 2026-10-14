
SELECT
    c.company_name,
    COUNT(*) AS total_questions,
    SUM(
        CASE
            WHEN f.job_role IS NULL
                 OR TRIM(f.job_role) = ''
                 OR LOWER(TRIM(f.job_role)) = 'unknown'
            THEN 1
            ELSE 0
        END
    ) AS unknown_job_role_count
FROM fact_questions AS f
JOIN dim_company AS c
    ON f.company_id = c.company_id
GROUP BY c.company_name
ORDER BY unknown_job_role_count DESC, total_questions DESC;
