WITH get_all AS (SELECT keyword, scrape_date, pos, url
                 FROM serp_flat_data_fact
                 WHERE country = 840
                   AND site = 'amazon.com'
                   AND device = 'mobile'
                   AND scrape_date >= '2021-01-01'
                   AND scrape_date <= '2021-01-31')
SELECT *
FROM get_all A
         INNER JOIN
     (SELECT keyword, MAX(scrape_date) AS max_scrape_date
      FROM get_all
      GROUP BY keyword) B
     ON A.keyword = B.keyword AND A.scrape_date = B.max_scrape_date
ORDER BY 1, 2, 3, 4, 5, 6;
