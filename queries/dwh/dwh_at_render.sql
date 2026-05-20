-- Shortcuts
-- ^E + Ê : Run selected query


select * from public.daily_snapshot order by snapshot_date desc limit 100;


SELECT product_id,
       nb_reviews,
       average_rating,
       oldest_rating,
       newest_rating,
       snapshot_date
FROM public.daily_snapshot
LIMIT 1000;

SELECT DISTINCT snapshot_date, count(*)
FROM public.daily_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date DESC;




-- DELETE FROM public.daily_snapshot
--WHERE snapshot_date = '2026-05-13';