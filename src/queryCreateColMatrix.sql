WITH pa AS (SELECT vid, sid::text, time::timestamp::date AS date, count(*) 
            FROM photos group by vid, sid, date),
        pb AS (SELECT vid, sid::text, count(date) AS cnt FROM pa GROUP BY vid, sid),
        pc AS (SELECT vid, sid::text, CASE WHEN cnt=1 THEN 1 WHEN cnt>1 THEN 2 ELSE 0 END pvalue FROM pb),
        ta AS (SELECT vid, sid::text, 1 AS liked FROM tips WHERE authorInteractionType='liked')
    SELECT u.vid, u.sid::text, pc.pvalue, liked
        INTO user_rate
        FROM useritem u
    LEFT JOIN pc ON u.vid = pc.vid AND u.sid = pc.sid
    LEFT JOIN ta ON u.vid = ta.vid AND u.sid = ta.sid;i:
