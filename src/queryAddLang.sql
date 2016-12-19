-- UPDATE TABLE ADD NEW COLUMN 
/* 
ALTER TABLE tips ADD COLUMN langProp numeric;
WITH tempA AS (SELECT vid, count(lang)::float 
	FROM tips WHERE lang='ja' GROUP BY vid, lang), 
	tempB AS (SELECT vid, count(vid)::float 
	FROM tips GROUP BY vid),
	tempC AS (SELECT tempA.vid, tempA.count/tempB.count AS prop 
	FROM tips LEFT JOIN tempA ON tips.vid=tempA.vid 
	LEFT JOIN tempB ON tips.vid=tempB.vid)
UPDATE tips SET langprop=tempC.prop FROM tempC WHERE tips.vid=tempC.vid;
*/
ALTER TABLE venues ADD COLUMN langProp numeric;
UPDATE venues v SET langprop=t.langprop FROM tips t WHERE v.vid=t.vid;
