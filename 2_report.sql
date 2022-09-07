-- За expiration взяла последний день месяца исходных данных в формате mm/yyyy
-- Отчет построен в разрезе fraud_type, person_id за один день transaction_date. Многократные мошеннические действия агрегированы.
-- REFRESH ON COMMIT не работает с UNION ALL :( Пришлось оставить ON DEMAND



DROP MATERIALIZED VIEW fraud_report;
CREATE MATERIALIZED VIEW fraud_report
--REFRESH ON COMMIT
AS SELECT
	TO_CHAR(transaction_time, 'yyyy-mm-dd') AS transaction_date,
	person_id,
	'transaction after expiration date' AS fraud_type,
	COUNT(*) AS fraud_events_count,            -- количество мошеннических действий
	MIN(transaction_time) AS first_fraud_time, -- первая транзакция после истечения срока карты
	MAX(transaction_time) AS last_fraud_time,  -- последняя транзакция после истечения срока карты
	sysdate AS report_date
FROM (
	SELECT t.*, c.expires,
	CASE WHEN t.transaction_time>c.expires THEN 'neok' ELSE 'ok' END AS expiration_status
	FROM transactions t
	JOIN cards c
	ON t.person_id=c.person_id AND t.card=c.card_index
	WHERE status='ok' AND merchant_city!='ONLINE'
)
WHERE expiration_status='neok'
GROUP BY TO_CHAR(transaction_time, 'yyyy-mm-dd'), person_id

UNION ALL

SELECT
	TO_CHAR(transaction_time, 'yyyy-mm-dd') AS transaction_date,
	person_id,
	'transactions in different cities' AS fraud_type,
	COUNT(*) AS fraud_events_count,
	MIN(transaction_time) AS first_fraud_time,
	MAX(transaction_time) AS last_fraud_time,
	sysdate AS report_date
	--COUNT(DISTINCT merchant_city) AS city_cnt
FROM transactions
WHERE status='ok' AND merchant_city!='ONLINE'
GROUP BY TO_CHAR(transaction_time, 'yyyy-mm-dd'), person_id
HAVING COUNT(DISTINCT merchant_city)>1

ORDER BY transaction_date, person_id, fraud_type;


SELECT * FROM users;
SELECT * FROM cards;
SELECT * FROM card_dpan;
SELECT * FROM transactions;
SELECT * FROM fraud_report;