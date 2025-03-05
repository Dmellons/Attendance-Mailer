q_stu_contacts = """
WITH all_stu AS (
	SELECT 
	stu.ID, STU.LN, STU.FN, CONVERT(DATE, STU.BD) BD
	, stu.SC, loc.NM, stu.GR
	, stu.AD, stu.CY, stu.ST, stu.ZC
	, stu.TL
	, stu.CL
	, stu.PG, stu.PEM, stu.SEM
	, stu.SN
	, CASE
		WHEN STU.GR >= 6 THEN COUN.TE
		ELSE ''
	END AS counselor
	, stu.U11
	, EL.DE as ELL

	, stu.TG
	, stu.LD

	, stu.LF

	, stu.SP

	, stu.hl

	, ROW_NUMBER() OVER (PARTITION BY STU.ID 
		ORDER BY STU.DEL
		, CASE WHEN stu.LD IS NULL THEN 0 ELSE 1 END ASC
		, CASE WHEN stu.ED IS NULL THEN 0 ELSE 1 END DESC
		, stu.ED ASC
		, stu.TG
	) AS RN

	FROM STU
	LEFT JOIN LOC ON stu.SC = loc.CD
	LEFT JOIN TCH AS COUN ON STU.CU = COUN.TN AND STU.SC=COUN.SC
	LEFT JOIN COD EL ON EL.TC='STU' AND EL.FC='LF' AND EL.CD = STU.LF



	WHERE 
	1=1
	AND [STU].DEL = 0
	AND stu.TG IN ('')
	AND stu.gr not in('-2', '14', '17', '18')
	AND stu.sc in( 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 16, 50, 60, 61, 62 )
	AND stu.sp not in('K', 'N')
	AND STU.ED <= '{date}'

)
select distinct
astu.ID, CON.EM, astu.PEM

from all_stu astu
left JOIN CON ON astu.ID = CON.PID
	AND CON.EM <> ''
	AND CON.AN IN ('','Y')
	AND (CON.PC IN ('Y') OR CON.LW IN ('Y'))
	AND CON.DEL = 0
    AND CON.RL NOT IN ('23','99')
    AND CON.NP NOT IN (0)

where 1=1
AND astu.SC IN ({sc_str})

--AND CON.PID = 99191
--AND CON.EM IS NULL
--AND astu.PEM = ''
"""


q_abs_counts = """
WITH all_stu AS (
	SELECT 
	stu.ID, STU.LN, STU.FN, CONVERT(DATE, STU.BD) BD
	, stu.SC, loc.NM, stu.GR
	, stu.AD, stu.CY, stu.ST, stu.ZC
	, stu.TL
	, stu.CL
	, stu.PG, stu.PEM, stu.SEM
	, stu.SN
	, CASE
		WHEN STU.GR >= 6 THEN COUN.TE
		ELSE ''
	END AS counselor
	, stu.U11
	, EL.DE as ELL

	, stu.TG
	, stu.LD

	, stu.LF

	, stu.SP

	, stu.hl
	, stu.DA, stu.DE, stu.DP

	, ROW_NUMBER() OVER (PARTITION BY STU.ID 
		ORDER BY STU.DEL
		, CASE WHEN stu.LD IS NULL THEN 0 ELSE 1 END ASC
		, CASE WHEN stu.ED IS NULL THEN 0 ELSE 1 END DESC
		, stu.ED ASC
		, stu.TG
	) AS RN

	FROM STU
	LEFT JOIN LOC ON stu.SC = loc.CD
	LEFT JOIN TCH AS COUN ON STU.CU = COUN.TN AND STU.SC=COUN.SC
	LEFT JOIN COD EL ON EL.TC='STU' AND EL.FC='LF' AND EL.CD = STU.LF



	WHERE 
	1=1
	AND [STU].DEL = 0
	AND stu.TG IN ('')
	AND stu.gr not in('-2', '14', '17', '18')
	AND stu.sc in( 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 15, 16, 50, 60, 61, 62 )
	AND stu.sp not in('K', 'N')
	AND STU.ED <= '{date}'

)
, att_details AS (
	SELECT 
	STU.SC, STU.SN

	, SUM(
		CASE 
			WHEN 
				R.PERIOD = 'AL'
				AND (ABS.AD = 0)
				AND ABS.CD NOT IN ('N')
			THEN 1 
			ELSE 0 
		END
	) AS num_all_day_abs
	, COUNT(DISTINCT(DAY.DY)) as num_days_enr
	
	, SUM(
		CASE 
			WHEN 
				(DAY.BL LIKE '%'+CAST(R.PERIOD AS nvarchar)+'%' OR DAY.BL = '' )
				AND (ABS.AD = 0)
				AND ABS.CD NOT IN ('E','M')
			THEN 1 
			ELSE 0 
		END
	) AS num_class_abs


	--, day.dt, DAY.BL
	--, ABS.CD abscd, ABS.AD, ABS.LT
	--, r.*
	

	FROM STU
	JOIN DAY ON STU.SC = DAY.SC
    
    JOIN (
		SELECT

		ENR.SC, ENR.ID, enr.SN, ENR.ED, '{date}' AS LD

		FROM ENR
		WHERE 1=1
		AND ENR.LD IS NULL
		AND ENR.YR IN ( SELECT MAX(YR) FROM ENR WHERE DEL = 0)
		AND ENR.DEL = 0

		UNION

		SELECT

		ENR.SC, ENR.ID, enr.SN, ENR.ED, ENR.LD

		FROM ENR
		WHERE 1=1
		AND ENR.LD IS NOT NULL
		AND ENR.YR IN ( SELECT MAX(YR) FROM ENR WHERE DEL = 0)
		AND ENR.DEL = 0

	) ENR
		ON DAY.SC = ENR.SC
		AND STU.SN = ENR.SN
		AND DAY.DT <= ENR.LD
		AND DAY.DT >= ENR.ED
	
	LEFT JOIN (  SELECT  ATT.SC, ATT.SN, ATT.DT, ATT.CD, ATT.TN, 
					CONVERT(VARCHAR, ATT.AL) [AL], 
					CONVERT(VARCHAR, ATT.A1) [1],
					CONVERT(VARCHAR, ATT.A2) [2],
					CONVERT(VARCHAR, ATT.A3) [3],
					CONVERT(VARCHAR, ATT.A4) [4],
					CONVERT(VARCHAR, ATT.A5) [5],
					CONVERT(VARCHAR, ATT.A6) [6]

			FROM ATT 
			WHERE DEL = 0 ) PVT
	UNPIVOT
	(
		[ATTENDANCE] FOR [PERIOD] IN ( [AL] , 
		[1], [2], [3], [4], [5], [6] )
	) R ON STU.SC = R.SC AND R.SN = STU.SN
		AND R.DT = DAY.DT
	LEFT JOIN ABS ON R.SC = ABS.SC AND R.ATTENDANCE = ABS.CD
	--LEFT JOIN DAY ON R.SC = DAY.SC AND R.DT = DAY.DT




	WHERE 1=1
	--AND R.SC = 16 AND R.SN = 28348
	--AND DAY.DT <= '2024-01-30'

	--AND ABS.AD = 0 AND ABS.CD NOT IN ('E','M')

	--AND STU.SN = 27253
	AND DAY.HO = ''
	AND [STU].DEL = 0
	AND stu.TG IN ('')
	AND stu.sp not in('K', 'N')


	GROUP BY STU.SC, STU.SN

	--ORDER BY R.DT
)

select

astu.id, astu.gr, astu.ln, astu.fn
, astu.sn, astu.sc, CASE WHEN astu.cl IS NOT NULL AND astu.cl <> '' THEN astu.CL ELSE astu.hl END as cl
, ad.num_all_day_abs
, ad.num_class_abs

, ad.num_days_enr

, astu.DA, astu.DE, astu.DP


from all_stu astu
left join att_details ad ON astu.SC = ad.SC AND astu.SN = ad.SN


WHERE astu.SC IN ({sc_str})
AND ad.num_days_enr IS NOT NULL

--AND ad.num_class_abs >= 12

--AND astu.ID = 100751

ORDER BY ad.num_class_abs
"""


q_att_details = """
SELECT

STU.SC
,STU.ID

, DAY.DT

, ATT.AL

, ATT.A0
, CASE
	WHEN (DAY.BL LIKE '%0%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D0'

, ATT.A1
, CASE
	WHEN (DAY.BL LIKE '%1%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D1'

, ATT.A2
, CASE
	WHEN (DAY.BL LIKE '%2%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D2'

, ATT.A3
, CASE
	WHEN (DAY.BL LIKE '%3%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D3'

, ATT.A4
, CASE
	WHEN (DAY.BL LIKE '%4%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D4'

, ATT.A5
, CASE
	WHEN (DAY.BL LIKE '%5%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D5'

, ATT.A6
, CASE
	WHEN (DAY.BL LIKE '%6%' OR DAY.BL = '' ) THEN 'X'
	ELSE ''
END AS 'D6'



--,ATT.AL, ATT.DT, ATT.SC, ATT.SN

FROM STU

JOIN DAY ON STU.SC = DAY.SC
            
JOIN (
    SELECT

    ENR.SC, ENR.ID, enr.SN, MIN(ENR.ED) AS ED
    , CASE
        WHEN COUNT(*) = COUNT(ENR.LD) THEN MAX(ENR.LD)
        ELSE NULL
    END AS LD

    from enr
    WHERE 1=1
    AND ENR.YR IN ( SELECT MAX(YR) FROM ENR WHERE DEL = 0)

    --AND ENR.ID = 103547

    GROUP BY ENR.ID, ENR.SN, ENR.SC
) ENR
    ON DAY.SC = ENR.SC
    AND STU.SN = ENR.SN
    AND DAY.DT <= '{date}'  -- BEGINNING OF YEAR TO CURRENT DATE
    AND DAY.DT >= ENR.ED

LEFT JOIN ATT ON ATT.SC = STU.SC AND ATT.SN = STU.SN AND ATT.DY = DAY.DY
LEFT JOIN ABS on ATT.AL = ABS.CD AND ATT.SC = ABS.SC

WHERE 1=1
AND DAY.DT IN (
	SELECT TOP 5 

	DT

	FROM DAY

	WHERE SC = 16
	AND DAY.DT <= '{date}'
	AND HO = ''

	ORDER BY DT DESC
)
AND STU.SC IN ({sc_str})
--AND STU.ID = 102223
"""

q_days = """
SELECT TOP 5 

DT

FROM DAY

WHERE SC = 16
AND DAY.DT <= '{date}'
AND HO = ''

ORDER BY DT DESC
"""

q_abs_codes = """
select CD, TI

from abs

where sc IN ({sc_str})
AND DEL = 0

GROUP BY CD, TI

"""