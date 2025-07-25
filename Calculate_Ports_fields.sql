BEGIN
    SET NOCOUNT ON;

    -- Update PORT_SIZE based on ARRIVE/EXPECTED counts (only if current size is 'S')
    UPDATE P
    SET P.PORT_SIZE = 
        CASE 
            WHEN EXPECTEDCOUNT <= 1 AND ARRIVECOUNT <= 5 THEN 'S'
            WHEN EXPECTEDCOUNT <= 10 AND ARRIVECOUNT <= 20 THEN 'M'
            ELSE 'L'
        END
    FROM ais.dbo.PORTS AS P
    INNER JOIN PORTS_CURRENT_BATCH B ON P.PORT_ID = B.PORT_ID
    WHERE P.CONFIRMED = 1 AND P.PORT_SIZE = 'S';

    -- Update COVERAGE only for ports that are missing it, based on latest terrestrial timestamp
    UPDATE P
    SET COVERAGE = LATEST.LatestTimestamp
    FROM PORTS P
    JOIN (
        SELECT PORT_ID, MAX(TIMESTAMP) AS LatestTimestamp
        FROM PORT_MOVES
        WHERE SAT = 0 AND TIMESTAMP IS NOT NULL
        GROUP BY PORT_ID
    ) LATEST ON P.PORT_ID = LATEST.PORT_ID
    WHERE P.COVERAGE IS NULL;

	-- Update AREA_CODE where it's missing by finding intersecting area with highest ZOOM
	UPDATE PORTS
	SET AREA_CODE = (
		SELECT TOP 1 AREA_CODE
		FROM AREAS WITH (INDEX(POLYGON))
		WHERE AREAS.POLYGON.STIntersects(PORTS.POLYGON) = 1
		  AND AREA_CODE IS NOT NULL
		ORDER BY ZOOM DESC
	)
	WHERE AREA_CODE IS NULL;

	-- Update AREA_ID where it's missing by finding intersecting area with highest ZOOM
	UPDATE PORTS
	SET AREA_ID = (
		SELECT TOP 1 AREA_ID
		FROM AREAS WITH (INDEX(POLYGON))
		WHERE AREAS.POLYGON.STIntersects(PORTS.POLYGON) = 1
		ORDER BY ZOOM DESC
	)
	WHERE AREA_ID IS NULL;

END;