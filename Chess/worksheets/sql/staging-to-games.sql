-- SET collation_server = 'utf8mb4_general_ci';

INSERT INTO GAMES (
        SELECT DISTINCT
            `EVENT`
            ,`SITE`
            ,`START_DATE`
            ,`ROUND`
            ,WHITE
            ,BLACK
            ,RESULT
            ,CURRENT_POSITION
            ,TIMEZONE
            ,ECO
            ,ECO_URL
            ,`UTC_DATE`
            ,`UTC_TIME`
            ,WHITE_ELO
            ,BLACK_ELO
            ,TIME_CONTROL
            ,TERMINATION
            ,START_TIME
            ,END_DATE
            ,END_TIME
            ,LINK
            ,MAINLINE
            ,TOURNAMENT
            ,VARIANT
            ,FEN
            ,SETUP
            ,`MATCH`
            ,md5(
                CONCAT(
                    NVL(`EVENT`, 'N/A')
                    ,NVL(`SITE`, 'N/A')
                    ,NVL(DATE_FORMAT(`START_DATE`, '%Y-%m-%d'), '9999-12-31')
                    ,NVL(`ROUND`, 'N/A')
                    ,NVL(WHITE, 'N/A')
                    ,NVL(BLACK, 'N/A')
                    ,NVL(RESULT, 'N/A')
                    ,NVL(CURRENT_POSITION, 'N/A')
                    ,NVL(TIMEZONE, 'N/A')
                    ,NVL(ECO, 'N/A')
                    ,NVL(ECO_URL, 'N/A')
                    ,NVL(DATE_FORMAT(`UTC_DATE`, '%Y-%m-%d'), '9999-12-31')
                    ,NVL(`UTC_TIME`, 'N/A')
                    ,NVL(WHITE_ELO, 'N/A')
                    ,NVL(BLACK_ELO, 'N/A')
                    ,NVL(TIME_CONTROL, 'N/A')
                    ,NVL(TERMINATION, 'N/A')
                    ,NVL(START_TIME, 'N/A')
                    ,NVL(DATE_FORMAT(END_DATE, '%Y-%m-%d'), '9999-12-31')
                    ,NVL(END_TIME, 'N/A')
                    ,NVL(LINK, 'N/A')
                    ,NVL(cast(MAINLINE as varchar(32000)), '')
                    ,NVL(TOURNAMENT, 'N/A')
                    ,NVL(`VARIANT`, 'N/A')
                    ,NVL(FEN, 'N/A')
                    ,NVL(SETUP, 'N/A')
                    ,NVL(`MATCH`, 'N/A')
                )
            ) AS ID_HASH
        FROM STAGING
)
;