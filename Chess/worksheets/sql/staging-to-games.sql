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
            ,SHA2(
                    CONCAT(
                        WHITE
                        ,BLACK
                        ,RESULT
                        ,DATE_FORMAT(`UTC_DATE`, '%Y-%m-%d')
                        ,`UTC_TIME`
                    )
                ,'256'
            ) AS ID_HASH
        FROM STAGING
)
;