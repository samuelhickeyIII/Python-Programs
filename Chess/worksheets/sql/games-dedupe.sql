-- select *
-- from (
--     SELECT 
--         row_number() over (PARTITION BY id_hash order by id_hash) as row_num
--     from games
-- )
-- where row_num = 1;

SELECT 
        row_number() over (PARTITION BY id_hash order by id_hash) as row_num
    from games;