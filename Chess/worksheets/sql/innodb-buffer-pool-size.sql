-- SHOW GLOBAL STATUS
--    LIKE 'Innodb_buffer_pool_resize_status';

SET GLOBAL innodb_buffer_pool_size=(2 * 1024 * 1024 * 1024);
