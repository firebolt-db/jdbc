SET advanced_mode=1;
SET enable_create_table_v2=true;
SET enable_struct_syntax=true;
SET prevent_create_on_information_schema=true;
DROP TABLE IF EXISTS test_struct;
DROP TABLE IF EXISTS test_struct_helper;
CREATE TABLE IF NOT EXISTS test_struct(id int not null, s struct(a array(text) null, "b column" datetime null) not null);
CREATE TABLE IF NOT EXISTS test_struct_helper(a array(text) not null, "b column" datetime null);
