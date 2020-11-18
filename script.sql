drop table if exists relation_r;
drop table if exists relation_s;

create table if not exists relation_r
(
    a
        text,
    b
        int

);


create table if not exists relation_s
(
    b int
        constraint relation_s_pk
            primary key,
    c text
);

