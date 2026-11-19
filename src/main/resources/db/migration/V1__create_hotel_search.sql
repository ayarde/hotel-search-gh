create table hotel_search (
    search_id   varchar2(36 char)  not null,
    hotel_id    varchar2(36 char)  not null,
    check_in    date               not null,
    check_out   date               not null,
    ages        varchar2(600 char) not null,
    created_at  timestamp(6)       not null,
    constraint pk_hotel_search primary key (search_id),
    constraint ck_hotel_search_dates check (check_in < check_out)
);

-- ages queda fuera del índice: puede superar el largo máximo de clave. Su
-- igualdad se resuelve en el predicado de la consulta.
create index idx_hotel_search_match
    on hotel_search (hotel_id, check_in, check_out);
