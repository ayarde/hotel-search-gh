-- Usuario de la aplicación para los integration tests. La imagen oficial no
-- crea usuarios de aplicación; este script corre como sysdba al arrancar.
-- La variante lite no trae el tablespace USERS, por eso se usa el tablespace
-- por defecto de la PDB con el privilegio unlimited tablespace.
whenever sqlerror exit sql.sqlcode

alter session set container = freepdb1;

create user hotel_search identified by "hotel_search_it";

grant create session, create table, unlimited tablespace to hotel_search;
