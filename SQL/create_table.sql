create table moscow_household_services
(
    id                             bigserial not null,
    global_id                      bigint,
    name                           varchar,
    is_net_object                  varchar,
    operating_company              varchar,
    type_service                   varchar,
    adm_area                       varchar,
    district                       varchar,
    address                        varchar,
    public_phone                   json,
    working_hours                  json,
    clarification_of_working_hours varchar,
    geo_data                       json,
    publication_date               date,
    version_number                 varchar,
    updated_at timestamp default current_timestamp
);
