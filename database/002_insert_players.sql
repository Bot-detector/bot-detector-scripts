/*
    Insert players
*/
insert ignore into playerdata_dev.Labels 
select * from playerdata.Labels;

insert ignore into playerdata_dev.LabelJagex 
select * from playerdata.LabelJagex;

insert ignore into playerdata_dev.Players
select p.* from playerdata.Labels lb
join lateral (
    select 
        pl.*
    from playerdata.Players pl
    where lb.id = pl.label_id
    limit 5000
) p on (1=1)
;


/*
    Encrypt player names
*/
update playerdata_dev.Players 
set 
    name=HEX(AES_ENCRYPT(name, "Victor_careers_onto_THE9_free0_endorser.")), 
    normalized_name=HEX(AES_ENCRYPT(normalized_name, "Victor_careers_onto_THE9_free0_endorser."))
;
/*
    To decrypt the encryption: ES_DECRYPT(BINARY(UNHEX(setting_value)), 'key')
*/