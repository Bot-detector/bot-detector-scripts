/*
    Insert players
*/
insert ignore into playerdata_dev.Labels 
select * from playerdata.Labels order by id desc;
update playerdata_dev.Labels set id=0  where label='Unknown';

insert ignore into playerdata_dev.LabelJagex 
select * from playerdata.LabelJagex;

insert ignore into playerdata_dev.Players
select p.* from playerdata.Labels lb
join lateral (
    select 
        pl.*
    from playerdata.Players pl
    where lb.id = pl.label_id
    limit 100
) p on (1=1)
;

/*
insert ignore into playerdata_dev.Players
select * from playerdata.Players pl 
where 1=1
    and pl.label_id = 0 -- unkown player
    and pl.id not in (select id from playerdata_dev.Players)
limit 100
;

insert ignore into playerdata_dev.Players
select * from playerdata.Players pl 
where 1=1
    and pl.label_id = 1 -- real player
    and pl.id not in (select id from playerdata_dev.Players)
limit 15000
;
*/

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