insert ignore into playerdata_dev.playerLocations
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        plo.*
    from playerdata.playerLocations plo
    where plo.reported_id = pl.id
    limit 100
) a on (1=1)
;

insert ignore into playerdata_dev.playerLocationsDetail
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        pld.*
    from playerdata.playerLocationsDetail pld
    where pld.reported_id = pl.id
    limit 100
) a on (1=1)
;