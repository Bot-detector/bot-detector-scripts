insert ignore into playerdata_dev.playerReports
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        plr.*
    from playerdata.playerReports plr
    where plr.reported_id = pl.id
    limit 100
) a on (1=1)
;

insert ignore into playerdata_dev.playerReports
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        plr.*
    from playerdata.playerReports plr
    where plr.reporting_id = pl.id
    limit 100
) a on (1=1)
;