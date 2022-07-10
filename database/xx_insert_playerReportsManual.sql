insert ignore into playerdata_dev.playerReportsManual
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        prm.*
    from playerdata.playerReportsManual prm
    where prm.reporting_id = pl.id
    limit 100
) a on (1=1)
;

insert ignore into playerdata_dev.playerReportsManual
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        prm.*
    from playerdata.playerReportsManual prm
    where prm.reported_id = pl.id
    limit 100
) a on (1=1)
;