insert ignore into playerdata_dev.Reports
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        plr.*
    from playerdata.Reports plr
    where plr.reportedid = pl.id
    limit 100
) a on (1=1)
;

insert ignore into playerdata_dev.Reports
select a.* from playerdata_dev.Players pl
join lateral (
    select 
        plr.*
    from playerdata.Reports plr
    where plr.reportingid = pl.id
    limit 100
) a on (1=1)
;