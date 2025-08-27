insert ignore into playerdata_dev.playerHiscoreData
select phd.* from playerdata.playerHiscoreData phd
join playerdata_dev.Players pl on phd.Player_id = pl.id;

insert ignore into playerdata_dev.playerHiscoreDataLatest
select phd.* from playerdata.playerHiscoreDataLatest phd
join playerdata_dev.Players pl on phd.Player_id = pl.id;

insert ignore into playerdata_dev.playerHiscoreDataXPChange
select phd.* from playerdata.playerHiscoreDataXPChange phd 
join playerdata_dev.Players pl on phd.Player_id = pl.id;

