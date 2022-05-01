insert ignore into apiPermissions (id, permission) values 
(1, 'verify_ban'),
(2, 'verify_players'),
(3, 'request_highscores')
;

insert ignore into apiUser (id, username, token, ratelimit, is_active) values 
(1, 'verify_ban', 'verify_ban', -1, 1),
(2, 'verify_players', 'verify_players', -1, 1),
(3, 'request_highscores', 'request_highscores', -1, 1)
;

insert ignore into apiUserPerms (user_id, permission_id) VALUES
(1,1),
(1,2),
(1,3),
(2,2),
(3,3)
;

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

update playerdata_dev.Players set name=AES_ENCRYPT(name, "Victor_careers_onto_THE9_free0_endorser."), normalized_name=AES_ENCRYPT(normalized_name, "Victor_careers_onto_THE9_free0_endorser.");

insert ignore into playerdata_dev.playerHiscoreData
select phd.* from playerdata.playerHiscoreData phd
join playerdata_dev.Players pl on phd.Player_id = pl.id;

insert ignore into playerdata_dev.playerHiscoreDataLatest
select phd.* from playerdata.playerHiscoreDataLatest phd
join playerdata_dev.Players pl on phd.Player_id = pl.id;

insert ignore into playerdata_dev.playerHiscoreDataXPChange
select phd.* from playerdata.playerHiscoreDataXPChange phd 
join playerdata_dev.Players pl on phd.Player_id = pl.id;

insert ignore into playerdata_dev.Predictions
select pr.* from playerdata.Predictions pr
join playerdata_dev.Players pl on pr.id = pl.id;

update playerdata_dev.Predictions set name=AES_ENCRYPT(name, "Victor_careers_onto_THE9_free0_endorser.");

insert ignore into playerdata_dev.PredictionsFeedback
select pr.* from playerdata.PredictionsFeedback pr
join playerdata_dev.Players pl on pr.voter_id = pl.id or pr.subject_id=pl.id;

insert ignore into playerdata_dev.reportLatest
select rp.* from playerdata.reportLatest rp
join playerdata_dev.Players pl on rp.reported_id = pl.id;

insert ignore into playerdata_dev.Reports
select r.* from playerdata_dev.Players pl 
join lateral (
	select 
    	rp.* 
    from playerdata.Reports rp
    where rp.reportingID = pl.id
	limit 100
) r on (1=1)
;

insert ignore into playerdata_dev.Reports
select r.* from playerdata_dev.Players pl 
join lateral (
	select 
    	rp.* 
    from playerdata.Reports rp
    where rp.reportedID = pl.id
	limit 100
) r on (1=1)
;
