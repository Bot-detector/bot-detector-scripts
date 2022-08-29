/*
    Generic data
*/
insert ignore into playerdata_dev.PredictionsFeedback
select pr.* from playerdata.PredictionsFeedback pr
join playerdata_dev.Players pl on pr.voter_id = pl.id or pr.subject_id=pl.id;

insert ignore into playerdata_dev.reportLatest
select rp.* from playerdata.reportLatest rp
join playerdata_dev.Players pl on rp.reported_id = pl.id;

insert ignore into playerdata_dev.Predictions
select pr.* from playerdata.Predictions pr
join playerdata_dev.Players pl on pr.id = pl.id;

ALTER TABLE playerdata_dev.Predictions CHANGE `name` `name` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL;
update playerdata_dev.Predictions set name=HEX(AES_ENCRYPT(name, "Victor_careers_onto_THE9_free0_endorser."));