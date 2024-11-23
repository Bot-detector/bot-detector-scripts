drop table if exists report;

drop table if exists report_gear;

drop table if exists report_location;

drop table if exists report_sighting;


CREATE TABLE `report_sighting` (
    `report_sighting_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `reporting_id` INT UNSIGNED NOT NULL,
    `reported_id` INT UNSIGNED NOT NULL,
    `manual_detect` TINYINT(1) DEFAULT 0,
    PRIMARY key (`report_sighting_id`),
    UNIQUE KEY unique_sighting (`reporting_id`, `reported_id`, `manual_detect`),
    KEY idx_reported_id (`reported_id`)
);

CREATE TABLE `report_gear` (
    `report_gear_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `equip_head_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_amulet_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_torso_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_legs_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_boots_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_cape_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_hands_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_weapon_id` SMALLINT UNSIGNED DEFAULT NULL,
    `equip_shield_id` SMALLINT UNSIGNED DEFAULT NULL,
    PRIMARY key (`report_gear_id`),
    UNIQUE KEY unique_gear (`equip_head_id`,`equip_amulet_id`,`equip_torso_id`,`equip_legs_id`,`equip_boots_id`,`equip_cape_id`,`equip_hands_id`,`equip_weapon_id`,`equip_shield_id`)
);
CREATE TABLE `report_location` (
    `report_location_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `region_id` MEDIUMINT UNSIGNED NOT NULL,
    `x_coord` MEDIUMINT UNSIGNED NOT NULL,
    `y_coord` MEDIUMINT UNSIGNED NOT NULL,
    `z_coord` MEDIUMINT UNSIGNED NOT NULL,
    PRIMARY key (`report_location_id`),
    UNIQUE KEY unique_location (`region_id`, `x_coord`, `y_coord`, `z_coord`)
);
CREATE TABLE `report` (
    `report_sighting_id` INT UNSIGNED NOT NULL,
    `report_location_id` INT UNSIGNED NOT NULL,
    `report_gear_id` INT UNSIGNED NOT NULL,
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `reported_at` timestamp NOT NULL,
    `on_members_world` TINYINT(1) DEFAULT NULL,
    `on_pvp_world` TINYINT(1) DEFAULT NULL,
    `world_number` SMALLINT UNSIGNED DEFAULT NULL,
    `region_id` MEDIUMINT UNSIGNED NOT NULL,
    PRIMARY key (`report_sighting_id`, `report_location_id`, `region_id`)
);
