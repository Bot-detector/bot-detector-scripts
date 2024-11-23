-- Step 1: Create temp_batch table
DROP TEMPORARY TABLE IF EXISTS temp_batch;

CREATE TEMPORARY TABLE IF NOT EXISTS temp_batch (
    ID BIGINT NOT NULL,
    created_at TIMESTAMP,
    reportedID INT,
    reportingID INT,
    region_id INT,
    x_coord INT,
    y_coord INT,
    z_coord INT,
    timestamp TIMESTAMP,
    manual_detect TINYINT,
    on_members_world INT,
    on_pvp_world TINYINT,
    world_number INT,
    equip_head_id INT,
    equip_amulet_id INT,
    equip_torso_id INT,
    equip_legs_id INT,
    equip_boots_id INT,
    equip_cape_id INT,
    equip_hands_id INT,
    equip_weapon_id INT,
    equip_shield_id INT,
    equip_ge_value BIGINT,
    report_sighting_id INT DEFAULT NULL,
    report_location_id INT DEFAULT NULL,
    report_gear_id INT DEFAULT NULL
);

-- Step 2: Insert data into temp_batch (manually adjust startId, endId, and batch_size as needed)

INSERT INTO temp_batch (ID, created_at, reportedID, reportingID, region_id, x_coord, y_coord, z_coord,
                        timestamp, manual_detect, on_members_world, on_pvp_world, world_number,
                        equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id, equip_boots_id,
                        equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id, equip_ge_value)
SELECT ID, created_at, reportedID, reportingID, region_id, x_coord, y_coord, z_coord,
       timestamp, manual_detect, on_members_world, on_pvp_world, world_number,
       equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id, equip_boots_id,
       equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id, equip_ge_value
FROM Reports
WHERE ID > :startId AND ID <= :endId -- Python Variables
ORDER BY ID ASC;

-- Step 3: Update temp_batch with existing foreign keys from report_sighting
UPDATE temp_batch AS tb
LEFT JOIN report_sighting AS rs
ON tb.reportingID = rs.reporting_id AND tb.reportedID = rs.reported_id AND tb.manual_detect = rs.manual_detect
SET tb.report_sighting_id = rs.report_sighting_id;



-- Debug: Check temp_batch after update
-- SELECT * FROM temp_batch WHERE report_sighting_id IS NULL;

-- Step 4: Insert missing rows into report_sighting
INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
SELECT DISTINCT reportingID, reportedID, manual_detect
FROM temp_batch
WHERE report_sighting_id IS NULL
ORDER BY reportingID, reportedID, manual_detect;

-- Debug: Check rows inserted into report_sighting
-- SELECT * FROM report_sighting ORDER BY report_sighting_id DESC LIMIT 10;

-- Step 5: Update temp_batch with newly inserted report_sighting IDs
UPDATE temp_batch AS tb
JOIN report_sighting AS rs
ON tb.reportingID = rs.reporting_id AND tb.reportedID = rs.reported_id AND tb.manual_detect = rs.manual_detect
SET tb.report_sighting_id = rs.report_sighting_id
WHERE tb.report_sighting_id IS NULL;

-- Debug: Verify report_sighting_id is updated
-- SELECT * FROM temp_batch;

-- Step 6: Insert missing rows into report_gear
INSERT INTO report_gear (equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                         equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id)
SELECT DISTINCT equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id
FROM temp_batch
WHERE report_gear_id IS NULL
ORDER BY equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id;

-- Debug: Check rows inserted into report_gear
-- SELECT * FROM report_gear ORDER BY report_gear_id DESC LIMIT 10;

-- Step 7: Update temp_batch with newly inserted report_gear IDs
UPDATE temp_batch AS tb
JOIN report_gear AS rg
ON tb.equip_head_id = rg.equip_head_id AND tb.equip_amulet_id = rg.equip_amulet_id
   AND tb.equip_torso_id = rg.equip_torso_id AND tb.equip_legs_id = rg.equip_legs_id
   AND tb.equip_boots_id = rg.equip_boots_id AND tb.equip_cape_id = rg.equip_cape_id
   AND tb.equip_hands_id = rg.equip_hands_id AND tb.equip_weapon_id = rg.equip_weapon_id
   AND tb.equip_shield_id = rg.equip_shield_id
SET tb.report_gear_id = rg.report_gear_id
WHERE tb.report_gear_id IS NULL;

-- Debug: Verify report_gear_id is updated
-- SELECT * FROM temp_batch;

-- Step 8: Insert missing rows into report_location
INSERT INTO report_location (region_id, x_coord, y_coord, z_coord)
SELECT DISTINCT region_id, x_coord, y_coord, z_coord
FROM temp_batch
WHERE report_location_id IS NULL
ORDER BY region_id, x_coord, y_coord, z_coord;

-- Debug: Check rows inserted into report_location
-- SELECT * FROM report_location ORDER BY report_location_id DESC LIMIT 10;

-- Step 9: Update temp_batch with newly inserted report_location IDs
UPDATE temp_batch AS tb
JOIN report_location AS rl
ON tb.region_id = rl.region_id AND tb.x_coord = rl.x_coord
   AND tb.y_coord = rl.y_coord AND tb.z_coord = rl.z_coord
SET tb.report_location_id = rl.report_location_id
WHERE tb.report_location_id IS NULL;

-- Debug: Verify report_location_id is updated
-- SELECT * FROM temp_batch;

-- Step 10: Final insert into the report table
INSERT IGNORE INTO report (report_sighting_id, report_location_id, report_gear_id, created_at, reported_at,
                           on_members_world, on_pvp_world, world_number, region_id)
SELECT DISTINCT report_sighting_id, report_location_id, report_gear_id, created_at, timestamp,
                on_members_world, on_pvp_world, world_number, region_id
FROM temp_batch;
