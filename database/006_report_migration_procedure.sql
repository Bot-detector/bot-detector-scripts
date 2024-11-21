DELIMITER $$

CREATE PROCEDURE ProcessReportsBatch(
    IN batch_size INT,
    IN startId INT,
    IN endId INT
)
BEGIN
    DECLARE last_processed_id BIGINT DEFAULT startId;
    DECLARE done INT DEFAULT FALSE;

    -- Drop any existing instance of temp_batch to ensure a fresh start
    DROP TEMPORARY TABLE IF EXISTS temp_batch;

    -- Create a temporary table to hold the batch results
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

    -- Loop through each batch
    read_loop: LOOP
        -- Clear the temporary table for the next batch
        TRUNCATE TABLE temp_batch;

        -- Populate temp_batch with the current batch of data
        INSERT INTO temp_batch (ID, created_at, reportedID, reportingID, region_id, x_coord, y_coord, z_coord,
                                timestamp, manual_detect, on_members_world, on_pvp_world, world_number,
                                equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id, equip_boots_id,
                                equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id, equip_ge_value)
        SELECT ID, created_at, reportedID, reportingID, region_id, x_coord, y_coord, z_coord,
               timestamp, manual_detect, on_members_world, on_pvp_world, world_number,
               equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id, equip_boots_id,
               equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id, equip_ge_value
        FROM Reports
        WHERE ID > last_processed_id AND ID <= endId
        ORDER BY ID ASC
        LIMIT batch_size;

        -- Check if the batch contains rows
        IF ROW_COUNT() = 0 THEN
            SET done = TRUE;
        ELSE
            -- Update the last processed ID
            SELECT MAX(ID) INTO last_processed_id FROM temp_batch;
        END IF;

        -- Exit the loop if no more rows to process
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Bulk Insert into report_sighting
        INSERT IGNORE INTO report_sighting (reporting_id, reported_id, manual_detect)
        SELECT DISTINCT reportingID, reportedID, manual_detect FROM temp_batch;

        -- Bulk Insert into report_gear
        INSERT IGNORE INTO report_gear (equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                                        equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id)
        SELECT DISTINCT equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                        equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id
        FROM temp_batch;

        -- Bulk Insert into report_location
        INSERT IGNORE INTO report_location (region_id, x_coord, y_coord, z_coord)
        SELECT DISTINCT region_id, x_coord, y_coord, z_coord FROM temp_batch;

        -- Update temp_batch with foreign keys
        UPDATE temp_batch AS tb
        JOIN report_sighting AS rs
        ON tb.reportingID = rs.reporting_id AND tb.reportedID = rs.reported_id AND tb.manual_detect = rs.manual_detect
        SET tb.report_sighting_id = rs.report_sighting_id;

        UPDATE temp_batch AS tb
        JOIN report_gear AS rg
        ON tb.equip_head_id = rg.equip_head_id AND tb.equip_amulet_id = rg.equip_amulet_id
           AND tb.equip_torso_id = rg.equip_torso_id AND tb.equip_legs_id = rg.equip_legs_id
           AND tb.equip_boots_id = rg.equip_boots_id AND tb.equip_cape_id = rg.equip_cape_id
           AND tb.equip_hands_id = rg.equip_hands_id AND tb.equip_weapon_id = rg.equip_weapon_id
           AND tb.equip_shield_id = rg.equip_shield_id
        SET tb.report_gear_id = rg.report_gear_id;

        UPDATE temp_batch AS tb
        JOIN report_location AS rl
        ON tb.region_id = rl.region_id AND tb.x_coord = rl.x_coord
           AND tb.y_coord = rl.y_coord AND tb.z_coord = rl.z_coord
        SET tb.report_location_id = rl.report_location_id;

        -- Final insert into the report table
        INSERT IGNORE INTO report (report_sighting_id, report_location_id, report_gear_id, created_at, reported_at,
                                   on_members_world, on_pvp_world, world_number, region_id)
        SELECT DISTINCT report_sighting_id, report_location_id, report_gear_id, created_at, timestamp,
                        on_members_world, on_pvp_world, world_number, region_id
        FROM temp_batch;

    END LOOP;

    -- Drop the temporary table at the end
    DROP TEMPORARY TABLE IF EXISTS temp_batch;

    -- Final debug message
    SELECT 'Procedure completed successfully' AS final_debug_message;
END$$

DELIMITER ;