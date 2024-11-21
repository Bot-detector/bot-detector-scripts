import asyncio
import os
import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

import time
import logging

# Setup basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Now the logs should be printed to the console when INFO or higher levels are logged

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None

engine = create_async_engine(connection_string, pool_size=1, max_overflow=10)
Session = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,  # Use AsyncSession for asynchronous operations
    autocommit=False,
    autoflush=False,
)


async def clear_output_tables(session: AsyncSession):
    """Clears the output tables before benchmarking."""
    clear_query = """
        DELETE FROM report_sighting;
        DELETE FROM report_gear;
        DELETE FROM report_location;
        DELETE FROM report;
    """
    await session.execute(text(clear_query))
    await session.commit()
    logging.info("Output tables cleared.")


async def process_reports_batch(session: AsyncSession, batch_size: int, start_id: int, end_id: int):
    done = False
    last_processed_id = start_id

    # Start the transaction
    async with session.begin():
        # Create the temporary table only once for the entire process
        create_temp_table_query = """
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
        """
        await session.execute(text(create_temp_table_query))

        while not done:
            # Populate the temp_batch with the current batch of data
            insert_query = text("""
                INSERT INTO temp_batch (ID, created_at, reportedID, reportingID, region_id, x_coord, y_coord, z_coord,
                                        timestamp, manual_detect, on_members_world, on_pvp_world, world_number,
                                        equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id, equip_boots_id,
                                        equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id, equip_ge_value)
                SELECT ID, created_at, reportedID, reportingID, region_id, x_coord, y_coord, z_coord,
                       timestamp, manual_detect, on_members_world, on_pvp_world, world_number,
                       equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id, equip_boots_id,
                       equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id, equip_ge_value
                FROM Reports
                WHERE ID > :last_processed_id AND ID <= :end_id
                ORDER BY ID ASC
                LIMIT :batch_size;
            """)
            result = await session.execute(insert_query, {"last_processed_id": last_processed_id, "end_id": end_id,
                                                          "batch_size": batch_size})

            # If no rows were inserted, we are done
            if result.rowcount == 0:
                done = True
            else:
                # Update the last_processed_id to the max ID in the batch
                max_id_query = text("SELECT MAX(ID) FROM temp_batch")
                result = await session.execute(max_id_query)
                last_processed_id = result.scalar()

            # Bulk Insert operations (using ON DUPLICATE KEY UPDATE to prevent conflicts)
            await session.execute(text("""
                INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
                SELECT DISTINCT reportingID, reportedID, manual_detect FROM temp_batch AS t
                ON DUPLICATE KEY UPDATE manual_detect = t.manual_detect;
            """))
            await session.execute(text("""
                INSERT INTO report_gear (equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                                         equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id)
                SELECT DISTINCT equip_head_id, equip_amulet_id, equip_torso_id, equip_legs_id,
                                equip_boots_id, equip_cape_id, equip_hands_id, equip_weapon_id, equip_shield_id
                FROM temp_batch AS t
                ON DUPLICATE KEY UPDATE equip_head_id = t.equip_head_id;
            """))
            await session.execute(text("""
                INSERT INTO report_location (region_id, x_coord, y_coord, z_coord)
                SELECT DISTINCT region_id, x_coord, y_coord, z_coord 
                FROM temp_batch AS t
                ON DUPLICATE KEY UPDATE region_id = t.region_id;
            """))

            # Update temp_batch with foreign keys from the inserted data
            await session.execute(text("""
                UPDATE temp_batch AS tb
                JOIN report_sighting AS rs
                ON tb.reportingID = rs.reporting_id AND tb.reportedID = rs.reported_id AND tb.manual_detect = rs.manual_detect
                SET tb.report_sighting_id = rs.report_sighting_id;
            """))
            await session.execute(text("""
                UPDATE temp_batch AS tb
                JOIN report_gear AS rg
                ON tb.equip_head_id = rg.equip_head_id AND tb.equip_amulet_id = rg.equip_amulet_id
                   AND tb.equip_torso_id = rg.equip_torso_id AND tb.equip_legs_id = rg.equip_legs_id
                   AND tb.equip_boots_id = rg.equip_boots_id AND tb.equip_cape_id = rg.equip_cape_id
                   AND tb.equip_hands_id = rg.equip_hands_id AND tb.equip_weapon_id = rg.equip_weapon_id
                   AND tb.equip_shield_id = rg.equip_shield_id
                SET tb.report_gear_id = rg.report_gear_id;
            """))
            await session.execute(text("""
                UPDATE temp_batch AS tb
                JOIN report_location AS rl
                ON tb.region_id = rl.region_id AND tb.x_coord = rl.x_coord
                   AND tb.y_coord = rl.y_coord AND tb.z_coord = rl.z_coord
                SET tb.report_location_id = rl.report_location_id;
            """))

            # Final insert into the report table
            await session.execute(text("""
                INSERT INTO report (report_sighting_id, report_location_id, report_gear_id, created_at, reported_at,
                                    on_members_world, on_pvp_world, world_number, region_id)
                SELECT DISTINCT report_sighting_id, report_location_id, report_gear_id, created_at, timestamp,
                                on_members_world, on_pvp_world, world_number, region_id
                FROM temp_batch AS t
                ON DUPLICATE KEY UPDATE report_sighting_id = t.report_sighting_id;
            """))

        # Drop the temporary table at the end
        await session.execute(text("DROP TEMPORARY TABLE IF EXISTS temp_batch;"))
        await session.commit()

    logging.info("Batch processing completed successfully.")


# Benchmarking function
async def benchmark_process_reports_batch(session: AsyncSession, batch_size: int, start_id: int, end_id: int):
    # Clear the output tables before benchmarking
    await clear_output_tables(session)

    # Start the timer for benchmarking
    start_time = time.time()

    # Process the reports batch
    await process_reports_batch(session, batch_size, start_id, end_id)

    # End the timer
    end_time = time.time()

    # Log the time taken
    elapsed_time = end_time - start_time
    logging.info(f"Batch processing took {elapsed_time} seconds.")


# Example of calling the function
async def main():
    async with Session() as session:
        # Run the benchmark with specific batch size and id range
        await benchmark_process_reports_batch(session, batch_size=1000, start_id=1, end_id=500_000)


if __name__ == "__main__":
    asyncio.run(main())
