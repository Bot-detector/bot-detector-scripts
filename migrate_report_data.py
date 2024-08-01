import asyncio
import os
import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqla
import csv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None

engine = create_async_engine(connection_string, pool_size=100, max_overflow=10)
Session = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,  # Use AsyncSession for asynchronous operations
    autocommit=False,
    autoflush=False,
)


def write_row(row: list, file: str) -> None:
    with open(file, "a") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(row)


async def migrate_report_data(player_id_list: list):
    sql_insert_sighting = """
        INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
        SELECT DISTINCT r.reportingID , r.reportedID , IFNULL(r.manual_detect,0) from Reports r
        WHERE 1
            and r.reportingID IN :player_id_list
            AND NOT EXISTS (
                SELECT 1 FROM report_sighting rs
                WHERE 1
                    AND r.reportingID = rs.reporting_id
                    AND r.reportedID = rs.reported_id
                    AND IFNULL(r.manual_detect,0) = rs.manual_detect
        );
    """
    sql_update_migrated = """
        UPDATE report_migrated
        SET
            migrated = 1
        WHERE 
            reporting_id IN :player_id_list
        ;
    """
    params = {"player_id_list": tuple(player_id_list)}
    async with Session() as session:
        session: AsyncSession
        async with session.begin():
            await session.execute(sqla.text(sql_insert_sighting), params=params)
            await session.execute(sqla.text(sql_update_migrated), params=params)
            await session.commit()


async def select_players_to_migrate():
    sql_select_migrated = """
        SELECT 
            rm.reporting_id as player_id 
        FROM report_migrated rm
        WHERE
            rm.migrated != 1
        limit 100
        ;
    """
    async with Session() as session:
        session: AsyncSession
        async with session.begin():
            try:
                data = await session.execute(sqla.text(sql_select_migrated))
                result = data.mappings().all()
                return result
            except Exception as e:
                logger.error(f"Error in select_players_to_migrate: {e}")
                return []


async def create_batches(batch_size: int, batch_queue: asyncio.Queue):
    sleep = 1
    while True:
        try:
            players = await select_players_to_migrate()
            if players:
                for i in range(0, len(players), batch_size):
                    batch = players[i : i + batch_size]
                    await batch_queue.put(batch)
                sleep = 1
            else:
                logger.info("No players to migrate, sleeping...")
                await asyncio.sleep(sleep)
                sleep = min(sleep * 2, 60)
        except Exception as e:
            logger.error(f"Error in create_batches: {e}")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue


async def task_migrate(batch_queue: asyncio.Queue, semaphore: asyncio.Semaphore):
    sleep = 1
    while True:
        try:
            async with semaphore:
                players = await batch_queue.get()
                if players:
                    _player_ids = [p["player_id"] for p in players]
                    logger.info(f"Started Migrating: {_player_ids}")
                    await migrate_report_data(player_id_list=_player_ids)
                    logger.info(f"Migrated: {_player_ids}")
                batch_queue.task_done()
                sleep = 1
        except Exception as e:
            logger.error(f"Error in task_migrate: {e}")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue


async def main():
    batch_queue = asyncio.Queue(maxsize=10)
    semaphore = asyncio.Semaphore(20)  # Limit the number of concurrent tasks
    batch_size = 1

    # Start the batch creation task
    batch_task = asyncio.create_task(create_batches(batch_size, batch_queue))

    # Start multiple migration tasks
    migration_tasks = [
        asyncio.create_task(task_migrate(batch_queue, semaphore))
        for _ in range(semaphore._value)
    ]

    try:
        await asyncio.gather(batch_task, *migration_tasks)
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        # Clean up tasks
        batch_task.cancel()
        for task in migration_tasks:
            task.cancel()
        await asyncio.gather(batch_task, *migration_tasks, return_exceptions=True)
        await engine.dispose()


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
