import asyncio
import os
import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqla
from sqlalchemy.exc import OperationalError
import csv
import logging
import time
import sys
import datetime

logger = logging.getLogger(__name__)
# # log formatting
fmt = "t: %(asctime)s - n: %(name)s - fn: %(funcName)s - l: %(levelname)s - m: %(message)s"
fmt = "t: %(asctime)s - l: %(levelname)s - m: %(message)s"

formatter = logging.Formatter()
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
handlers = [stream_handler]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)

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
counter = 0


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
            and r.created_at < '2024-07-30'
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
    sql_combined = sql_insert_sighting + sql_update_migrated + "COMMIT;"
    params = {"player_id_list": tuple(player_id_list)}
    async with Session() as session:
        session: AsyncSession

        async with session.begin():
            await session.connection(
                execution_options={"isolation_level": "READ COMMITTED"}
            )
            # # Set innodb_lock_wait_timeout to a very low value (e.g., 1 second)
            await session.execute(sqla.text("SET SESSION innodb_lock_wait_timeout = 5"))

            # # Perform insert operation
            # await session.execute(sqla.text(sql_insert_sighting), params=params)

            # # Perform update operation
            # await session.execute(sqla.text(sql_update_migrated), params=params)

            # # Commit the transaction
            # await session.commit()
            await session.execute(sqla.text(sql_combined), params=params)


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
    try:
        async with Session() as session:
            session: AsyncSession
            async with session.begin():
                data = await session.execute(sqla.text(sql_select_migrated))
                result = data.mappings().all()
    except Exception as e:
        logger.error(f"Error in select_players_to_migrate: {e}")
        return []
    return result


async def create_batches(batch_size: int, batch_queue: asyncio.Queue):
    sleep = 1
    while True:
        try:
            players = await select_players_to_migrate()
            if not players:
                logger.info("No players to migrate, sleeping...")
                await asyncio.sleep(sleep)
                sleep = min(sleep * 2, 60)
            for i in range(0, len(players), batch_size):
                batch = players[i : i + batch_size]
                await batch_queue.put(batch)
            
            if len(players) < 100:
                await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Error in create_batches: {e}")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue


async def task_migrate(batch_queue: asyncio.Queue, semaphore: asyncio.Semaphore):
    global counter
    sleep = 1
    while True:
        if batch_queue.empty():
            await asyncio.sleep(1)
            continue
        try:
            players = await batch_queue.get()
            batch_queue.task_done()
            async with semaphore:

                if players:
                    _player_ids = [p["player_id"] for p in players]
                    logger.info(f"Started Migrating: {_player_ids}")
                    start = time.time()
                    await migrate_report_data(player_id_list=_player_ids)
                    counter += 1
                    delta = int(time.time() - start)
                    logger.info(f"Migrated: {_player_ids}, time: {delta}")
                
                sleep = 1
        except OperationalError as e:
            logger.warning(f"task_migrate: [{sleep}] {_player_ids} {e._message()}")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue
        except Exception as e:
            logger.error(f"task_migrate: [{sleep}] {_player_ids} {e}")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue


async def write_progress():
    global counter
    while True:
        now_epoch = int(time.time())
        now_dt = datetime.datetime.now()
        row = [now_epoch, now_dt, counter]
        write_row(row=row, file="./report_migration.csv")
        await asyncio.sleep(60)


async def main():
    batch_queue = asyncio.Queue(maxsize=10)
    semaphore = asyncio.Semaphore(100)  # Limit the number of concurrent tasks
    batch_size = 1

    # Start the batch creation task
    batch_task = asyncio.create_task(create_batches(batch_size, batch_queue))
    progress_task = asyncio.create_task(write_progress())

    # Start multiple migration tasks
    migration_tasks = [
        asyncio.create_task(task_migrate(batch_queue, semaphore))
        for _ in range(semaphore._value)
    ]

    tasks = [batch_task, progress_task, *migration_tasks]
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        # Clean up tasks
        batch_task.cancel()
        for task in migration_tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.dispose()


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
