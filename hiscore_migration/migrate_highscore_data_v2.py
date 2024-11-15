import asyncio
import os

import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqla
import time
from sqlalchemy.exc import OperationalError
import logging
import json

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)


# Configure JSON logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "ts": self.formatTime(record, self.datefmt),
            "lvl": record.levelname,
            "module": record.module,
            "funcName": record.funcName,
            "lineNo": record.lineno,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


class IgnoreSpecificWarnings(logging.Filter):
    def filter(self, record):
        # Return False to filter out messages containing "Unknown table"
        return "Unknown table" not in record.getMessage()


# Set up the logger
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.getLogger("asyncmy").addFilter(IgnoreSpecificWarnings())

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None


engine = create_async_engine(
    connection_string,
    pool_size=100,
    max_overflow=10,
    # echo=True,
)

Session = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,  # Use AsyncSession for asynchronous operations
    autocommit=False,
    autoflush=False,
)


async def get_players_to_migrate(player_id: int, limit: int):
    sql = """
        SELECT DISTINCT 
            player_id 
        FROM scraper_data 
        WHERE player_id > :player_id
        ORDER  BY player_id 
        LIMIT :limit
        ;
    """
    params = {"player_id": player_id, "limit": limit}
    async with Session() as session:
        session: AsyncSession
        async with session.begin():
            data = await session.execute(sqla.text(sql), params=params)
            result = data.mappings().all()
    return result


async def migrate(player_id: int):
    sql_create_temp_table = """
        CREATE TEMPORARY TABLE temp_hs_data (
            scraper_id BIGINT NOT NULL,
            player_id INT NOT NULL,
            scrape_ts DATETIME NOT NULL,
            scrape_date DATE NOT NULL,
            skills JSON,
            activities JSON
        );
    """

    sql_insert_temp_table = """
        INSERT INTO temp_hs_data (scraper_id, player_id, scrape_ts, scrape_date, skills, activities)
        SELECT
            sd.scraper_id,
            sd.player_id,
            sd.created_at as scrape_ts,
            sd.record_date as scrape_date,
            (
                SELECT
                    JSON_OBJECTAGG(
                        s.skill_name, ps.skill_value
                    ) AS skills
                FROM player_skills ps
                JOIN skills s ON ps.skill_id = s.skill_id
                WHERE ps.scraper_id = sd.scraper_id
                GROUP BY
                    sd.scraper_id
            ) as skills,
            (
            SELECT
                JSON_OBJECTAGG(
                    a.activity_name , pa.activity_value 
                ) AS activities
            FROM player_activities pa
            JOIN activities a ON pa.activity_id = a.activity_id
            WHERE pa.scraper_id = sd.scraper_id
            GROUP BY
                sd.scraper_id
            ) as activities
        FROM scraper_data sd
        WHERE 1=1
            and sd.player_id IN :player_id
        ;
    """

    sql_insert_table = """
        INSERT IGNORE INTO highscore_data (player_id, scrape_ts, scrape_date, skills, activities)
        SELECT player_id, scrape_ts, scrape_date, skills, activities FROM temp_hs_data thd
        WHERE NOT EXISTS (
            SELECT 1 FROM highscore_data hd
            WHERE 1
                AND thd.player_id = hd.player_id
                AND thd.scrape_date = hd.scrape_date
        );
    """

    sql_delete_data = """
        DELETE FROM scraper_data where scraper_id in (select scraper_id from temp_hs_data);
    """

    async with Session() as session:
        session: AsyncSession
        async with session.begin():
            await session.execute(sqla.text("DROP TABLE IF EXISTS temp_hs_data;"))
            await session.execute(sqla.text(sql_create_temp_table))
            await session.execute(
                sqla.text(sql_insert_temp_table), {"player_id": player_id}
            )
            await session.execute(sqla.text(sql_insert_table))
            await session.execute(sqla.text(sql_delete_data))
            result = await session.execute(
                sqla.text("select count(*) as cnt from temp_hs_data;")
            )
            cnt = result.mappings().all()
            await session.execute(sqla.text("DROP TABLE IF EXISTS temp_hs_data;"))

            await session.commit()
    return cnt


async def task_migrate(queue: asyncio.Queue, semaphore: asyncio.Semaphore):
    sleep = 1

    while True:
        if queue.empty():
            await asyncio.sleep(1)
            continue

        player_id = await queue.get()
        queue.task_done()

        async with semaphore:
            try:
                start_time = time.time()
                cnt = await migrate(player_id=player_id)
                delta = int(time.time() - start_time)
                logger.info(
                    f"[{player_id[0]}..{player_id[-1]}] l:{len(player_id)}, {delta} sec {cnt}"
                )
                sleep = 1
            except OperationalError as e:
                logger.error(
                    f"err: sleep: {sleep} [{player_id[0]}..{player_id[-1]}] l:{len(player_id)}, {e._message()}"
                )
                await asyncio.sleep(sleep)
                sleep = min(sleep * 2, 60)
                continue


async def task_get_players(
    queue: asyncio.Queue, player_id: int = 0, limit: int = 1000, batch_size: int = 100
):
    sleep = 1
    while True:
        logger.info(player_id)
        players = await get_players_to_migrate(player_id=player_id, limit=limit)

        if not players:
            logger.info(f"No players to migrate, sleeping {sleep} seconds.")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue

        players = [p["player_id"] for p in players]
        for i in range(0, len(players), batch_size):
            batch = players[i : i + batch_size]
            await queue.put(tuple(batch))

        player_id = players[-1]

        if len(players) < limit:
            logger.info("No players to migrate, sleeping 300 seconds.")
            await asyncio.sleep(300)

# 10 => 5 sec
async def main():
    player_id = 0
    batch_size = 100
    async_tasks = 1
    limit = 1000

    player_queue = asyncio.Queue(maxsize=25)
    # semaphore limits the number of async tasks
    semaphore = asyncio.Semaphore(value=async_tasks)

    get_players = asyncio.create_task(
        task_get_players(player_queue, player_id, limit, batch_size)
    )
    migration_tasks = [
        asyncio.create_task(task_migrate(player_queue, semaphore))
        for _ in range(semaphore._value)
    ]
    tasks = [get_players, *migration_tasks]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
