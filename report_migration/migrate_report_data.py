import asyncio
import os
import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.exc import OperationalError
import logging
import json
import time

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)


# Configure JSON logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "ts": self.formatTime(record, self.datefmt),
            "lvl": record.levelname,
            "module": record.module,
            "funcName": record.funcName,
            # "lineNo": record.lineno,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


class IgnoreSQLWarnings(logging.Filter):
    def filter(self, record):
        ignore_messages = [
            "Unknown table",
            "Duplicate entry",
            "Out of range value for column",
        ]
        # Check if any of the ignore messages are in the log record message
        if any(msg in record.getMessage() for msg in ignore_messages):
            return False  # Don't log
        return True  # Log


# Set up the logger
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.getLogger("asyncmy").addFilter(IgnoreSQLWarnings())

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None

engine = create_async_engine(
    connection_string,
    pool_size=100,
    max_overflow=10,
)
Session = sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,  # Use AsyncSession for asynchronous operations
    autocommit=False,
    autoflush=False,
)


async def get_players_to_migrate(player_id: int, limit: int = 100):
    sql_select_migrated = """
        select distinct 
            r.reportingID as player_id
        from Reports r
        where r.reportingID > :player_id
        limit :limit
        ;
    """
    params = {"player_id": player_id, "limit": limit}

    async with Session() as session:
        session: AsyncSession
        async with session.begin():
            sql = TextClause(sql_select_migrated)
            data = await session.execute(sql, params=params)
            result = data.mappings().all()
    return result


def _create_temp_report() -> TextClause:
    return TextClause(
        """
        CREATE TEMPORARY TABLE temp_report (
            id BIGINT,
            created_at timestamp,
            /*sighting*/
            reporting_id INT,
            reported_id INT,
            manual_detect TINYINT DEFAULT 0,
            /*gear*/
            `equip_head_id` SMALLINT UNSIGNED,
            `equip_amulet_id` SMALLINT UNSIGNED,
            `equip_torso_id` SMALLINT UNSIGNED,
            `equip_legs_id` SMALLINT UNSIGNED,
            `equip_boots_id` SMALLINT UNSIGNED,
            `equip_cape_id` SMALLINT UNSIGNED,
            `equip_hands_id` SMALLINT UNSIGNED,
            `equip_weapon_id` SMALLINT UNSIGNED,
            `equip_shield_id` SMALLINT UNSIGNED,
            /*location*/
            `region_id` MEDIUMINT UNSIGNED NOT NULL,
            `x_coord` MEDIUMINT UNSIGNED NOT NULL,
            `y_coord` MEDIUMINT UNSIGNED NOT NULL,
            `z_coord` MEDIUMINT UNSIGNED NOT NULL,
            /*report*/
            `reported_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `on_members_world` TINYINT DEFAULT NULL,
            `on_pvp_world` TINYINT DEFAULT NULL,
            `world_number` SMALLINT UNSIGNED DEFAULT NULL
        ) ENGINE=MEMORY;
    """
    )


def _insert_temp_report() -> TextClause:
    return TextClause(
        """
        INSERT ignore INTO temp_report (
            id,
            created_at,
            /*sighting*/
            reporting_id,
            reported_id,
            manual_detect,
            /*gear*/
            equip_head_id,
            equip_amulet_id,
            equip_torso_id,
            equip_legs_id,
            equip_boots_id,
            equip_cape_id,
            equip_hands_id,
            equip_weapon_id,
            equip_shield_id,
            /*location*/
            region_id,
            x_coord,
            y_coord,
            z_coord,
            /*report*/
            reported_at,
            on_members_world,
            on_pvp_world,
            world_number
        )
        SELECT
            r.ID,
            r.created_at,
            /*sighting*/
            r.reportingID , 
            r.reportedID , 
            IFNULL(r.manual_detect,0),
            /*gear*/
            r.equip_head_id,
            r.equip_amulet_id,
            r.equip_torso_id,
            r.equip_legs_id,
            r.equip_boots_id,
            r.equip_cape_id,
            r.equip_hands_id,
            r.equip_weapon_id,
            r.equip_shield_id,
            /*location*/
            region_id,
            x_coord,
            y_coord,
            z_coord,
            /*report*/
            r.timestamp,
            r.on_members_world,
            r.on_pvp_world,
            r.world_number
        FROM Reports r
        WHERE 1=1
            and r.reportingID IN :player_ids
    """
    )


def _insert_sighting() -> TextClause:
    return TextClause(
        """
        INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
        SELECT DISTINCT tr.reporting_id, tr.reported_id, tr.manual_detect FROM temp_report tr
        WHERE NOT EXISTS (
            SELECT 1 FROM report_sighting rs
            WHERE 1
                AND tr.reporting_id = rs.reporting_id
                AND tr.reported_id = rs.reported_id
                AND tr.manual_detect = rs.manual_detect
        );
    """
    )


def _insert_gear() -> TextClause:
    return TextClause(
        """
        INSERT INTO report_gear (
            equip_head_id,
            equip_amulet_id,
            equip_torso_id,
            equip_legs_id,
            equip_boots_id,
            equip_cape_id,
            equip_hands_id,
            equip_weapon_id,
            equip_shield_id
        )
        SELECT DISTINCT
            tr.equip_head_id,
            tr.equip_amulet_id,
            tr.equip_torso_id,
            tr.equip_legs_id,
            tr.equip_boots_id,
            tr.equip_cape_id,
            tr.equip_hands_id,
            tr.equip_weapon_id,
            tr.equip_shield_id
        FROM temp_report tr
        WHERE NOT EXISTS (
            SELECT 1 FROM report_gear rg
            WHERE tr.equip_head_id = rg.equip_head_id
            AND tr.equip_amulet_id = rg.equip_amulet_id
            AND tr.equip_torso_id = rg.equip_torso_id
            AND tr.equip_legs_id = rg.equip_legs_id
            AND tr.equip_boots_id = rg.equip_boots_id
            AND tr.equip_cape_id = rg.equip_cape_id
            AND tr.equip_hands_id = rg.equip_hands_id
            AND tr.equip_weapon_id = rg.equip_weapon_id
            AND tr.equip_shield_id = rg.equip_shield_id
        );
    """
    )


def _insert_location() -> TextClause:
    return TextClause(
        """
        INSERT INTO report_location (region_id, x_coord, y_coord, z_coord)
        SELECT DISTINCT tr.region_id, tr.x_coord, tr.y_coord, tr.z_coord FROM temp_report tr
        WHERE NOT EXISTS (
            SELECT 1 FROM report_location rl
            WHERE 1
                AND tr.region_id = rl.region_id
                AND tr.x_coord = rl.x_coord
                AND tr.y_coord = rl.y_coord
                AND tr.z_coord = rl.z_coord
        );
        """
    )


def _insert_report() -> TextClause:
    return TextClause(
        """
            INSERT IGNORE INTO report (
                report_sighting_id,
                report_location_id,
                report_gear_id,
                reported_at,
                on_members_world,
                on_pvp_world,
                world_number,
                region_id
            )
            SELECT DISTINCT
                rs.report_sighting_id,
                rl.report_location_id,
                rg.report_gear_id,
                tr.reported_at,
                tr.on_members_world,
                tr.on_pvp_world,
                tr.world_number,
                tr.region_id
            FROM temp_report tr
            JOIN report_sighting rs
                ON rs.reporting_id = tr.reporting_id
                AND rs.reported_id = tr.reported_id
            JOIN report_location rl
                ON rl.region_id = tr.region_id
                AND rl.x_coord = tr.x_coord
                AND rl.y_coord = tr.y_coord
                AND rl.z_coord = tr.z_coord
            JOIN report_gear rg
                ON rg.equip_head_id = tr.equip_head_id
                AND rg.equip_amulet_id = tr.equip_amulet_id
                AND rg.equip_torso_id = tr.equip_torso_id
                AND rg.equip_legs_id = tr.equip_legs_id
                AND rg.equip_boots_id = tr.equip_boots_id
                AND rg.equip_cape_id = tr.equip_cape_id
                AND rg.equip_hands_id = tr.equip_hands_id
                AND rg.equip_weapon_id = tr.equip_weapon_id
                AND rg.equip_shield_id = tr.equip_shield_id
            WHERE NOT EXISTS (
                SELECT 1 FROM report rp
                WHERE 1
                    AND rs.report_sighting_id = rp.report_sighting_id
                    AND rl.report_location_id = rp.report_location_id
                    AND tr.region_id = rp.region_id
            )
            ;
        """
    )


def _delete_reports() -> TextClause:
    return TextClause(
        """
        DELETE FROM Reports
        WHERE ID in (
            SELECT tr.ID FROM temp_report tr
        );
        """
    )


async def migrate(player_ids_to_migrate: list[int]) -> None:
    sql_create_temp_report = _create_temp_report()
    sql_insert_temp_report = _insert_temp_report()
    sql_insert_sighting = _insert_sighting()
    sql_insert_gear = _insert_gear()
    sql_insert_location = _insert_location()
    sql_insert_report = _insert_report()
    sql_delete_reports = _delete_reports()

    params = {"player_ids": player_ids_to_migrate}

    async with Session() as session:
        session: AsyncSession

        async with session.begin():
            await session.execute(TextClause("DROP TABLE IF EXISTS temp_report;"))
            await session.execute(sql_create_temp_report)
            await session.execute(sql_insert_temp_report, params=params)
            await session.execute(sql_insert_sighting)
            await session.execute(sql_insert_gear)
            await session.execute(sql_insert_location)
            await session.execute(sql_insert_report)
            await session.execute(sql_delete_reports)
            result = await session.execute(
                TextClause("select count(*) as cnt from temp_report;")
            )
            cnt = result.mappings().all()
            await session.execute(TextClause("DROP TABLE IF EXISTS temp_report;"))
            await session.commit()
    return cnt


async def task_migrate(batch_queue: asyncio.Queue, semaphore: asyncio.Semaphore):
    sleep = 1
    while True:
        if batch_queue.empty():
            await asyncio.sleep(1)
            continue
        try:
            players = await batch_queue.get()
            batch_queue.task_done()

            # if for some reason players is empty
            if not players:
                continue

            # check if players is list of int
            assert all(isinstance(item, int) for item in players)

            async with semaphore:
                start = time.time()
                cnt = await migrate(player_ids_to_migrate=players)
                delta = int(time.time() - start)

                logger.info(
                    f"[{players[0]}..{players[-1]}]"
                    f"l:{len(players)}, {delta} sec {cnt}"
                )
                sleep = 1
        except OperationalError as e:
            logger.error(
                f"err: sleep: {sleep} [{players[0]}..{players[-1]}]"
                f"l:{len(players)}, {e._message()}"
            )
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue


async def create_batches(
    batch_size: int,
    batch_queue: asyncio.Queue,
    player_id: int = 0,
    limit: int = 100,
):
    sleep = 1
    while True:
        try:
            players = await get_players_to_migrate(
                player_id=player_id,
                limit=limit,
            )

            if not players:
                logger.info("No players to migrate, sleeping...")
                await asyncio.sleep(sleep)
                sleep = min(sleep * 2, 60)
                continue

            players = [p["player_id"] for p in players]
            for i in range(0, len(players), batch_size):
                batch = players[i : i + batch_size]
                await batch_queue.put(tuple(batch))

            player_id = players[-1]

            if len(players) < limit:
                await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"Error in create_batches: {e}")
            await asyncio.sleep(sleep)
            sleep = min(sleep * 2, 60)
            continue


async def main():
    batch_queue = asyncio.Queue(maxsize=10)
    semaphore = asyncio.Semaphore(1)
    batch_size = 1
    player_id = 0

    # Start the batch creation task
    batch_task = asyncio.create_task(
        create_batches(
            batch_size,
            batch_queue,
            player_id=player_id,
            limit=100,
        )
    )

    migration_tasks = [
        asyncio.create_task(task_migrate(batch_queue, semaphore))
        for _ in range(semaphore._value)
    ]

    tasks = [batch_task, *migration_tasks]
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


if __name__ == "__main__":
    asyncio.run(main())
