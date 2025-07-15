import asyncio
import json
import logging
import os
from datetime import date, datetime
from typing import Optional

import dotenv
import orjson
import sqlalchemy as sqla
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError
from pydantic import BaseModel
from sqlalchemy import TextClause
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

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


class MetaData(BaseModel):
    version: int
    source: str


class PlayerStruct(BaseModel):
    id: int
    name: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    possible_ban: bool = False
    confirmed_ban: bool = False
    confirmed_player: bool = False
    label_id: int = 0
    label_jagex: int = 0
    ironman: Optional[bool] = None
    hardcore_ironman: Optional[bool] = None
    ultimate_ironman: Optional[bool] = None
    normalized_name: Optional[str] = None


class HighscoreBaseStruct(BaseModel):
    player_id: int
    scrape_date: date
    time_to_live: date
    skills: Optional[dict[str, int]] = None
    activities: Optional[dict[str, int]] = None


class ScrapedStruct(BaseModel):
    metadata: MetaData
    player_data: PlayerStruct
    highscore_data: HighscoreBaseStruct | None


# Set up the logger
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())

logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.getLogger("asyncmy").addFilter(IgnoreSpecificWarnings())

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_session_factory(
    connection_string: str,
) -> tuple[async_sessionmaker[AsyncSession], AsyncEngine]:
    async_engine = create_async_engine(
        connection_string,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=90,
        pool_timeout=30,
        pool_recycle=30,
        echo=False,
    )
    async_session = async_sessionmaker(
        bind=async_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    return async_session, async_engine


def _records_to_migrate() -> TextClause:
    sql = sqla.text("""
        SELECT 
            pl.id as player_id,
            pl.name as player_name,
            pl.created_at,
            pl.updated_at,
            pl.possible_ban,
            pl.confirmed_ban,
            pl.confirmed_player,
            pl.label_id,
            pl.label_jagex,
            sdv.scrape_date,
            (
                SELECT JSON_OBJECTAGG(s.skill_name, ps.skill_value)
                FROM scraper_player_skill sps
                JOIN player_skill ps ON sps.player_skill_id = ps.player_skill_id
                JOIN skill s ON ps.skill_id = s.skill_id
                WHERE sps.scrape_id = sdv.scrape_id
            ) AS skills,
            (
                SELECT JSON_OBJECTAGG(a.activity_name, pa.activity_value)
                FROM scraper_player_activity spa
                JOIN player_activity pa ON spa.player_activity_id = pa.player_activity_id
                JOIN activity a ON pa.activity_id = a.activity_id
                WHERE spa.scrape_id = sdv.scrape_id
            ) AS activities
        FROM scraper_data_v3 sdv
        join (select * from Players where id > :player_id and label_id = 0 limit :limit) pl on sdv.player_id = pl.id
        GROUP BY sdv.scrape_id
        ORDER BY pl.id asc;
    """)
    return sql


async def get_hiscore_data(
    session_factory: async_sessionmaker[AsyncSession],
    params: dict,
):
    logger.info(f"{params=}")
    async with session_factory() as session:
        async with session.begin():
            result = await session.execute(_records_to_migrate(), params)
            data = result.mappings().all()

    logger.info(f"Received: {len(data)}")
    return data


async def main():
    START_PLAYER_ID: int = 0
    LIMIT: int = 10_000
    PRODUCE_MESSAGES: bool = False

    connection_string: str | None = os.environ.get("sql_uri")
    if not connection_string:
        return

    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9094",
        value_serializer=lambda v: orjson.dumps(v),
        acks="all",
    )
    await producer.start()

    session_factory, engine = get_session_factory(connection_string=connection_string)
    params = {"player_id": START_PLAYER_ID, "limit": LIMIT}

    while True:
        try:
            data = await get_hiscore_data(
                session_factory=session_factory,
                params=params,
            )
        except OperationalError as e:
            logger.info(f"{params=}, {e=}")
            continue
        except Exception as e:
            logger.error(f"{params=}, {e=}")
            break

        if not data:
            logger.error("no data")
            break

        try:
            i = 0
            while i < len(data):
                d = data[i]
                payload = ScrapedStruct(
                    metadata=MetaData(version=1, source="migration"),
                    player_data=PlayerStruct(
                        id=d["player_id"],
                        name=d["player_name"],
                        created_at=d["created_at"],
                        updated_at=d["updated_at"],
                        possible_ban=d["possible_ban"],
                        confirmed_ban=d["confirmed_ban"],
                        confirmed_player=d["confirmed_player"],
                        label_id=d["label_id"],
                        label_jagex=d["label_jagex"],
                    ),
                    highscore_data=HighscoreBaseStruct(
                        player_id=d["player_id"],
                        scrape_date=d["scrape_date"],
                        time_to_live=d["scrape_date"],
                        skills=json.loads(d["skills"]) if d["skills"] else {},
                        activities=json.loads(d["activities"])
                        if d["activities"]
                        else {},
                    ),
                ).model_dump()

                # print(payload)
                # print("*" * 10)

                try:
                    if PRODUCE_MESSAGES:
                        await producer.send(topic="players.scraped", value=payload)
                    i += 1  # move to next message only on success
                except KafkaTimeoutError:
                    logger.warning(
                        f"KafkaTimeoutError on player_id={d['player_id']}, retrying..."
                    )
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Unexpected error on player_id={d['player_id']}: {e}")
                    raise  # exit outer loop
        except Exception as e:
            logger.error(f"{params=}, {e=}")
            break
        params["player_id"] = data[-1]["player_id"]
    await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
