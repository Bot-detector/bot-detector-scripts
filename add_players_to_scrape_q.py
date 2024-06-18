import asyncio
import json
import logging
import os
import sys
from datetime import datetime

import dotenv
import sqlalchemy
import sqlalchemy.ext.asyncio as sqla
from AioKafkaEngine import ProducerEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)


# # log formatting
formatter = logging.Formatter(
    json.dumps(
        {
            "ts": "%(asctime)s",
            "name": "%(name)s",
            "function": "%(funcName)s",
            "level": "%(levelname)s",
            "msg": json.dumps("%(message)s"),
        }
    )
)

stream_handler = logging.StreamHandler(sys.stdout)

stream_handler.setFormatter(formatter)

handlers = [stream_handler]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)
logging.getLogger("aiokafka").setLevel(logging.WARNING)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None

engine = sqla.create_async_engine(connection_string, pool_size=100, max_overflow=10)
print("pool size", engine.pool.__sizeof__())
# engine.echo=True
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=True)


# Function to convert datetime objects to strings
def convert_datetime_to_str(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, datetime):
                obj[key] = value.isoformat()
            elif isinstance(value, dict):
                convert_datetime_to_str(value)
            elif isinstance(value, list):
                for item in value:
                    convert_datetime_to_str(item)
    elif isinstance(obj, list):
        for item in obj:
            convert_datetime_to_str(item)
    return obj


async def select_latest_player(session: AsyncSession) -> list[dict]:
    sql = """
        SELECT * from Players order by id desc limit 1;
    """
    result = await session.execute(sqlalchemy.text(sql))
    rows = result.fetchall()
    return [row._mapping for row in rows if row]


async def select_players(
    session: AsyncSession, player_id: int, limit: int = 10
) -> list[dict]:
    sql = """
        SELECT * from Players where id <= :player_id order by id desc limit :limit;
    """
    params = {
        "player_id": player_id,
        "limit": limit,
    }
    result = await session.execute(sqlalchemy.text(sql), params=params)
    rows = result.fetchall()
    return [dict(row._mapping) for row in rows if row]


async def main():
    producer = ProducerEngine(
        bootstrap_servers="localhost:9094", report_interval=30, queue_size=1000
    )
    counter = 0
    await producer.start_engine(topic="player")
    async with Session() as session:
        session: sqla.AsyncSession
        async with session.begin():
            latest_player = await select_latest_player(session=session)
            latest_player = latest_player[0]

            latest_pid = latest_player.get("id")
            while True:
                assert isinstance(latest_pid, int)
                print(f"{latest_pid=}, qsize={producer.send_queue.qsize()}, {counter=}")
                players = await select_players(
                    session=session, player_id=latest_pid, limit=1000
                )
                latest_pid = min([p.get("id") for p in players])
                # print(f"{players[0]=}")
                # print(f"{players[-1]=}")

                players = [
                    convert_datetime_to_str(p)
                    for p in players
                    if p.get("updated_at") is None
                ]
                counter += len(players)
                await asyncio.gather(*[producer.send_queue.put(p) for p in players])

                # latest_pid = players[-1].get("id")

                # if players[-1].get("updated_at") is not None:
                #     break
    while producer.send_queue.qsize() != 0:
        print(producer.send_queue.qsize())
        await asyncio.sleep(1)
    else:
        print(producer.send_queue.qsize())
        await producer.stop_engine()


if __name__ == "__main__":
    asyncio.run(main())
