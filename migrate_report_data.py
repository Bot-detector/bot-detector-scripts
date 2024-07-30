import asyncio
import os
import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqla
import csv

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


async def migrate_report_data(player_id: int, semaphore: asyncio.Semaphore):
    async with semaphore:
        print(semaphore._value, player_id)
        sql_insert_sighting = """
            INSERT INTO report_sighting (reporting_id, reported_id, manual_detect)
            SELECT DISTINCT r.reportingID , r.reportedID , IFNULL(r.manual_detect,0) from Reports r
            WHERE 1
                and r.reportingID = :player_id
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
                reporting_id = :player_id
            ;
        """
        params = {"player_id": player_id}
        async with Session() as session:
            session: AsyncSession
            async with session.begin():
                await session.execute(sqla.text(sql_insert_sighting), params=params)
                await session.execute(sqla.text(sql_update_migrated), params=params)
                await session.commit()
                print("migrated:", player_id)


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
            data = await session.execute(sqla.text(sql_select_migrated))
            result = data.mappings().all()
        return result


async def main():
    semaphore = asyncio.Semaphore(10)  # Limit the number of concurrent tasks
    sleep = 1
    while True:
        try:
            players = await select_players_to_migrate()
            tasks = []
            for player in players:
                player_id = player.get("player_id")
                tasks.append(
                    migrate_report_data(player_id=player_id, semaphore=semaphore)
                )
                # write_row(row=[player_id], file="./report_migration.csv")
            await asyncio.gather(*tasks)
            sleep = 1
        except Exception as e:
            print(e)
            await asyncio.sleep(sleep)
            sleep += 1
            continue
        if len(players) < 100:
            break


if __name__ == "__main__":
    asyncio.run(main())
