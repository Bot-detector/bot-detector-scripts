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
def write_row(row:list, file:str) -> None:
    with open(file, 'a') as csvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(row)

async def migrate_hs_data(player_id: int = 0, limit: int = 100):
    # create temp (staging) tables
    sql_create_temp_scrapes = """
        CREATE TEMPORARY TABLE temp_player
        SELECT 
            distinct sd.player_id 
        from scraper_data sd
        WHERE 1
            and sd.player_id > :player_id
        ORDER BY sd.player_id ASC 
        limit :limit
        ;
    """
    sql_select_latest_temp_scrapes = """
        SELECT * from temp_player
        order by player_id DESC 
        limit 1;
    """
    sql_create_temp_skills = """
        CREATE TEMPORARY TABLE temp_migration_skill(
            skill_id TINYINT, 
            skill_value INT, 
            scrape_ts DATETIME, 
            scrape_date DATE, 
            player_id INT
        ) ENGINE=MEMORY;
    """
    sql_create_temp_activities = """
        CREATE TEMPORARY TABLE temp_migration_activity (
            activity_id TINYINT, 
            activity_value INT, 
            scrape_ts DATETIME, 
            scrape_date DATE, 
            player_id INT
        ) ENGINE=MEMORY;
    """
    # insert into temp (staging) tables
    sql_insert_temp_skills = """
        INSERT INTO temp_migration_skill(skill_id, skill_value, scrape_ts, scrape_date, player_id) 
        SELECT
            s.skill_id,
            ps.skill_value,
            sd.created_at ,
            sd.record_date,
            sd.player_id
        FROM scraper_data sd
        JOIN temp_player tp ON sd.player_id = tp.player_id
        join player_skills ps ON sd.scraper_id  = ps.scraper_id
        JOIN skills ss on ps.skill_id = ss.skill_id 
        JOIN skill s on ss.skill_name = s.skill_name
        ;
    """
    sql_insert_temp_activities = """
        INSERT INTO temp_migration_activity (activity_id, activity_value, scrape_ts, scrape_date, player_id) 
        SELECT
            a.activity_id,
            pa.activity_value,
            sd.created_at,
            sd.record_date,
            sd.player_id
        FROM scraper_data sd
        JOIN temp_player tp ON sd.player_id = tp.player_id
        join player_activities pa ON sd.scraper_id  = pa.scraper_id
        JOIN activities aa on pa.activity_id = aa.activity_id 
        JOIN activity a on aa.activity_name = a.activity_name
        ;
    """
    # insert into the normalized tables
    sql_insert_pl_skill = """
        INSERT IGNORE INTO player_skill (skill_id, skill_value)
        SELECT DISTINCT skill_id, skill_value FROM temp_migration_skill tp
        WHERE NOT EXISTS (
            SELECT 1 FROM player_skill ps
            WHERE 1
                AND tp.skill_id = ps.skill_id
                AND tp.skill_value = ps.skill_value
        );
    """
    sql_insert_pl_activity = """
        INSERT IGNORE INTO player_activity (activity_id, activity_value)
        SELECT DISTINCT activity_id, activity_value FROM temp_migration_activity tp
        WHERE NOT EXISTS (
            SELECT 1 FROM player_activity pa
            WHERE 1
                AND tp.activity_id = pa.activity_id
                AND tp.activity_value = pa.activity_value
        );
    """

    sql_insert_sc_data = """
        INSERT IGNORE INTO scraper_data_v3 (scrape_ts, scrape_date, player_id)
        select DISTINCT scrape_ts, scrape_date, player_id from (
            SELECT scrape_ts, scrape_date, player_id FROM temp_migration_skill ts
            UNION
            SELECT scrape_ts, scrape_date, player_id FROM temp_migration_activity ta
        ) tp
        WHERE NOT EXISTS (
            SELECT 1 FROM scraper_data_v3 sd
            WHERE 1
                AND tp.scrape_date = sd.scrape_date
                AND tp.player_id = sd.player_id
        )
        ;
    """
    # insert into the joinging tables
    sql_insert_sc_pl_skill = """
        INSERT IGNORE INTO scraper_player_skill (scrape_id, player_skill_id)
        SELECT sd.scrape_id, ps.player_skill_id FROM temp_migration_skill tp
        join scraper_data_v3 sd ON (
            tp.scrape_date = sd.scrape_date AND 
            tp.player_id = sd.player_id
        )
        JOIN player_skill ps ON (
            tp.skill_id = ps.skill_id AND
            tp.skill_value = ps.skill_value
        )
        WHERE NOT EXISTS (
            SELECT 1 FROM scraper_player_skill sps
            WHERE 1
                AND sps.scrape_id = sd.scrape_id
                AND sps.player_skill_id = ps.player_skill_id
        );
    """
    sql_insert_sc_pl_activity = """
        INSERT IGNORE INTO scraper_player_activity (scrape_id, player_activity_id)
        SELECT sd.scrape_id, pa.player_activity_id FROM temp_migration_activity tp
        join scraper_data_v3 sd ON (
            tp.scrape_date = sd.scrape_date AND 
            tp.player_id = sd.player_id
        )
        JOIN player_activity pa ON (
            tp.activity_id = pa.activity_id AND
            tp.activity_value = pa.activity_value
        )
        WHERE NOT EXISTS (
            SELECT 1 FROM scraper_player_activity spa
            WHERE 1
                AND spa.scrape_id = sd.scrape_id
                AND spa.player_activity_id = pa.player_activity_id
        );
    """
    params = {"limit": limit, "player_id": player_id}
    async with Session() as session:
        session: AsyncSession
        async with session.begin():
            # cleanup
            await session.execute(sqla.text("DROP TABLE IF EXISTS temp_player"))
            await session.execute(
                sqla.text("DROP TABLE IF EXISTS temp_migration_skill")
            )
            await session.execute(
                sqla.text("DROP TABLE IF EXISTS temp_migration_activity")
            )
            # create temp (staging) tables
            await session.execute(
                sqla.text(sql_create_temp_scrapes),
                params=params
            )
            await session.execute(sqla.text(sql_create_temp_skills))
            await session.execute(sqla.text(sql_create_temp_activities))
            # insert into staging tables
            await session.execute(sqla.text(sql_insert_temp_skills))
            await session.execute(sqla.text(sql_insert_temp_activities))

            # report migration count
            count_skill = "select count(*) as cnt from temp_migration_skill"
            result = await session.execute(sqla.text(count_skill))
            print(f"migrating_skill: {result.mappings().first()}")

            count_activity = "select count(*) as cnt from temp_migration_activity"
            result = await session.execute(sqla.text(count_activity))
            print(f"migrating_activity: {result.mappings().first()}")

            # insert data into normalized table
            await session.execute(sqla.text(sql_insert_sc_data))
            await session.execute(sqla.text(sql_insert_pl_skill))
            await session.execute(sqla.text(sql_insert_pl_activity))

            # insert data into linking table
            await session.execute(sqla.text(sql_insert_sc_pl_skill))
            await session.execute(sqla.text(sql_insert_sc_pl_activity))

            # get latest player_id we migrated data for
            result = await session.execute(sqla.text(sql_select_latest_temp_scrapes))
            data = dict(result.mappings().first())

            # get number of players we migrated
            count_migrated = "select count(*) as cnt from temp_player"
            result = await session.execute(sqla.text(count_migrated))
            number_migrated = dict(result.mappings().first())
            # cleanup
            await session.execute(sqla.text("DROP TABLE IF EXISTS temp_player"))
            await session.execute(
                sqla.text("DROP TABLE IF EXISTS temp_migration_skill")
            )
            await session.execute(
                sqla.text("DROP TABLE IF EXISTS temp_migration_activity")
            )
            await session.commit()
        return data | number_migrated

async def main():
    player_id = 513111
    limit = 100
    sleep = 1
    while True:
        try:
            data = await migrate_hs_data(player_id=player_id, limit=limit)
            sleep = 1
        except Exception as e:
            print(e)
            await asyncio.sleep(sleep)
            sleep += 1
            continue
    
        print(data)
        player_id = data.get("player_id")
        assert player_id
        write_row(row=[player_id], file="./hs_migration.csv")

        count = data.get("cnt", 0)
        if count < limit:
            break


if __name__ == "__main__":
    asyncio.run(main())
