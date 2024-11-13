import asyncio
import os

import dotenv
import sqlalchemy.ext.asyncio as sqla
from asyncio import Semaphore
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from collections import defaultdict

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None

engine = sqla.create_async_engine(connection_string, pool_size=100, max_overflow=10)
print("pool size", engine.pool.__sizeof__())
# engine.echo=True
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=True)

SKILLS = {
    "attack": 2,
    "defence": 3,
    "strength": 4,
    "hitpoints": 5,
    "ranged": 6,
    "prayer": 7,
    "magic": 8,
    "cooking": 9,
    "woodcutting": 10,
    "fletching": 11,
    "fishing": 12,
    "firemaking": 13,
    "crafting": 14,
    "smithing": 15,
    "mining": 16,
    "herblore": 17,
    "agility": 18,
    "thieving": 19,
    "slayer": 20,
    "farming": 21,
    "runecraft": 22,
    "hunter": 23,
    "construction": 24,
}

ACTIVITIES = {
    "abyssal_sire": 1,
    "alchemical_hydra": 2,
    "artio": 3,
    "barrows_chests": 4,
    "bounty_hunter_hunter": 5,
    "bounty_hunter_rogue": 6,
    "bryophyta": 7,
    "callisto": 8,
    "calvarion": 9,
    "cerberus": 10,
    "chambers_of_xeric": 11,
    "chambers_of_xeric_challenge_mode": 12,
    "chaos_elemental": 13,
    "chaos_fanatic": 14,
    "commander_zilyana": 15,
    "corporeal_beast": 16,
    "crazy_archaeologist": 17,
    "cs_all": 18,
    "cs_beginner": 19,
    "cs_easy": 20,
    "cs_elite": 21,
    "cs_hard": 22,
    "cs_master": 23,
    "cs_medium": 24,
    "dagannoth_prime": 25,
    "dagannoth_rex": 26,
    "dagannoth_supreme": 27,
    "deranged_archaeologist": 28,
    "duke_sucellus": 29,
    "general_graardor": 30,
    "giant_mole": 31,
    "grotesque_guardians": 32,
    "hespori": 33,
    "kalphite_queen": 34,
    "king_black_dragon": 35,
    "kraken": 36,
    "kreearra": 37,
    "kril_tsutsaroth": 38,
    "league": 39,
    "lms_rank": 40,
    "mimic": 41,
    "nex": 42,
    "nightmare": 43,
    "obor": 44,
    "phantom_muspah": 45,
    "phosanis_nightmare": 46,
    "rifts_closed": 47,
    "sarachnis": 48,
    "scorpia": 49,
    "skotizo": 50,
    "soul_wars_zeal": 51,
    "spindel": 52,
    "tempoross": 53,
    "the_corrupted_gauntlet": 54,
    "the_gauntlet": 55,
    "the_leviathan": 56,
    "the_whisperer": 57,
    "theatre_of_blood": 58,
    "theatre_of_blood_hard": 59,
    "thermonuclear_smoke_devil": 60,
    "tombs_of_amascut": 61,
    "tombs_of_amascut_expert": 62,
    "tzkal_zuk": 63,
    "tztok_jad": 64,
    "vardorvis": 65,
    "venenatis": 66,
    "vetion": 67,
    "vorkath": 68,
    "wintertodt": 69,
    "zalcano": 70,
    "zulrah": 71,
}


async def query_highscores(player_id: int, limit: int) -> dict:
    async with Session() as session:
        session: sqla.AsyncSession
        async with session.begin():
            sql = """
                SELECT phd.* FROM playerHiscoreData phd 
                join (select * from Players where id > :player_id and label_id != 0 LIMIT :limit) pl  on phd.Player_id=pl.id
            """
            # sql = """
            #     SELECT phd.* FROM playerHiscoreDataLatest phd 
            #     join (select * from Players where id > 1 and label_id !=0) pl  on phd.Player_id=pl.id
            #     LEFT  JOIN scraper_data_latest sdl on phd.Player_id =sdl.player_id 
            #     where 1=1
            #         and sdl.scraper_id is null
            #     limit :limit
            # """
            params = {"player_id": player_id, "limit": limit}
            result = await session.execute(sqlalchemy.text(sql), params=params)
            rows = result.fetchall()
            return [row._mapping for row in rows if row]


async def select_scraper_data(session: AsyncSession, sd: dict):
    sql_select_sd = """
        SELECT scraper_id from scraper_data where created_at = :created_at and player_id = :player_id;
    """
    result = await session.execute(sqlalchemy.text(sql_select_sd), params=sd)
    row = result.fetchone()
    return row


async def insert_scraper_data(session: AsyncSession, sd: dict):
    sql_insert_sd = """
        INSERT IGNORE INTO scraper_data (created_at, player_id)
        VALUES (:created_at, :player_id);
    """
    await session.execute(sqlalchemy.text(sql_insert_sd), params=sd)


async def insert_player_skills(session: AsyncSession, data: dict):
    sql_insert_ps = """
        INSERT IGNORE INTO player_skills (scraper_id, skill_id, skill_value)
        VALUES (:scraper_id, :skill_id, :skill_value);
    """
    await session.execute(sqlalchemy.text(sql_insert_ps), params=data)


async def insert_player_activities(session: AsyncSession, data: dict):
    sql_insert_pa = """
        INSERT IGNORE INTO player_activities (scraper_id, activity_id, activity_value)
        VALUES (:scraper_id, :activity_id, :activity_value);
    """
    await session.execute(sqlalchemy.text(sql_insert_pa), params=data)


async def query_insert(hs_data: list[dict], sm: Semaphore):
    async with Session() as session:
        session: sqla.AsyncSession
        async with session.begin():
            skills_data = []
            activity_data = []

            for hs in hs_data:
                scraper_data = {
                    "created_at": hs.get("timestamp"),
                    "player_id": hs.get("Player_id"),
                }
                row = await select_scraper_data(session=session, sd=scraper_data)

                if row:
                    # print("==========duplicate row==========")
                    continue

                await insert_scraper_data(session=session, sd=scraper_data)

                row = await select_scraper_data(session=session, sd=scraper_data)

                if not row:
                    # You may want to raise an exception or handle this case differently based on your requirements
                    # print("No scraper_id found for the given data:", scraper_data)
                    continue

                scraper_id = row[0]

                skills_data.extend(
                    [
                        {
                            "scraper_id": scraper_id,
                            "skill_id": SKILLS.get(k),
                            "skill_value": v,
                        }
                        for k, v in hs.items()
                        if k in SKILLS.keys()
                        if v
                        if v > 0
                    ]
                )

                activity_data.extend(
                    [
                        {
                            "scraper_id": scraper_id,
                            "activity_id": ACTIVITIES.get(k),
                            "activity_value": v,
                        }
                        for k, v in hs.items()
                        if k in ACTIVITIES.keys()
                        if v
                        if v > 0
                    ]
                )

            # insert the rest of the data
            if skills_data:
                await insert_player_skills(session=session, data=skills_data)
            if activity_data:
                await insert_player_activities(session=session, data=activity_data)
            
            print(scraper_data, len(skills_data), len(activity_data), sm._value)


async def task(semaphore: Semaphore, batch: list[dict]):
    async with semaphore:
        await query_insert(hs_data=batch, sm=semaphore)


async def main():
    # last player label_id !=0 : 122448237
    # 3625208
    player_id = 1  
    limit = 1000
    tasks = []
    semaphore = Semaphore(100)
    while True:
        print(f"next batch {player_id=}")
        hs_data: list[dict] = await query_highscores(player_id, limit)

        if hs_data is None:
            break

        batches = defaultdict(list)

        for hs in hs_data:
            player_id = hs.get("Player_id")
            batches[player_id].append(hs)

        # If you specifically need a list of lists, you can convert defaultdict to list of lists
        batches = list(batches.values())
        tasks = [task(semaphore, b) for b in batches]
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        player_id = hs_data[-1].get("Player_id")


if __name__ == "__main__":
    asyncio.run(main())
