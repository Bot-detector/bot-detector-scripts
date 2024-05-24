import asyncio
import os
import dotenv
import sqlalchemy.ext.asyncio as sqla
from asyncio import Semaphore
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import csv

from datetime import datetime


dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)

# Establishing database connection
connection_string = os.environ.get("sql_uri")
assert connection_string is not None

engine = sqla.create_async_engine(connection_string, pool_size=100, max_overflow=10)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=True)

counter = 0
async def select_report(player_names: list):
    global counter
    while True:
        try:
            async with Session() as session:
                session: AsyncSession
                sql = """
                select 
                    pl.name,
                    rp.*
                from Reports rp
                join Players pl on rp.reportedID = pl.id
                where 1=1
                    and rp.created_at between '2023-11-04 20:08:55' and '2024-01-17 15:30:32'
                    and pl.name IN :player_names
                """
                result = await session.execute(sqlalchemy.text(sql), params={"player_names": tuple(player_names)})
                rows = result.fetchall()
                data = [row._mapping for row in rows if row]
                counter += 1
                now = datetime.now() # current date and time

                date_time = now.strftime("%Y%m%d-%H:%M:%S")
                print(f'{counter}, {date_time}, {len(player_names)} Players, {len(data)} rows exported')
                return data
        except Exception as e:
            print(player_names, e)
            await asyncio.sleep(5)
    

async def query_with_semaphore(semaphore: Semaphore, player_names: list):
    async with semaphore:
        return await select_report(player_names)

async def process_batch(players: list, semaphore: Semaphore):
    tasks = []
    for player_name_batch in batch_usernames(players, 10):
        task = asyncio.create_task(query_with_semaphore(semaphore, player_name_batch))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]

async def main():
    semaphore = Semaphore(25)
    batch_size = 500
    skip = 58
    
    with open('example_public_players.csv') as csvfile:
        reader = csv.reader(csvfile)
        players = [row[0] for row in reader]


    for i,batch in enumerate(batch_usernames(players, 100_000)):
        batch_results = await process_batch(batch, semaphore)
        write_to_csv(batch_results, f'reports/results_batch_{i}.csv')

def write_to_csv(data, filename):
    if data:
        print(f"writing: {len(data)}")
        keys = data[0].keys()
        
        # Ensure the directory exists
        directory = os.path.dirname(filename)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        
        with open(filename, 'w', newline='') as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)


def batch_usernames(usernames, batch_size=1000):
    # Create batches of usernames with each batch having up to batch_size usernames
    return [usernames[i:i + batch_size] for i in range(0, len(usernames), batch_size)]


if __name__ == "__main__":
    asyncio.run(main())