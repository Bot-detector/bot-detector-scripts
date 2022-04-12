import asyncio
import os

import dotenv
import sqlalchemy.ext.asyncio as async_sql
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import collections
import csv
from typing import List
import pandas as pd 

dotenv.load_dotenv(dotenv.find_dotenv(), verbose=True)

connection_string = os.environ.get('sql_url')

engine = async_sql.create_async_engine(connection_string)
Session = sessionmaker(engine, class_=async_sql.AsyncSession, expire_on_commit=True)
file =  "./files/playerlocations.csv"

class sql_cursor:
    def __init__(self, rows):
        self.rows = rows

    def rows2dict(self) -> List[dict]:
        return self.rows.mappings().all()

    def rows2tuple(self) ->list[tuple]:
        Record = collections.namedtuple("Record", self.rows.keys())
        return [Record(*r) for r in self.rows.fetchall()]


def write_row(row:List, file:str) -> None:
    with open(file, 'a') as csvfile: 
        # creating a csv writer object 
        csvwriter = csv.writer(csvfile) 
        csvwriter.writerow(row)

async def execute_command(command) -> List:
    # outer context calls session.close() 
    async with Session() as session:
        # inner context calls session.commit(), if there were no exceptions
        async with session.begin():
            result = await session.execute(sqlalchemy.text(command))
    return result

def pandas_read(engine, sql:str, param:dict) -> pd.DataFrame:
    with engine.connect() as conn:
        data = pd.read_sql(sql, con=conn, params=param)
    return data

async def main():
    sql = (
        """
        select id from Players pl limit 10 
        """
    )
    players = pandas_read(engine, sql, param={})
    print(players)
    # commands = ['select * from Players limit 10']

    # results = await asyncio.gather(
    #     *[
    #         asyncio.tasks.create_task(execute_command(c)) for c in commands
    #     ]
    # )

    # results = [sql_cursor(r).rows2dict() for r in results]
    # print(results)

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())