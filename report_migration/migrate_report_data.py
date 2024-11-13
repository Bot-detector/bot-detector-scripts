import asyncio
import os
import dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sqla

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

async def main():
    # get a session
    async with Session() as session:
        session: AsyncSession
        # get a transaction
        async with session.begin():
            # do something
            pass
    
if __name__ == "__main__":
    asyncio.run(main())