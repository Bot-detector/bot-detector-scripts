import asyncio
import time
from migrate_report_data import reset_tables, migrate_selection, migrate_selection_where_not_exists


async def reset_and_time(func, *args, **kwargs):
    """
    Helper function to reset tables and measure execution time of a function.
    """
    await reset_tables()  # Reset tables before timing
    start_time = time.time()
    await func(*args, **kwargs)
    end_time = time.time()
    return end_time - start_time


async def test_migrate_selection():
    """
    Test migrate_selection and measure average time for 10 calls.
    """
    print("Testing migrate_selection...")
    total_time = 0
    for i in range(10):
        elapsed = await reset_and_time(migrate_selection, 1, 100_000)
        print(f"Run {i + 1}: {elapsed:.4f} seconds")
        total_time += elapsed
    avg_time = total_time / 10
    print(f"Average time for migrate_selection: {avg_time:.4f} seconds")


async def test_migrate_selection_where_not_exists():
    """
    Test migrate_selection_where_not_exists and measure average time for 10 calls.
    """
    print("\nTesting migrate_selection_where_not_exists...")
    total_time = 0
    for i in range(10):
        elapsed = await reset_and_time(migrate_selection_where_not_exists, 1, 100_000)
        print(f"Run {i + 1}: {elapsed:.4f} seconds")
        total_time += elapsed
    avg_time = total_time / 10
    print(f"Average time for migrate_selection_where_not_exists: {avg_time:.4f} seconds")


async def main():
    """
    Main function to run all tests sequentially.
    """
    await test_migrate_selection()
    await test_migrate_selection_where_not_exists()


if __name__ == "__main__":
    asyncio.run(main())
