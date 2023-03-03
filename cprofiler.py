import cProfile
from pstats import Stats

from load_data_into_database import main

timing = {}
for chuck_size in [10, 100, 1000]:
    for cpu_count in [
        1,
        8,
        20,
    ]:
        prof = cProfile.Profile()
        prof.enable()
        main(
            start_date="2023-02-20",
            end_date="2023-02-20",
            instrument_substring="None",
            chunk_size=chuck_size,
            cpu_count=cpu_count,
        )
        prof.disable()

        prof.print_stats(sort="time")
        prof.dump_stats(f"main_func_{chuck_size}_{cpu_count}.prof")
        timing[(chuck_size, cpu_count)] = Stats(
            f"main_func_{chuck_size}_{cpu_count}.prof"
        ).total_tt


print(timing)
