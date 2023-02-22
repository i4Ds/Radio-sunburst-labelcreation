import cProfile

from load_data_into_database import main

prof = cProfile.Profile()
for chuck_size in [10, 100, 1000]:
    prof.enable()
    main(
        start_date="2023-02-19",
        end_date="2023-02-20",
        instrument_substring="None",
        chunk_size=chuck_size,
    )
    prof.disable()

    prof.print_stats(sort="time")
    prof.dump_stats(f"main_func_map_async_{chuck_size}.prof")
