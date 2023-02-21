import cProfile

from load_data_into_database import main

prof = cProfile.Profile()
prof.enable()
main(start_date="2023-02-20", end_date="2023-02-20", instrument_substring="None")
prof.disable()

prof.print_stats(sort="time")
prof.dump_stats("main_func.prof")
