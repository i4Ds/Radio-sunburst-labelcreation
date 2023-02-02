# Database for E-Callisto Data
This repository contains code for the E-Callisto database. The database is a relational database that contains data from the E-Callisto project. 

## Database Structure
The database is structured as follows:
The database has one table per instrument.
- **<INSTRUMENT>** - Table containing data from the instrument with the corresponding frequencies as columns. The index is a timestamp.

## Rules on how the data is ingested
The data is ingested from the raw data files into the database with minimal processing. However, there are still steps done:
- `np.ma.masked` is converted before to an `np.array` with dtype `np.int32`.
- If an instrument has duplicate frequencies, the two frequencies are averanged (e.g. `indonesia_59`)
- If an instrument changes it frequencies, the new frequencies are added to the table and the others are filled up with nans. However, if an instrument has more than 1600 frequencies, the data is not ingested anymore and the import is skipped. If you want to change your frequencies, please rename your instrument and it will automatically be ingested.