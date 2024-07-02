# DE Challenge 1

## Partition Justification

To replicate the storage within an S3 folder, I formatted the file storage with a `report_date=` prefix which can be used as hive partitioning. Each report date will contain at least 144 files with the name formatted as `%H%M`, this will give ease of access to further consumers.

## Results Location

All results should be located within the `$DATA_DIR/challenge_1` folder set in the `.env` file
