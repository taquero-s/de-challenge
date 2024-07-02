# DE Challenge

## Requirements

* Python >=3.11,<3,12
* Poetry >=1.6

## Setup

Run the dependency installation using:

```bash
make dev
```

Additionally, include a `.env` file on the root of the repository that includes a variable for the data directory. The contents of the file used for testing are:

```bash
export DATA_DIR=~/Data
```

## Run Stream

A streaming application was developed as part of the challenge's requirements. This application currently works as a side command and can be executed using the following command:

```bash
make hermes-run
```

After this command has been issued, a local EL process will start.

## Start Orchestrator

Orchestration is done using Dagster as engine. All schedules and jobs are managed within this framework. To start the engine, run the following command:

```bash
make orca-start
```

After this has been issued, a local server will run, this will be used to execute the ETL workflows.

## Challenge Solutions

Further description on the solution descriptions can be found in the docs:
* [Challenge 1](./docs/challenge_1.md)
* [Challenge 2](./docs/challenge_2.md)
