@echo off
setlocal enabledelayedexpansion

REM Jump to repo root (parent of scripts\)
cd /d "%~dp0.."

echo Running ETL pipeline...

REM 1) Ingest P6 -> data\processed\schedule.parquet
python -m etl.p6_ingest --samples "data\samples" --out "data\processed" || goto :fail

REM 2) Compute EVM -> data\processed\evm_timeseries.parquet
python -m etl.evm_calculator --samples "data\samples" --processed "data\processed" || goto :fail

REM 3) Join procurement -> data\processed\procurement_impacts.parquet
python -m etl.procurement_join --samples "data\samples" --out "data\processed" || goto :fail

REM 4) Monte Carlo -> summary/runs/S-curves
python -m etl.monte_carlo --iters 5000 --seed 42 --processed "data\processed" --samples "data\samples" --outdir "data\processed" || goto :fail

echo ETL pipeline completed successfully.
exit /b 0

:fail
echo ETL pipeline failed.
exit /b 1
