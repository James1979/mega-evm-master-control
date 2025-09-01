@echo off
echo Running ETL pipeline...
python -m etl.p6_ingest --samples data/samples --out data/processed || goto :error
python -m etl.evm_calculator --samples data/samples --out data/processed || goto :error
python -m etl.procurement_join --samples data/samples --out data/processed || goto :error
python -m etl.monte_carlo --iters 5000 --seed 42 --processed data/processed --samples data/samples --outdir data/processed || goto :error
echo ETL pipeline complete.
goto :eof
:error
echo ETL pipeline failed.
exit /b 1
