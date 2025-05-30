#!/usr/bin/env sh
set -e

# 1) Run your full pipeline (ingest → clean → metrics)
python run_pipeline.py

# 2) Launch Streamlit (listen on all interfaces)
exec streamlit run app/app.py --server.address=0.0.0.0 --server.port=8501
