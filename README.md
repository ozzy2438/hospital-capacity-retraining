# Hospital Capacity Retraining Pipeline

Automated ML retraining pipeline for hospital capacity forecasting with drift detection and auto-promotion.

## Features

- **Monthly scheduled retraining** via Apache Airflow
- **Drift detection** using KS-test and PSI
- **Auto-promotion criteria**:
  - AUC improvement > 1%
  - No recall regression > 10%
  - No significant drift detected
- **Safety checks** to prevent model degradation

## DAG Structure

1. `extract_training_data` - Load latest data from warehouse
2. `train_candidate_model` - Train new model on fresh data
3. `calculate_drift` - Detect feature/target drift (KS-test, PSI)
4. `evaluate_models` - Compare production vs candidate metrics
5. `maybe_promote_model` - Auto-promote if criteria met

## Retraining Triggers

**Scheduled**: Monthly (1st of each month)

**Performance degradation**:
- AUC drops > 3-5 percentage points
- Precision/recall drops > 10-15%
- Brier score worsens > 10-15%

**Data drift**:
- KS-test p-value < 0.01
- PSI > 0.25
- Target drift > 20-30% over 4-6 weeks

**Hard events**: Protocol changes, EHR updates, external shocks

## Setup

1. Install Airflow: `pip install apache-airflow`
2. Copy DAG to your Airflow dags folder
3. Configure data sources and model storage
4. Update TODOs with your implementation

## Usage

The DAG runs automatically on schedule. Monitor via Airflow UI for:
- Drift alerts
- Model promotion decisions
- Performance metrics
