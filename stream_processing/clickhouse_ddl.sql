CREATE DATABASE IF NOT EXISTS hpc_monitor;

CREATE TABLE IF NOT EXISTS hpc_monitor.realtime_alerts
(
    machine_id      String,
    alert_type      String,
    value           Float64,
    severity        String,
    alert_timestamp String,
    inserted_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (machine_id, inserted_at);
