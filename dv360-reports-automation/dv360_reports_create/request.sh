curl -X POST localhost:8080 \
   -H "Content-Type: application/cloudevents+json" \
   -d '{
    "logs_dataset_id": "poc_dv360_reports",
    "logs_table_id": "ekam_main_extract_7days_reports_logs",
    "report": "ekam_main_extract_7days",
    "data_range": "LAST_7_DAYS"
}'
















