curl -X POST localhost:8080 \
   -H "Content-Type: application/cloudevents+json" \
   -d '
{
    "bq_logs_table": "ekam_reach_reports_logs",
    "bq_report_destination_table": "042_mdlz_dm_amea_ekam_india_DV360_reach",
    "bq_report_destination_dataset": "042_mdlz_dm_amea",
    "bq_finished_table": "ekam_reach_reports_finished",
    "destination_folder": "ekam_reach"
}'