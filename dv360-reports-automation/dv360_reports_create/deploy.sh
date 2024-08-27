# Use this script to easily deploy a CF using the CLI.
# You can save this in your root folder and name it deploy.sh

#### Config ####
FUNCTION_NAME='dv360_reports_create'
REGION='us-central1'
PROJECT_ID='mdlz-na'
SERVICE_ACCOUNT_EMAIL='dv360-api-reporting@mdlz-na.iam.gserviceaccount.com'
IAM_ROLE='roles/cloudfunctions.developer'
ENTRY_POINT=main

gcloud functions deploy $FUNCTION_NAME \
    --trigger-http \
    --memory=8192MB  \
    --runtime=python39 \
    --entry-point=$ENTRY_POINT \
    --region=$REGION \
    --timeout=540s \
    --security-level=secure-optional \
    --allow-unauthenticated \
    --service-account=$SERVICE_ACCOUNT_EMAIL \
    --quiet \
    --project $PROJECT_ID