# DV360 reports automation

This middleware automates the import of Display & Video 360 (DV360) Offline Reporting files into BigQuery.
It can be deployed onto [Cloud Functions](https://cloud.google.com/functions/).
You can trigger jobs by making an http "POST" request with the required body and optional arguments.

It consists of 2 separate steps, one for requesting the reports to the API and the second one to download and ingest those reports once they have been created. Each step is exlained in more detail in their respectives folders.

# Author contacts

Juan Cruz Montes de Oca - juan.montes@mightyhive.com