Transforming Options Market Data with the Dataflow SDK
=====

## About 

In December 2015, FIS released a follow-up whitepaper to May 2015's "Scaling to Build the Consolidated Audit Trail - a financial services application of Google Cloud Bigtable", focusing specifically on Google Cloud Dataflow's programming model and execution environent.  This is the repository associated with that paper, entitled  "Transforming Options Market Data with the Dataflow SDK".

## Introduction

This repository provides the buildable source code referenced within the whitepaper.  It requires Maven, the Google Cloud Platform tools and the Google Cloud Dataflow SDK.

## Running the example project

To build the project:

```mvn clean install```

Out of the box, the repository is configured to run a standalone Dataflow job on the local workstation, using input data that ships with the repository (`input/zvzzt.input.txt`).


The example can be run locally either executing:

```cd bin && ./run``` 

or by calling Maven with:

```mvn clean install && mvn -Plocal exec:exec```.  

## Running the project on Google Cloud Platform / BigQuery

Once you have activated a Google account on Google Cloud Platform, you will need your Project ID and at least one GCS bucket to be created (for storing deployment artifacts and input files.)

Log your shell into GCP:

```gcloud auth login```

If you do not already have a Google Cloud Storage bucket, you can create one with the following command:

```gsutil mb gs://<pick-a-bucket-name>```

Copy input specimen to Google Cloud Storage:

```gsutil cp input/zvzzt.input.txt gs://<my-gcs-bucket>```

Ensure that there is a proper destination dataset in your BigQuery account.  For example, this command will create a dataset called dataflow-project within BigQuery for your account:

```bq mk dataflow-project```  

Execute the following, substituting your own values: 

```cd bin && ./run gs://<my-gcs-bucket>/zvzzt.input.txt dataflow-project.options```

*The Pipeline will automatically create the table if it does not exist, although it cannot create the initial dataset.*

To execute the job upon Google Cloud Platform using Maven, edit the associated values for your project ID and account within `pom.xml` and then run:

```mvn -Pgcp exec:exec```

## Errata

Please open up a GitHub issue for any discrepancies or inconsistencies you may discover and we will correct and publish here.

## See Also

* [Dataflow Whitepaper](http://)
* [Bigtable Whitepaper](https://cloud.google.com/bigtable/pdf/ConsolidatedAuditTrail.pdf)
* [FIS/SunGard](https://fisglobal.com)
* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Google BigQuery](https://cloud.google.com/bigquery/)
* [OCC](http://www.optionsclearing.com/)

## License
MIT. See license text in [LICENSE](LICENSE).

## Copyrights and Names
Copyright © SunGard 2015. Licensed under the MIT license.

