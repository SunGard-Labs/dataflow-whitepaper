FIS Google Cloud Platform Whitepapers 
=====

## About 

In December 2015, FIS Advanced Technology released the whitepaper "Transforming Options Market Data with the Dataflow SDK" to provide engineers insight into Google Cloud Dataflow's programming model and execution environent.  

In August of 2016, FIS Advanced Technology produced a sequel to the original 2015 Bigtable whitepaper, detailing the introduction of Google Cloud Dataflow and Google Cloud BigQuery to the Market Reconstruction Tool's solution architecture, as well as to provide a deeper look into the material covered at our [Analyzing 25 billion stock market events in an hour with NoOps on GCP](https://www.youtube.com/watch?v=fqOpaCS117Q) talk from Google NEXT 2016.

Assets referenced in both whitepapers can be found in this repository.

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

```gsutil mb gs://<pick_a_bucket_name>```

Copy input specimen to Google Cloud Storage:

```gsutil cp input/zvzzt.input.txt gs://<pick_a_bucket_name>```

Ensure that there is a proper destination dataset in your BigQuery account.  For example, this command will create a dataset called dataflow-project within BigQuery for your account:

```bq mk <dataflow_project>```

Execute the following, substituting your own values `PROJECT` and `BQDEST` in `bin/run`:

```cd bin && ./run gs://<pick_a_bucket_name>/zvzzt.input.txt```

*The Pipeline will automatically create the table if it does not exist, although it cannot create the initial dataset.*

To execute the job upon Google Cloud Platform using Maven, edit the associated values for your project ID and account within `pom.xml` and then run:

```mvn clean install && mvn -Pgcp exec:exec```

Remember that you can not use local files but have to use files stored from/to GCS (`gs://`).

## Errata

Please open up a GitHub issue for any discrepancies or inconsistencies you may discover and we will correct and publish here.

## See Also

* [Market Reconstruction 2.0: A Financial Services Application of Google Cloud Bigtable and Google Cloud Dataflow](http://www.fisglobal.com/Solutions/Institutional-and-Wholesale/Broker-Dealer/-/media/FISGlobal/Files/Whitepaper/A-Financial-Services-Application-of-Google-Cloud-Bigtable-and-Google-Cloud-Dataflow.pdf)
* [Transforming Options Market Data with the Dataflow SDK](https://cloud.google.com/dataflow/pdf/TransformingOptionsMarketData.pdf)
* [Scaling to Build the Consolidated Audit Trail: A Financial Services Application of Google Cloug Bigtable](https://cloud.google.com/bigtable/pdf/ConsolidatedAuditTrail.pdf)
* [FIS Market Reconstruction Tool](http://www.fisglobal.com/Solutions/Institutional-and-Wholesale/Broker-Dealer/Market-Reconstructions-and-Visualization)
* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Google BigQuery](https://cloud.google.com/bigquery/)
* [OCC](http://www.optionsclearing.com/)

## License
MIT. See license text in [LICENSE](LICENSE).

## Copyrights and Names
Copyright © FIS 2016. Licensed under the MIT license.

