---
page_type: sample
languages:
- python
products:
- azure
description: "PySpark examples running on Azure Databricks to analyze sample Microsoft Academic Graph Data on Azure storage."
urlFragment: microsoft-academic-graph-pyspark-samples
---

# Microsoft Academic Graph PySpark Samples

PySpark examples running on Azure Databricks to analyze sample Microsoft Academic Graph Data on Azure storage.

## Prerequisites

Before running these examples, you need to complete the following setups:

* Setting up provisioning of Microsoft Academic Graph to an Azure blob storage account. See [Get Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/academic-services/graph/get-started-setup-provisioning).

* Setting up Azure Databricks service. See [Set up Azure Databricks](https://docs.microsoft.com/academic-services/graph/get-started-setup-databricks).

## Gather the information that you need

   Before you begin, you should have these items of information:

   :heavy_check_mark:  The name of your Azure Storage (AS) account containing MAG dataset from [Get Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/academic-services/graph/get-started-setup-provisioning.md#note-azure-storage-account-name-and-primary-key).

   :heavy_check_mark:  The access key of your Azure Storage (AS) account from [Get Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/academic-services/graph/get-started-setup-provisioning.md#note-azure-storage-account-name-and-primary-key).
   
   :heavy_check_mark:  The name of the container in your Azure Storage (AS) account containing MAG dataset.
   
   :heavy_check_mark:  The name of the output container in your Azure Storage (AS) account.

### Quickstart

1. git clone https://github.com/Azure-Samples/microsoft-academic-graph-pyspark-samples.git

1. Follow instructions in [PySpark analytics samples for Microsoft Academic Graph](https://docs.microsoft.com/academic-services/graph/samples-azure-databricks) to run PySpark scripts in this repository.

## Resources

* [Microsoft Academic Graph documentation](https://docs.microsoft.com/en-us/academic-services/graph/)
* [Create an Azure Databricks service](https://azure.microsoft.com/services/databricks/)
* [Create a cluster for the Azure Databricks service](https://docs.azuredatabricks.net/user-guide/clusters/create.html)
* [Import a Databrick notebook](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook)
* [Get started with Storage Explorer](https://docs.microsoft.com/en-us/azure/vs-azure-tools-storage-manage-with-storage-explorer)

