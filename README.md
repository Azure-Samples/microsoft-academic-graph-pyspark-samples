# Microsoft Academic Graph PySpark Samples

PySpark examples to analyze sample Microsoft Academic Graph Data on Azure storage.

## Prerequisites

Before running these examples, you need to complete the following setups:

* Setting up provisioning of Microsoft Academic Graph to an Azure blob storage account. See [Get Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/academic-services/graph/get-started-setup-provisioning).

* Setting up Azure Databricks service. See [Set up Azure Databricks](https://docs.microsoft.com/academic-services/graph/get-started-setup-databricks).

## Gather the information that you need

   Before you begin, you should have these items of information:

   :heavy_check_mark:  The name of your Azure Storage (AS) account containing MAG dataset from [Get Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/academic-services/graph/get-started-setup-provisioning.md#note-azure-storage-account-name-and-primary-key).

   :heavy_check_mark:  The access key of your Azure Storage (AS) account from [Get Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/academic-services/graph/get-started-setup-provisioning.md#note-azure-storage-account-name-and-primary-key).

   :heavy_check_mark:  The name of the container in your Azure Storage (AS) account containing MAG dataset.

### Quickstart

1. git clone https://github.com/Azure-Samples/microsoft-academic-graph-pyspark-samples.git
1. cd microsoft-academic-graph-pyspark-samples/src
1. [Create a Databricks notebook](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-manage.html#create-a-notebook) and run Lab0 script before you run other scripts.

## Resources

- [Microsoft Academic Graph documentation](https://docs.microsoft.com/en-us/academic-services/graph/)
