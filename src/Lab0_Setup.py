AzureStorageAccount = '<AzureStorageAccount>'     # Azure Storage (AS) account containing MAG dataset
AzureStorageAccessKey = '<AzureStorageAccessKey>' # Access Key of the Azure Storage (AS) account
MagContainer = '<MagContainer>'                   # The container name in Azure Storage (AS) account containing MAG dataset, Usually in forms of mag-yyyy-mm-dd
OutputContainer = '<OutputContainer>'             # The container name in Azure Storage (AS) account where the output goes to

MagDir = '/mnt/mag'
OutputDir = '/mnt/output'

if (any(mount.mountPoint == MagDir for mount in dbutils.fs.mounts())):
  dbutils.fs.unmount(MagDir)

dbutils.fs.mount(
  source = ('wasbs://%s@%s.blob.core.windows.net' % (MagContainer, AzureStorageAccount)),
  mount_point = MagDir,
  extra_configs = {('fs.azure.account.key.%s.blob.core.windows.net' % AzureStorageAccount) : AzureStorageAccessKey})

if (any(mount.mountPoint == OutputDir for mount in dbutils.fs.mounts())):
  dbutils.fs.unmount(OutputDir)

dbutils.fs.mount(
  source = ('wasbs://%s@%s.blob.core.windows.net' % (OutputContainer, AzureStorageAccount)),
  mount_point = OutputDir,
  extra_configs = {('fs.azure.account.key.%s.blob.core.windows.net' % AzureStorageAccount) : AzureStorageAccessKey})

dbutils.fs.ls('/mnt')
