# Introduction 
Tool to help data migration from OMNIA V2 application to OMNIA V3 application.

This tool requires access to the V2 Source Database and blob storage and to the V3 Target database and blob storage.
The migration process will take into consideration a mapping file that should describe how to move from one model to the other.

# Getting started

## 1. How to use the tool

The migration tool is a command line tool. To know the available commands just run:

```
OmniaMigrationTool.exe --help
```

To know the arguments of a given command, just execute from the command line:

```
OmniaMigrationTool.exe _COMMAND_ --help
```

Where the \_COMMAND_ is the name of one of the commands in the command line.

## 2. Creating a mapping template

The mapping file is a hierarchical JSON file that describes how to map entities and attributes from the Source (V2) to the Target (V3).
To help you out to speed up the configuration, you can use the following command to export a template of the mapping file based on the V2 Source model.

To export the template use the template command. 

Example:

```
OmniaMigrationTool.exe template --tenant 95c7899e-3957-42bd-acb2-bb5e25226163 --connection-string "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;"
```

The console will output the file path where the mapping file is.

## 3. Export

This tool is a two-step tool where you can export and import in different runs.

The export/import is defined in 3 main categories: Data, Files, and Users.

### 3.1. Export Data

Use the command Export to export the data existing in the V2 Source database.

Example:

```
OmniaMigrationTool.exe export --tenant 95c7899e-3957-42bd-acb2-bb5e25226163 --connection-string "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;" --mapping c:\mapping.json
```

This command will generate a folder with the data that must be imported to the target database.
Make sure you keep the files.

### 3.2. Export Files

Use the command export-files to export the files existing in the V2 Source blob storage and ensure that attachments are moved.

Example:

```
OmniaMigrationTool.exe export-files --tenant 95c7899e-3957-42bd-acb2-bb5e25226163 --connection-string "DefaultEndpointsProtocol=http;AccountName=myAccount;AccountKey=myKey;" --encryption-key "MYKEY"
```

This command will generate a folder with the files attached in the V2.

### 3.3. Export Users

Use the command export-users to export the users existing in the V2 Source tenant.

Example:

```
OmniaMigrationTool.exe export-users --tenant 95c7899e-3957-42bd-acb2-bb5e25226163 --connection-string "Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;"
```


## 4. Import

Before executing the import operation, make sure you have created a new Tenant, imported the Model and have a success Build.

### 4.1. Import Data

Use the command Import to import the data existing in the V2 Source database.

Example:

```
OmniaMigrationTool.exe import --tenant MyTenant --connection-string "User ID=root;Password=myPassword;Host=myTargetServer;Port=5432;Database=myDataBase;" --folder c:\exported\
```

Make sure that you provide to the folder argument the folder exported in step 3.1.

### 4.2. Import Files

Use the command Import-files to import the files existing in the V2 Source to the Target V3.

Example:

```
OmniaMigrationTool.exe import-files --tenant MyTenant --connection-string "DefaultEndpointsProtocol=http;AccountName=myAccount;AccountKey=myKey;" --folder c:\exportedfiles\ --mappings c:\exportedfiles\
```

Make sure that you provide to the folder argument the folder exported in step 3.2.

The mappings argument is the folder from where the file *file_mapping.csv*, generated in step 3.2, will be loaded.

### 4.3. Import Users

Use the command Import-users to create the users in target V3.
All the users in the CSV will be created and they will receive an email to define their password.

Users will be created with the Security Roles defined in the CSV.

To execute this step you will need an API Client to invoke the API, and that API Client will need to have privileges to create users. Assign the API Client to management *Administration* Role. At the end of the migration, the client can be removed from the role.

Example:

```
OmniaMigrationTool.exe import-users --tenant MyTenant --folder c:\exportedusers\ -e https:\\myomnia.com --client-id MYCLIENTID --client-secret MYCLIENTSECRET
```

Make sure that you provide to the folder argument the folder exported in step 3.3, where the *users.csv* is located.

# Mapping File - How to

## Mapping Agent Users

In V2, an agent can be related to a user.
When mapping an Agent, you can access to the *UserEmail* and *UserContactEmail* attributes and map them to an attribute in the target.

## Mapping non-base attributes

When mapping custom attributes from V2, the Source Type should be "Text" even when the Target Type is Decimal or Int for example.

# License
OMNIA 3 Samples are available under the MIT license.
