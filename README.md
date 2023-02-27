# BqJsonStreamWriter

Kotlin version of [google java-bigquerystorage tutorial](https://github.com/googleapis/java-bigquerystorage/tree/main/tutorials/JsonWriterDefaultStream)

Note the original code has reference to a `BqToBqStorageSchemaConverter` class that needs to be implemented locally

To run from a non gcp environment (e.g. dev workstation) then prefer a service account (or suitable alternative) to application default credentials.

e.g. `export GOOGLE_APPLICATION_CREDENTIALS=<path to service account file>`

If using a service account file it is best to generate a new one for this and then delete when no longer needed.

## Usage

Build a runnable with 

`./gradlew ":app:installDist"`

Run with

`app/build/install/app/bin/app <project_id> <dataset> <table_name> ./github.json`

which will create the table in the dataset if it does not exist already and load from the filename

