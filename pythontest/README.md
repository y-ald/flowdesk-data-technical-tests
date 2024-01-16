## Description 
this program includes a generate_bigquery_schema_from_pandas module and a set of utility functions to convert a pandas dataframe schema into a BigQuerry schema.

## Run it
To make it work, you'll need to install python3.12 and activate the dedicated python environment 
```sh
python3.12 -m venv .venv
source .venv/bin/activate

.venv/bin/python3.12 -m pip install -r requirements.txt
```
For now, to test them, you can use the unit tests(modified input) provided in the program or create a new module. We'll call our module and pass on our inputs 

## Go Further
To go further, we could
- Better code structure 
- Create an interface to interact with the user
- Add the ability to read data from a file to output the schema