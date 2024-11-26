# Dynatrace Migration Hub Strategy Parsing Code

The files needed are:

dynatrace_migration_parser.py - parsing code

AWS_Valid_Values.json - AWS valid values dictionary

credentials.py - Dynatrace credentials 



## Requirements
python 3+ 

Dynatrace Tenant

Dynatrace API key with [entities.read]



## To Run
1. Input the dynatrace API key and tenant in the credentals.py

2. Run code: ` python dynatrace_migration_parser.py `

The output file is defined in the code. It should produce a JSON file with the output.