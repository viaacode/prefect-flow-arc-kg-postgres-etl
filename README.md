# prefect-flow-template


## Register saved queries in TriplyDB

You can run the `register.js` script in the `queries/` folder to register all saved queries.

```
cd queries
TOKEN=x ACCOUNT=x DATASET=x node register.js
```
This script will output a `config.json` which can be used as input to the Prefect JSON block that serves a configuraton for the Flow. 
For debugging purposes, you can immediately download the first page of results after a query is registered by setting `FETCH_RESULT=1`. The results will stored as CSV files in a `tables` folder.

To manage multiple environments, you can use different `.env` files and [dotenvx](https://github.com/dotenvx/dotenvx).
First install dotenvx with:
```
npm install @dotenvx/dotenvx -g # installs dotenvx globally
```

Then use dotenvx to run `register.js`:

```
cd queries
dotenvx run --env ENV=int -f .env.int -- node register.js
```

Or use the bash script `register` like so:

```
ENV=int ./register 
```

This will output a `config-int.json` and storethe CSV filesa in a `tables-int` folder.
