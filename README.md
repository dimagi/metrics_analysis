### USAGE :
 Update the `config.yml` with datadot `app_key` and `api_key` which you will find in vault file. 

```
$ python scripts/machine_sizes.py --help
usage: machine_sizes.py [-h]
                        [--env-name {icds,pna,production,softlayer,staging,swiss}]
                        [-c CONFIG] [-d DAYS_PAST] [--fixed-date FIXED_DATE]

Print machine sizes for cluster.

optional arguments:
  -h, --help            show this help message and exit
  --env-name {icds,pna,production,softlayer,staging,swiss}
                        Environment to query.
  -c CONFIG, --config CONFIG
                        Path to config file.
  -d DAYS_PAST, --days-past DAYS_PAST
                        How many days in the past to query.
  --fixed-date FIXED_DATE
                        Particular Date for which to query <YYYY-MM-DD>


python scripts/machine_sizes.py --config config.yml --fixed-date 2019-12-09  --env-name icds
```