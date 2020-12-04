# dapla-data-maintenance


To test from Jupyter terminal:

`dapla --jupyter --server <data-maintenance-url> ls <path>`

Examples:

staging: `dapla --jupyter --server https://data-maintenance.staging-bip-app.ssb.no ls felles`

local (against localstack): `dapla --jupyter --server http://172.18.0.1:10200 ls felles`