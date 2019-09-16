# To get running

## Generate some data

`generate_data.py` will handle generating documents with various numbers
of UUIDs in them:

1. Tiny: 100k docs, 1 to 4 ids -- `python generate_data.py 100000 --num-services 100 --jobs-per-service 1000 --jobs-per-container 2 --job-count-variation 100 > rally-tracks/documents-tiny.json`
2. Small: 100k docs, 50 to 150 ids -- `python generate_data.py 100000 --num-services 100 --jobs-per-service 1000 --jobs-per-container 100 --job-count-variation 50 > rally-tracks/documents-small.json`
3. Med: 100k docs, 400 to 600 ids -- `python generate_data.py 100000 --num-services 100 --jobs-per-service 1000 --jobs-per-container 500 --job-count-variation 20 > rally-tracks/documents-med.json`
4. Large: 100k docs, 1440 to 2160 ids -- `python generate_data.py 100000 --num-services 100 --jobs-per-service 1000 --jobs-per-container 1800 --job-count-variation 20 > rally-tracks/documents-large.json`
5. XLarge: 100k docs, 2250 to 2750 ids -- `python generate_data.py 100000 --num-services 100 --jobs-per-service 1000 --jobs-per-container 2500 --job-count-variation 10 > rally-tracks/documents-xlarge.json`
NOTE: HLL Precision 11 has an LC -> HLL threshold of 1800

`track.json` expects `documents_tiny.json` right now. If you want to
use a different file, you'll need to edit that.


## Put this at the bottom ~/.rally/rally.ini

This will enable the plugin within esrally. Use your own path, obviously.


```
[distributions]
release.cache = true
plugin.elasticsearch-hll-rollups.release.url=file:///Users/kfb/src/ct/elasticsearch-hll-rollups/build/distributions/elasticsearch-hll-rollups-0.0.1.zip
```

## Run `esrally`

For HLLs: `esrally --track-path=./rally-tracks/cardinality-perf --distribution-version=6.8.2 --elasticsearch-plugins="elasticsearch-hll-rollups"  --on-error=abort --car="4gheap" --challenge=cardinality-hll`

For a baseline using `long`s: `esrally --track-path=./rally-tracks/cardinality-perf --distribution-version=6.8.2 --elasticsearch-plugins="elasticsearch-hll-rollups"  --on-error=abort --car="4gheap" --challenge=cardinality-long`

## TODO:

- [ ] Generate files and put them in S3
- [ ] Analyze some Parse.ly diagnostic data to generate a realistic sample
- [ ] Figure out a better way to switch between data files.
