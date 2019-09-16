# elasticsearch-hll-rollups

Elasticsearch plugins to support "HLL Rollups". The goal is to store the computed HyperLogLogPlusPlus sketch
instead of raw values. This serialized version is different from Elasticsearch, which will store all values and
then compute the HLL at query time. What this plugin loses is flexibility (precision is fixed at index-time) it gains
in disk savings and overall scalability for very high-cardinality fields.

## State of the Project

This is an initial implementation of the idea which runs on Elasticsearch 6.8.2.  We've validated the correctness
of the plugin, but there's a lot more work to be done in terms of optimizations and code cleanups. Our team
doesn't write a lot of Java, so there's likely more than a few issues to be worked through.

There's also a lot of code copied from the ES codebase. I'm sure there's a lot to be done cleaning
that up so we only keep or rewrite the bits we need. Since there was a cardinality agg already there
it made to most sense to follow that model.

TODO:

- [ ] Rename `precision` to `precisionThreshold` so it matches the param for `cardinality`.
- [ ] Consider skipping the HLL step when indexing LC-sized datasets. It may speed up indexing and
      later querying to instead store either the raw values of the mmh3 hash that the LC uses.
- [ ] Write up a section about how storing the HLL can help with GDPR compliance (and validate this assumption).
- [ ] Figure out how reindexing is going to work with all of this. Do we need a way to return a base64 encoded
      HLL to the user? 
- [ ] Many more tests for the aggregation code.
