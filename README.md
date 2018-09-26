# Flink Load Trades into Elasticsearch

## Average Stock Sales Price

#### Implementation details:

- Logical Flow:
    - `Kafka > Parse JSON payload > Avg Sales price per window > Elasticsearch`
- Read from Kafka
- Extract key fields into a `Tuple2`
- Applies a Keyed stream, using `Symbol` as the key
- Implements a custom `WindowFunction` to work out Average `lastSalePrice`
- Writes to local Elasticsearch 6.x

#### To Do:

- Write to Elastic Cloud 6.x cluster.
