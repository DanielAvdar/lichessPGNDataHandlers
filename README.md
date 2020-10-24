# lichessPGNDataHandlers
Ranking players with page rank using spark graphx API and store the ranking result into BigQuery. Include png extractor transforming it into game rows saved in BigQuery.

## games schema
|Event|WightName|BlackName|Winner|WightRating|BlackRating|ECO|Opening|time-control|Date|Time|Termination|
|---|---|---|---|---|---|---|---|---|---|---|---|

## Ranked players schema
|name|innerPRgraph|innerPRgraphClass|innerPRgraphBullet|innerPRgraphBlitz|outerPRgraph|outerPRgraphClass|outerPRgraphBullet|outerPRgraphBlitz|
|---|---|---|---|---|---|---|---|---|

## Copyright
@ Daniel Avdar
