# v1 maintains order-book state; it does not match orders

The flagship's story is ingestion → state → persistence → fan-out. "Order book" here means the maintained L2 view (snapshot bootstrap plus continuity-checked diffs), not an engine that accepts orders. A matching engine would drag in an execution domain — orders, balances, fills — that no v1 claim uses, adding correctness risk with zero evidence value. Recorded so nobody "completes" the order book into a matching engine by accident; if a matching engine ever matters it is a separate effort.
