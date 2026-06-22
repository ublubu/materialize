# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for SS-282: "input compacted past resume upper".
#
# An Iceberg sink resumes from the upper recorded in the table's snapshots
# (resume_upper). On startup it errors if the input collection's since (as_of)
# has advanced past resume_upper, because the data between resume_upper and
# as_of is gone and the sink would otherwise silently skip it.
#
# This reproduces the scenario from the issue: the Iceberg table is left in
# the catalog with old snapshots while the upstream object the sink reads from
# is recreated. The recreated object's since is at the current time, well past
# the resume_upper still recorded in the stale Iceberg table.

> CREATE SECRET overcompact_access_key AS '${arg.s3-access-key}'

> CREATE CONNECTION overcompact_polaris TO ICEBERG CATALOG (
    CATALOG TYPE = 'REST',
    URL = 'http://polaris:8181/api/catalog',
    CREDENTIAL = 'root:root',
    WAREHOUSE = 'default_catalog',
    SCOPE = 'PRINCIPAL_ROLE:ALL'
  );

> CREATE TABLE overcompact_src (a int, b text);

> INSERT INTO overcompact_src VALUES (1, 'one'), (2, 'two'), (3, 'three');

> CREATE SINK overcompact_sink
    FROM overcompact_src
    INTO ICEBERG CATALOG CONNECTION overcompact_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'overcompact_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

# Wait for the sink to commit at least one snapshot, advancing the Iceberg
# table's resume_upper past the minimum.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'overcompact_sink'
running

# Drop the sink and the object it reads from, then recreate the object. The
# Iceberg table 'overcompact_table' stays in the catalog with its old snapshots.
> DROP SINK overcompact_sink;

> DROP TABLE overcompact_src;

# Separate the recreated table's timestamps from the old resume_upper.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

> CREATE TABLE overcompact_src (a int, b text);

> INSERT INTO overcompact_src VALUES (4, 'four');

# Recreate the sink against the same (stale) Iceberg table. Its as_of is the
# recreated table's since, which is now well past the resume_upper recorded in
# the table's snapshots, so the sink must stall instead of silently dropping
# the gap.
> CREATE SINK overcompact_sink
    FROM overcompact_src
    INTO ICEBERG CATALOG CONNECTION overcompact_polaris (
        NAMESPACE 'default_namespace',
        TABLE 'overcompact_table'
    )
    MODE APPEND
    WITH (COMMIT INTERVAL '1s');

> SELECT bool_or(error ~* 'input compacted past resume upper')
  FROM mz_internal.mz_sink_statuses
  WHERE name = 'overcompact_sink' AND status = 'stalled'
true
