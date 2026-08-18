# Standalone retirement controls

OKS or OAE may be marked read-only only after all applicable items have dated evidence and an
owner. Archival is a later, separate action after the declared rollback window closes.

- Unified K1-K7 and WP10 gates are green for the release candidate.
- Downstream Maven/API/Studio consumers are enumerated and migrated.
- Definition export/import counts and SHA-256 digests match per tenant.
- Existing executions are terminal in their original engine, or that engine remains available.
- Shadow read comparison is green, or explicitly `not-applicable` because no source deployment is
  available; absence is never reported as a successful comparison.
- Source database, Kafka and Pekko backups have a tested restore procedure and retention expiry.
- New standalone artifact publication is disabled only after dependency-repository consumers are
  confirmed migrated.
- Standalone deployment is write-fenced and provenance remains readable throughout rollback.
- Rollback owner, decision deadline and success criteria are recorded.
- Archive approval occurs only after rollback expiry and legal/data-retention review.

The old repositories remain immutable provenance. Retirement does not delete tags, release notes,
schemas, topic descriptions, serialization manifests or migration evidence.

