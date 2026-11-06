<!-- xid: E43BB05C6E4B -->
<a id="xid-E43BB05C6E4B"></a>

# K-C01 KSQL generation and K-C07 declared window formats

## Approved basis

User requested continuing remaining repairs, including reproduction/tests, independent review and commit/push after valid checks. User explicitly corrected generic SQL assumptions: target is KSQL and its rules must be verified in Docker. This final design supersedes the earlier generic-blacklist draft; history and basis_refuted correction handoff remain in governing XRefKit work/sessions/2026-11-06_session_ksql_sql_window_format_fix.md, not as competing instructions here.

## K-C01 difference

Use actual ksqlDB7.9.2 behavior: ordinary identifiers EXEC/EXECUTE/UPDATE/UNION/sp_/xp_ and embedded@ are accepted; quoted identifiers and doubled-apostrophe literals are accepted. Shared BuilderValidation.SafeToString doubles literal apostrophes, preserving normal/numeric/null/bool rendering. WHERE unsupported LINQSelect projections are checked as actual AST nodes, separately from literal or identifier text SELECT.

KsqlFragmentGuard checks generated clause fragment integrity: quotes/escaped delimiters and identifier boundaries distinguish data/identifier roles from bare reserved command tokens DROP/DELETE/INSERT/SCRIPT. Generic EXEC/UPDATE/UNION prefix/keyword blacklist is removed. Existing raw comment, multi-statement separator and unterminated-quote restriction is only a generated-fragment constraint, not a general KSQL rule: actual full KSQL accepts comments and statement terminators. Do not claim a complete parser or security audit.

## K-C07 difference and derivation

Installed Streamiz.Kafka.Net1.7.1 nuspec pins upstream32474e143a4469fe57a893015fb33ec05c855831. TimeWindowedSerDes/WindowKeyHelper define Store[key][timestamp8][sequence4], timestampoffsetlength-12; Topic[key][timestamp8]. Sole production FixedTimeWindowedAvroSerDes caller consumes a topic then SelectKey-stringifies/repartitions with StringSerDes. Evidence in governing work/reviews/2026-11-06_ksql_window_wire_evidence.md.

Internal explicit Topic/Store mode replaces decoder-exception fallback guessing. Serialize/Deserialize use the declared layout. Existing2param constructor defaults Topic, so its prior12byte writer is corrected to8byte; productioncaller declaresTopic explicitly. ExplicitStore retains12byte layout and corrects timestamp offset. Null/tombstone behavior retained. This is not a storedstate migration or universal arbitrary wrong-format detection.

## Targets and verification

Targets: Query Builders/Common BuilderBase, BuilderValidation, new KsqlFragmentGuard; WhereClauseBuilder; SerDes/FixedTimeWindowedAvroSerDes; CacheExtensions soleconstructor/comment; KsqlFragmentRegressionTests and existing windowSerDes tests; this instruction and new diffrecord. No publicAPI/dependency/version/release/mainmerge change.

CorrectedKSQL40cases red on cleanbase9a6d038e90f14b285f84baa918a388f1f13af438 before final source acceptance:25fail15pass. CurrentKSQL40+wire10green; wire original4red3fail1pass and6post-source mode/oracle supplements. Generic provisional tests are historical, not KSQL acceptance proof. TraitL3 metadata places all50cases in regular gate without changing their assertions.

Mandatory actualDocker cp-ksqldb-server7.9.2 /info7.9.2 andimage sha256:dd7a2a94121f93278eb9351a181cf000d0c2e5194f819cc17ca27ee847b25771:29lexical/expression probes,16rebuilt actual C#generated clauses viaEXPLAIN,7executed SELECTs checking exact rowId/value from independentlyproducedJSON. Record fullrequests/responses, distinguish EXPLAIN from evaluated results. Dedicatedfixture resources only; priorresourcesnotreset. Version-scoped proof, not allserverversions.

Mandatory parent-serial fullmain/CLI/cache, L3/L4golden without Approve, Release net8/net10 builds, independent source+operationalreport review. Testsnet8;net10compileonly. Wire oracle uses installed Streamiz with sameinner and exactlayout; no actual KafkaAvro window capture/live store assertion. No monitor/performance/security/delivery generalization. External traceability completes in instruction/logs; no temporary trace comments.

## Deferred decisions

Original K-R06 delivery/DLQ commit policy and K-C03 deprecated decimal support remain separately needs_confirmation; original review heldunknowns stay open. This scoped repair does not decide those policies or change formal reportSkill adoption.
