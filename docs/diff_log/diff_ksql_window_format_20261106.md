# K-C01 KSQL roles and K-C07 declared window formats

Basis: [approved KSQL scope](../../features/sql-window-format/instruction.md#xid-E43BB05C6E4B) and explicit user correction requiring KSQL rules validated in Docker, not generic SQL assumptions.

## KSQL behavior correction

Actual ksqlDB7.9.2 accepts ordinary identifiers such as EXEC/UPDATE/UNION and embedded@, plus correctlyquoted literals. Builder validation now recognizes literal/quotedidentifier/token boundaries; genericSQL keyword assumptions removed. Generated-fragment raw comment/separator/malformed quote restrictions remain explicitly a client fragment constraint, not a claim about full KSQL (where comments and statementterminators are valid). Actual LINQSelect AST projections remain unsupported separately from identifier/literal text SELECT.

The shared SafeToString formatter doubles embeddedapostrophes according to KSQL lexical rules. This fixes actual generated O'Brien literals rather than weakening validation. Ordinary/numeric/null/bool formatting is preserved. New helper is KsqlFragmentGuard, not a general SQL parser or security audit.

## Window encoding correction

The sole production FixedTimeWindowedAvroSerDes caller is a source topic before string-key repartition; it explicitly selects Topic format. Internal Topic/Store selection replaces ambiguous decode-by-inner-exception fallback. Serialize/Deserialize use the same chosen layout: Topic key+8byte timestamp; Store key+8timestamp+4sequence with timestamp offsetlength-12. Existing2param default nowTopic, so its prior12byte writer becomes8byte consistent with actualsourcecontract; explicitStore retains old12byte representation. No storedstate migration or universal wrongformat detection is claimed.

Installed Streamiz1.7.1 nuspec/source commit32474e143a4469fe57a893015fb33ec05c855831 supplies format basis. Supplemental tests use actual installed TimeWindowedSerDes with sameinner in both directions, strict/permissive framed controls and exactbytes/sequence/null/shortbounds. No actualexternalAvro windowwirecapture.

## Evidence and boundaries

CorrectedKSQL baseline40:25fail15pass; correctedKSQL+wire50allpass. Earliergeneric assumptions/tests were superseded after usercorrection, preserved in local evidence. ActualDocker29rulecases18accept11reject;16actualC#generatedclauses allEXPLAIN200;7realqueries allreturn expectedId/value. InitialNDJSONharnessparsingerror corrected without product changes; repeatedfixtures notcountedextra. Ownfixturestreams/topics retained; no oldresourcesmodified.

Fullmain866pass27existing skips;CLI4/cache7pass;distinct877passes. L4golden2pass ApproveFalse, no snapshotchange. LibraryRelease net8/net10 builds0warnings/errors. Testsnet8;net10compileonly. L3final299pass2existing skips includes metadata-tagged40KSQL+10wirecases; overlapsfullsuite. Detailed local logs/TRX andDocker requests/responses review-artifacts/sql-window-format and review-artifacts/ksql-rules. IndependentQA pending.

No dependency/version/publicAPI/mainmerge/release change. Original design-policy unknowns K-R06/C03 remain held, not closed by these fixes. Formal unadopted report-composition Skill not executed.
