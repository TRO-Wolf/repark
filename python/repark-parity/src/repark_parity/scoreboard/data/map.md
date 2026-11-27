# map — python/repark-parity/src/repark_parity/scoreboard/data

## Purpose

The Spark-door scoreboard corpus and its committed scoreboards. The cells and their Spark answers
come from the outside whole-door audit of 2026-11-21: oracle **Spark 4.1.2**, audited RePark main
**`dac2af6162b2195f7cd0b9e5a4cbd6aeae627174`** (#1391, package 1.5.6, native SHA-256
`85e492284a2b41b0cb13e4fe608e9dec126c4f1a1673a3ea81e213364c5042aa`). Procedure:
[../../../../../../.agents/skills/spark-door-scoreboard/SKILL.md](../../../../../../.agents/skills/spark-door-scoreboard/SKILL.md).
This directory closes when the corpus is re-recorded against a newer Spark; the new corpus then
replaces it whole, with its own provenance here.

## Contents

| Path | Holds |
|---|---|
| `cells/<lane>.cases.jsonl.gz` | one cell definition per line, sorted-key ASCII JSON; the answer digest binds to these bytes |
| `cells/<lane>.spark.jsonl.gz` | the recorded Spark 4.1.2 answer per cell, with `input_sha256` and `engine: spark` |
| `exclusions.jsonl.gz` | the 619 adjudicated exclusions (`id`, `lane`, `reason`), frozen |
| `scoreboards/<date>-<sha8>.json.gz` | one scoreboard: totals, ANSI split, lanes, families, every cell's class |
| `scoreboards/<date>-<sha8>.md` | its short Markdown table |
| `reports/<date>-<base8>-to-<head8>.json.gz` + `.md` | one comparison: moved on, moved off, changed, by lane and id |

Committed: [the 2026-11-21 baseline](scoreboards/2026-11-21-dac2af61.md),
[main `9df269acb` on 2026-11-27](scoreboards/2026-11-27-9df269ac.md), and
[their comparison](reports/2026-11-27-dac2af61-to-9df269ac.md).

## Provenance and the rules applied when it was stored (2026-11-27)

- **Primary definitions only.** The audit's raw lanes hold 17,725 answer pairs in 62 directories.
  The 17,264 it counted are its primary definitions: a sub-lane folds into its top lane, the
  `dates/corrected` definition replaces an earlier one with the same id, and a `refute-*` lane keeps
  only its `refute-`/`review.` ids (its other rows repeat cells stored elsewhere). That set is
  stored, one file pair per top lane.
- **Exclusions are frozen.** The audit derived some exclusions from the `dac2af61` RePark answer
  (its `EXCLUDED` class, the `str.printf-` typed-seat rule). Those answers are not stored, so the
  619 ids and reasons are data, not code. 16,645 cells remain comparable.
- **The `dac2af61` RePark answers are not stored.** The baseline scoreboard replaces them; it was
  rebuilt from the audit's own diff files and equals the audit's counts exactly.
- **Scrub.** 31 Spark refusal `message` texts (files 16, settings-residual 10, refute-settings 5)
  and one excluded `value` (`setting.read.spark.sql.warehouse.dir`) held machine paths. The classifier
  never reads `message`, and the excluded cell is not compared, so each path prefix became
  `<spark-site-packages>/`, `<run-tmp>/` or `<tmp>/`. No cell definition held a path. Two
  `refute-time.dates-13.prior-card-*` cells set a fourth IANA zone as a test parameter; kept.
- **Generators dropped.** Each lane's `generate.py` imported audit-root paths and carried module
  docstrings, so none passes the repository gates. Their source revisions are below.

| Lane | Stored | Comparable | Excluded |
| --- | ---: | ---: | ---: |
| aggregate-api-extras | 28 | 28 | 0 |
| aggregate-grid | 1,043 | 1,043 | 0 |
| aggregate-grid-writes | 40 | 40 | 0 |
| aggregate-registry | 396 | 314 | 82 |
| card-writes | 46 | 40 | 6 |
| catalog | 10 | 10 | 0 |
| constructor-schema | 72 | 72 | 0 |
| controls | 15 | 15 | 0 |
| dates | 3,105 | 3,072 | 33 |
| declared-refusals | 88 | 88 | 0 |
| extended-writes | 14 | 14 | 0 |
| file-pins | 20 | 20 | 0 |
| files | 172 | 172 | 0 |
| frame-methods | 198 | 198 | 0 |
| frames-groups | 300 | 300 | 0 |
| frames-groups-bounds | 12 | 12 | 0 |
| frames-groups-writes | 43 | 43 | 0 |
| inventory | 1 | 0 | 1 |
| local-statements | 34 | 10 | 24 |
| nested-pairs | 2,124 | 2,124 | 0 |
| nested-writes | 36 | 36 | 0 |
| numbers | 1,360 | 1,360 | 0 |
| parser-errors | 50 | 50 | 0 |
| public-examples | 465 | 376 | 89 |
| record-samples | 104 | 104 | 0 |
| reference | 2,292 | 2,282 | 10 |
| refute-collections | 15 | 15 | 0 |
| refute-core | 2 | 2 | 0 |
| refute-frames | 22 | 22 | 0 |
| refute-prior | 15 | 15 | 0 |
| refute-seeded | 11 | 11 | 0 |
| refute-settings | 27 | 27 | 0 |
| refute-time | 43 | 43 | 0 |
| rewrites | 310 | 302 | 8 |
| rewrites-followup | 50 | 50 | 0 |
| rewrites-pins | 36 | 36 | 0 |
| rewrites-prior | 946 | 926 | 20 |
| rewrites-prior-reconcile | 10 | 10 | 0 |
| rewrites-seats | 228 | 228 | 0 |
| seeded | 76 | 76 | 0 |
| session-types | 99 | 98 | 1 |
| settings | 398 | 126 | 272 |
| settings-files | 66 | 66 | 0 |
| settings-residual | 134 | 90 | 44 |
| settings-writes | 24 | 24 | 0 |
| sql | 112 | 112 | 0 |
| strings-collections | 2,265 | 2,236 | 29 |
| strings-collections-discriminants | 75 | 75 | 0 |
| strings-collections-final | 27 | 27 | 0 |
| strings-collections-followup | 130 | 130 | 0 |
| strings-collections-typed | 75 | 75 | 0 |
| **total** | **17,264** | **16,645** | **619** |

Generator source revisions (SHA-256 of the file in the audit's `out/cells/`):

| Generator | SHA-256 |
| --- | --- |
| `aggregate-api-extras/generate.py` | `67c6c45843e4c0c679f0530f429d911e0c511a389c26a56b905d0b366c51d574` |
| `aggregate-grid/generate.py` | `1a2439cb6e1bc6e31be5b883155949f42df39acb23ff820ed876fe473a1f5d43` |
| `aggregate-grid-writes/generate.py` | `e568386f23ba3f5d629cda09e6e40aeb34ae4d168e5f67ada896b5cebd51cb07` |
| `aggregate-registry/generate.py` | `c0436617cd58d506ef6662890489bcba70670275f2582c272bc9ea27cc8a6c04` |
| `card-writes/generate.py` | `ded4a499a58b181d9b6f372c41447e02faf3f4573fa80302a58f8292058508c1` |
| `catalog/generate.py` | `371f787e73040e40f1ed84f0c0077cd67a7179d59a1fa9f21185e6fa918a5477` |
| `constructor-schema/generate.py` | `ee685a65ed16efc63332394293b47adee3c87a5ee2c16f9240e8a06096c21cb1` |
| `controls/generate.py` | `35932c4c6560e01476779d347863c05100a2d0ea3fa512fe90e2b93aa7e1e51d` |
| `dates/confirmation/generate.py` | `7881f7777968803bc85da0d732e937884f1a8f2fc135e23369404254e8cba261` |
| `dates/corrected/generate.py` | `0512e1d2362a485acb532437379bad23bcfe86ca2679e5736a22d67bc31ec527` |
| `dates/followup/generate.py` | `ad164c86b4a29b0a82c98fd1a4f60bda3303533723db77c101842a5cea00038c` |
| `dates/generate.py` | `e00e8aea91f4bf1a7502f4ce343fd57c9252fa21b84be90e623f8cda3dbe2787` |
| `dates/isolated/generate.py` | `b2375e16b08a31dad6218a5747fdb7b4c3814dbcb033d0db4a0047f49815be34` |
| `dates/reach/generate.py` | `c5667736e3086ee83e557ee9cc4ad9256af9448e8ded816353a38b5765853a31` |
| `dates/targeted/generate.py` | `499bdddfd754af9943f5425e658223a9cb51a801fda50045adb9d2c631fd0335` |
| `declared-refusals/generate.py` | `837ff4a8d297642a8278d3f8661212e61ffaca50d43a92df5432c0bafe98eb31` |
| `extended-writes/generate.py` | `a4bb65ef7e05a2260801bf071870b4c640e546378acc7f89fb6a575aebd01348` |
| `file-pins/generate.py` | `fa13b00344a11dd34b1493e5b1de2ae71c8ec8245557a6516695e11ee59152d1` |
| `files/generate.py` | `6c5e45fa22b58fc69a23ef85073b2a0c357a505fcb7eba24ba44c6d2f1cc3674` |
| `frame-methods/generate.py` | `02131ab8dc16e74f93c95a0096259225150e7e39917e646c7fce400582981735` |
| `frames-groups/generate.py` | `0ef3bda3990e3175a0b9529b7ae73dd68bb56a4e23335b8907d888c598f068da` |
| `frames-groups-bounds/generate.py` | `abcd0dc82346996b3c91ad56ba73acf4863789e787a4253075474fa9c2bc6058` |
| `frames-groups-writes/generate.py` | `f898781218c5318ed27261b05b205db59e1ec29e633ab4a58c61728734bf4646` |
| `inventory/generate.py` | `f9b8410703feb7b1f16a8dfff7f9d26d4af082522b9f423cbf21987ea5a22212` |
| `local-statements/generate.py` | `f62b05139ba95a1f750677775fc3452ad6eccee901255c045a8bfcb11b653d35` |
| `nested-pairs/generate.py` | `b032da9f1ab40b8b3eeb9cccb865ec39f3c940e858d001894ba957d3a8beef83` |
| `nested-writes/generate.py` | `b9a070ddc591833215ef1eaf9161deb5795119c85c3a5bbf40359e98cde70ac8` |
| `numbers/generate.py` | `7dae0ddff60178980e0338eef28e6b3183c17a14d46f5bcbcd48e0c2b1ffe0e5` |
| `parser-errors/generate.py` | `e0c45d8e324b330e83b4d6dcee258afa74f2aea48210a33fcd4317334d7cc02a` |
| `public-examples/generate.py` | `2d98c49ccf8960ddf904153e12532e7be73d55d29a147f5588d6c18407d7ad42` |
| `record-samples/generate.py` | `0f8a8582d7eb7b26ca40da48840c0de8c61b2d2758d8678a87bb15f04f8371d3` |
| `reference/generate.py` | `a62a29db0e3d12cda566f190633986546451f1cc066e43c67cd49632e39b97e7` |
| `refute-collections/generate.py` | `53bc3cae68fa260f9556eb06427ccbb75973289bf7bb2de8c6a5fe7cf2cdb7e0` |
| `refute-core/generate.py` | `be8a1972535e26f02502fb9d1f24a52a31ff094c04634f12eb686363ced43e86` |
| `refute-frames/generate.py` | `3fa2861a4c19e6357143f634999224d610b0c87c657ae38d6ff3309a4b741570` |
| `refute-prior/generate.py` | `b7a5803c506f4e661eb864314bb64054e7f6add11ed038948714b0ffe1e513c9` |
| `refute-seeded/generate.py` | `4958ab2b26c077e978e10593f91629533f3cf1feab82e791f0ee1f9c80e42bdc` |
| `refute-settings/generate.py` | `ab8483a58df1e6b3d593b7b539f7ba1242c267bb43471618ed0507d68d39aff2` |
| `refute-time/generate.py` | `7532f4ece8c6522fe629d526cdfe6a8c56825e6d5bc2d78931757f2fbc6390fb` |
| `rewrites/generate.py` | `207708354f07069cdae4dcdbbf7b9c3b37967cf1cd6be9cdbe066d531c09e6c9` |
| `rewrites-followup/generate.py` | `3f8716625c02a22e0ef2ee1f9d8bd6f53748850e3df713155bb8133e4a2d83ba` |
| `rewrites-pins/generate.py` | `2da05df219376d9b7fdcd8caafe682b7800eb057c5cedd412826d5bb9d8b951a` |
| `rewrites-prior/generate.py` | `e40622c0e175bc1cb40019e0ffed63cea67dfc6dbdd7400c3ea76553e4572fe1` |
| `rewrites-prior-reconcile/generate.py` | `cfe336e2932ac47a290cf6b4ecc442465ae3b114e0391208be9222ab08e844e2` |
| `rewrites-seats/generate.py` | `874a8ef93245894b6bbff2f4d37d160d833505249455b883900bb2c8e1fd768f` |
| `seeded/generate.py` | `982a5100aad874836c51ed7c26de1898276dcdc068eab61ded547a0ae7a4e9dc` |
| `seeded/operand-types/generate.py` | `64314c086c8744f2fc84673390c3ac6a5656946bcd9a688f0f498eec16ff3c5a` |
| `session-types/generate.py` | `760d9f7fd5c39856f23778ece055fef86c7e5776e65382abeee0a3f3a48929d7` |
| `settings/generate.py` | `e6551ccd209f8e50dfc4cc426abf8962e745f77a584f0ce97764a6881e073c15` |
| `settings-files/generate.py` | `8150222e987e9ea4d4a53c53f0aa5b0141bcf320b56bdc94048950400508b249` |
| `settings-residual/generate.py` | `995452196b71ad9a6ffe5cfdc0a1a445e308dd84986772154df35609c770bcd5` |
| `settings-writes/generate.py` | `ade964a84ec43ab80c8794a0baee7eead639dc8d68198fbe1fdc59fce2c9d094` |
| `sql/generate.py` | `adf9018b7cbdec4b4d2dbea7a360d1676cd9c2d523b15828275b30fa53dae723` |
| `strings-collections/generate.py` | `13ca9bdd4d27c95a0e8bb1bab02b561bccfc82ed115e1012b0327bfed6c31a3f` |
| `strings-collections-discriminants/generate.py` | `9f5faf9516a253dd43a211680043162ee7f3010beb7234a8e58741de4cbb5aab` |
| `strings-collections-final/generate.py` | `3ecfa6891cacbdd04979472884663f80dd19011fbe98acbb5b83a83b8188b85e` |
| `strings-collections-followup/generate.py` | `c89bfdce7478bb55c45098a002f2d1e404bb18579ea32ca2858955f12f505127` |
| `strings-collections-typed/generate.py` | `9fd6482bae596b8012c806ac1ab1bbbf1d18d1e3a2fd524c8505dc27e95f8b7a` |

## Pointers

- Up: [../map.md](../map.md)
- Iceberg is outside this corpus: [../../../../../../docs/spark-sql-iceberg-parity.md](../../../../../../docs/spark-sql-iceberg-parity.md).
