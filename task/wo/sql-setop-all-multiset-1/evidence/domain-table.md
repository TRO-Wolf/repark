# Domain table: Spark 4.1.2 against main `e80e6bc8`, the mutant and head

Every cell of `python/repark/tests/sql/_setop_all_multiset_1_cells.py` on the Spark door (ANSI on and off) and the native door (against Spark's ANSI-on answer). `mutant` is head with the rewrite rule not seated: the semi and anti join main plans under its refusal. A row shows `EQUAL` or the verdict and the answer.

| cell | mode | Spark 4.1.2 | main | mutant | head |
|---|---|---|---|---|---|
| control / EXCEPT | ANSI true | [[4]] | EQUAL | EQUAL | EQUAL |
| control / EXCEPT DISTINCT | ANSI true | [[4]] | EQUAL | EQUAL | EQUAL |
| control / INTERSECT | ANSI true | [[1], [2]] | EQUAL | EQUAL | EQUAL |
| control / INTERSECT DISTINCT | ANSI true | [[1], [2]] | EQUAL | EQUAL | EQUAL |
| control / UNION | ANSI true | [[1], [2], [3], [4]] | EQUAL | EQUAL | EQUAL |
| control / UNION ALL | ANSI true | [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]] | EQUAL | EQUAL | EQUAL |
| dataframe / bigint / exceptAll | ANSI true | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / bigint / except_all | ANSI true | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / bigint / intersectAll | ANSI true | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| dataframe / bigint / intersect_all | ANSI true | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| dataframe / columns / exceptAll | ANSI true | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| dataframe / columns / except_all | ANSI true | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| dataframe / columns / intersectAll | ANSI true | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| dataframe / columns / intersect_all | ANSI true | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| dataframe / date / exceptAll | ANSI true | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| dataframe / date / except_all | ANSI true | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| dataframe / date / intersectAll | ANSI true | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| dataframe / date / intersect_all | ANSI true | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| dataframe / decimal / exceptAll | ANSI true | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| dataframe / decimal / except_all | ANSI true | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| dataframe / decimal / intersectAll | ANSI true | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| dataframe / decimal / intersect_all | ANSI true | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| dataframe / double-signs / exceptAll | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / double-signs / except_all | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / double-signs / intersectAll | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / double-signs / intersect_all | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / double / exceptAll | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / double / except_all | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / double / intersectAll | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / double / intersect_all | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / float-signs / exceptAll | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / float-signs / except_all | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / float-signs / intersectAll | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / float-signs / intersect_all | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / float / exceptAll | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / float / except_all | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / float / intersectAll | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / float / intersect_all | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / int / exceptAll | ANSI true | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / int / except_all | ANSI true | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / int / intersectAll | ANSI true | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| dataframe / int / intersect_all | ANSI true | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| dataframe / null / exceptAll | ANSI true | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / null / except_all | ANSI true | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / null / intersectAll | ANSI true | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| dataframe / null / intersect_all | ANSI true | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| dataframe / string / exceptAll | ANSI true | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| dataframe / string / except_all | ANSI true | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| dataframe / string / intersectAll | ANSI true | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| dataframe / string / intersect_all | ANSI true | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| dataframe / struct / exceptAll | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| dataframe / struct / except_all | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| dataframe / struct / intersectAll | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| dataframe / struct / intersect_all | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| dataframe / timestamp / exceptAll | ANSI true | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| dataframe / timestamp / except_all | ANSI true | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| dataframe / timestamp / intersectAll | ANSI true | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| dataframe / timestamp / intersect_all | ANSI true | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| edge / both-empty / EXCEPT ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / both-empty / INTERSECT ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / both-empty / MINUS ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / EXCEPT ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / INTERSECT ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / MINUS ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / EXCEPT ALL / lr | ANSI true | [[1], [1], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / INTERSECT ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / MINUS ALL / lr | ANSI true | [[1], [1], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / EXCEPT ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / INTERSECT ALL / lr | ANSI true | [[7], [7]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / MINUS ALL / lr | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates / EXCEPT ALL / lr | ANSI true | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| edge / only-duplicates / INTERSECT ALL / lr | ANSI true | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [[7], [7], [7], [7]] | EQUAL |
| edge / only-duplicates / MINUS ALL / lr | ANSI true | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| error / column-count / EXCEPT ALL | ANSI true | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-count / INTERSECT ALL | ANSI true | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-count / MINUS ALL | ANSI true | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / EXCEPT ALL | ANSI true | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / INTERSECT ALL | ANSI true | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / MINUS ALL | ANSI true | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| fragment / fexpr-filter-col / EXCEPT ALL | ANSI true | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-filter-col / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| fragment / fexpr-filter-col / MINUS ALL | ANSI true | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-filter-in / EXCEPT ALL | ANSI true | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-filter-in / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| fragment / fexpr-filter-in / MINUS ALL | ANSI true | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-scalar / EXCEPT ALL | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| fragment / fexpr-scalar / INTERSECT ALL | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[5]] | EQUAL |
| fragment / fexpr-scalar / MINUS ALL | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| fragment / selectexpr-scalar / EXCEPT ALL | ANSI true | [[3], [3], [3], [3]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [1]] | EQUAL |
| fragment / selectexpr-scalar / INTERSECT ALL | ANSI true | [[3], [3], [3], [3]] | REFUSED: UnsupportedOperationException | WRONG: [[5], [5], [5], [5]] | EQUAL |
| fragment / selectexpr-scalar / MINUS ALL | ANSI true | [[3], [3], [3], [3]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [1]] | EQUAL |
| guard / df / decimal / anti | ANSI true | [["d2"]] | EQUAL | EQUAL | EQUAL |
| guard / df / decimal / semi | ANSI true | [["d1.1"], ["d1.1"], [null]] | EQUAL | EQUAL | EQUAL |
| guard / df / one / anti | ANSI true | [[2]] | EQUAL | EQUAL | EQUAL |
| guard / df / one / semi | ANSI true | [[1], [1], [null], [null]] | EQUAL | EQUAL | EQUAL |
| guard / df / two / anti | ANSI true | [[2, "y"]] | EQUAL | EQUAL | EQUAL |
| guard / df / two / semi | ANSI true | [[1, "x"], [1, "x"], [null, null], [null, null]] | EQUAL | EQUAL | EQUAL |
| guard / sql / decimal / anti | ANSI true | [["d2"]] | EQUAL | EQUAL | EQUAL |
| guard / sql / decimal / semi | ANSI true | [["d1.1"], ["d1.1"], [null]] | EQUAL | EQUAL | EQUAL |
| guard / sql / one / anti | ANSI true | [[2]] | EQUAL | EQUAL | EQUAL |
| guard / sql / one / semi | ANSI true | [[1], [1], [null], [null]] | EQUAL | EQUAL | EQUAL |
| guard / sql / two / anti | ANSI true | [[2, "y"]] | EQUAL | EQUAL | EQUAL |
| guard / sql / two / semi | ANSI true | [[1, "x"], [1, "x"], [null, null], [null, null]] | EQUAL | EQUAL | EQUAL |
| shape / ctas / EXCEPT ALL | ANSI true | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / ctas / INTERSECT ALL | ANSI true | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / ctas / MINUS ALL | ANSI true | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / cte / EXCEPT ALL | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / cte / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / cte / MINUS ALL | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / explain / EXCEPT ALL | ANSI true | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / explain / INTERSECT ALL | ANSI true | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / explain / MINUS ALL | ANSI true | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / iceberg-source / EXCEPT ALL | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / iceberg-source / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / iceberg-source / MINUS ALL | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / insert / EXCEPT ALL | ANSI true | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / insert / INTERSECT ALL | ANSI true | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / insert / MINUS ALL | ANSI true | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / in / EXCEPT ALL | ANSI true | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| shape / in / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / in / MINUS ALL | ANSI true | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| shape / merge / EXCEPT ALL | ANSI true | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / merge / INTERSECT ALL | ANSI true | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / merge / MINUS ALL | ANSI true | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / persistent-view / EXCEPT ALL | ANSI true | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / persistent-view / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: AnalysisException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / persistent-view / MINUS ALL | ANSI true | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / scalar / EXCEPT ALL | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| shape / scalar / INTERSECT ALL | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[5]] | EQUAL |
| shape / scalar / MINUS ALL | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| shape / temp-view / EXCEPT ALL | ANSI true | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / temp-view / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: AnalysisException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / temp-view / MINUS ALL | ANSI true | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / top / EXCEPT ALL | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / top / INTERSECT ALL | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / top / MINUS ALL | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / union-nested / EXCEPT ALL | ANSI true | [[1], [1], [4], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[4], [9]] | EQUAL |
| shape / union-nested / INTERSECT ALL | ANSI true | [[1], [2], [2], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [9]] | EQUAL |
| shape / union-nested / MINUS ALL | ANSI true | [[1], [1], [4], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[4], [9]] | EQUAL |
| spelling / 0 | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 1 | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 2 | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 3 | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 4 | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 5 | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 6 | ANSI true | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 7 | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 8 | ANSI true | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / EXCEPT ALL / lr | ANSI true | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / EXCEPT ALL / rl | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / bigint / INTERSECT ALL / lr | ANSI true | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| type / bigint / INTERSECT ALL / rl | ANSI true | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / bigint / MINUS ALL / lr | ANSI true | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / MINUS ALL / rl | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / columns / EXCEPT ALL / lr | ANSI true | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| type / columns / EXCEPT ALL / rl | ANSI true | [[1, null, "f0000000000000000"], [2, "a", "f7ff8000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [[1, null, "f0000000000000000"]] | EQUAL |
| type / columns / INTERSECT ALL / lr | ANSI true | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| type / columns / INTERSECT ALL / rl | ANSI true | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | EQUAL |
| type / columns / MINUS ALL / lr | ANSI true | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| type / columns / MINUS ALL / rl | ANSI true | [[1, null, "f0000000000000000"], [2, "a", "f7ff8000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [[1, null, "f0000000000000000"]] | EQUAL |
| type / date / EXCEPT ALL / lr | ANSI true | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| type / date / EXCEPT ALL / rl | ANSI true | [["t2000-01-01"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / date / INTERSECT ALL / lr | ANSI true | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| type / date / INTERSECT ALL / rl | ANSI true | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / date / MINUS ALL / lr | ANSI true | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| type / date / MINUS ALL / rl | ANSI true | [["t2000-01-01"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / EXCEPT ALL / lr | ANSI true | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| type / decimal / EXCEPT ALL / rl | ANSI true | [["d3"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / INTERSECT ALL / lr | ANSI true | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| type / decimal / INTERSECT ALL / rl | ANSI true | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / MINUS ALL / lr | ANSI true | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| type / decimal / MINUS ALL / rl | ANSI true | [["d3"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / EXCEPT ALL / lr | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / double-signs / EXCEPT ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / INTERSECT ALL / lr | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| type / double-signs / INTERSECT ALL / rl | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / MINUS ALL / lr | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / double-signs / MINUS ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double / EXCEPT ALL / lr | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / double / EXCEPT ALL / rl | ANSI true | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double / INTERSECT ALL / lr | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| type / double / INTERSECT ALL / rl | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f7ff8000000000000"], ["f8000000000000000"],  | EQUAL |
| type / double / MINUS ALL / lr | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / double / MINUS ALL / rl | ANSI true | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / EXCEPT ALL / lr | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / float-signs / EXCEPT ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / INTERSECT ALL / lr | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| type / float-signs / INTERSECT ALL / rl | ANSI true | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / MINUS ALL / lr | ANSI true | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / float-signs / MINUS ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float / EXCEPT ALL / lr | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / float / EXCEPT ALL / rl | ANSI true | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float / INTERSECT ALL / lr | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| type / float / INTERSECT ALL / rl | ANSI true | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f7ff8000000000000"], ["f8000000000000000"],  | EQUAL |
| type / float / MINUS ALL / lr | ANSI true | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / float / MINUS ALL / rl | ANSI true | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / EXCEPT ALL / lr | ANSI true | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / int / EXCEPT ALL / rl | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / INTERSECT ALL / lr | ANSI true | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| type / int / INTERSECT ALL / rl | ANSI true | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / MINUS ALL / lr | ANSI true | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / int / MINUS ALL / rl | ANSI true | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / EXCEPT ALL / lr | ANSI true | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / null / EXCEPT ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / INTERSECT ALL / lr | ANSI true | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| type / null / INTERSECT ALL / rl | ANSI true | [[null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / MINUS ALL / lr | ANSI true | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / null / MINUS ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / EXCEPT ALL / lr | ANSI true | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| type / string / EXCEPT ALL / rl | ANSI true | [[" a"], ["c"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / INTERSECT ALL / lr | ANSI true | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| type / string / INTERSECT ALL / rl | ANSI true | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / MINUS ALL / lr | ANSI true | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| type / string / MINUS ALL / rl | ANSI true | [[" a"], ["c"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / EXCEPT ALL / lr | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| type / struct / EXCEPT ALL / rl | ANSI true | [[[["x", 3], ["y", "c"]]]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / INTERSECT ALL / lr | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| type / struct / INTERSECT ALL / rl | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / MINUS ALL / lr | ANSI true | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| type / struct / MINUS ALL / rl | ANSI true | [[[["x", 3], ["y", "c"]]]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / EXCEPT ALL / lr | ANSI true | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| type / timestamp / EXCEPT ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / INTERSECT ALL / lr | ANSI true | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| type / timestamp / INTERSECT ALL / rl | ANSI true | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / MINUS ALL / lr | ANSI true | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| type / timestamp / MINUS ALL / rl | ANSI true | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| control / EXCEPT | ANSI false | [[4]] | EQUAL | EQUAL | EQUAL |
| control / EXCEPT DISTINCT | ANSI false | [[4]] | EQUAL | EQUAL | EQUAL |
| control / INTERSECT | ANSI false | [[1], [2]] | EQUAL | EQUAL | EQUAL |
| control / INTERSECT DISTINCT | ANSI false | [[1], [2]] | EQUAL | EQUAL | EQUAL |
| control / UNION | ANSI false | [[1], [2], [3], [4]] | EQUAL | EQUAL | EQUAL |
| control / UNION ALL | ANSI false | [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]] | EQUAL | EQUAL | EQUAL |
| dataframe / bigint / exceptAll | ANSI false | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / bigint / except_all | ANSI false | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / bigint / intersectAll | ANSI false | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| dataframe / bigint / intersect_all | ANSI false | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| dataframe / columns / exceptAll | ANSI false | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| dataframe / columns / except_all | ANSI false | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| dataframe / columns / intersectAll | ANSI false | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| dataframe / columns / intersect_all | ANSI false | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| dataframe / date / exceptAll | ANSI false | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| dataframe / date / except_all | ANSI false | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| dataframe / date / intersectAll | ANSI false | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| dataframe / date / intersect_all | ANSI false | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| dataframe / decimal / exceptAll | ANSI false | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| dataframe / decimal / except_all | ANSI false | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| dataframe / decimal / intersectAll | ANSI false | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| dataframe / decimal / intersect_all | ANSI false | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| dataframe / double-signs / exceptAll | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / double-signs / except_all | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / double-signs / intersectAll | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / double-signs / intersect_all | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / double / exceptAll | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / double / except_all | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / double / intersectAll | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / double / intersect_all | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / float-signs / exceptAll | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / float-signs / except_all | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / float-signs / intersectAll | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / float-signs / intersect_all | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| dataframe / float / exceptAll | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / float / except_all | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| dataframe / float / intersectAll | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / float / intersect_all | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| dataframe / int / exceptAll | ANSI false | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / int / except_all | ANSI false | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| dataframe / int / intersectAll | ANSI false | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| dataframe / int / intersect_all | ANSI false | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| dataframe / null / exceptAll | ANSI false | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / null / except_all | ANSI false | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| dataframe / null / intersectAll | ANSI false | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| dataframe / null / intersect_all | ANSI false | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| dataframe / string / exceptAll | ANSI false | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| dataframe / string / except_all | ANSI false | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| dataframe / string / intersectAll | ANSI false | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| dataframe / string / intersect_all | ANSI false | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| dataframe / struct / exceptAll | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| dataframe / struct / except_all | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| dataframe / struct / intersectAll | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| dataframe / struct / intersect_all | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| dataframe / timestamp / exceptAll | ANSI false | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| dataframe / timestamp / except_all | ANSI false | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| dataframe / timestamp / intersectAll | ANSI false | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| dataframe / timestamp / intersect_all | ANSI false | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| edge / both-empty / EXCEPT ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / both-empty / INTERSECT ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / both-empty / MINUS ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / EXCEPT ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / INTERSECT ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / MINUS ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / EXCEPT ALL / lr | ANSI false | [[1], [1], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / INTERSECT ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / MINUS ALL / lr | ANSI false | [[1], [1], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / EXCEPT ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / INTERSECT ALL / lr | ANSI false | [[7], [7]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / MINUS ALL / lr | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates / EXCEPT ALL / lr | ANSI false | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| edge / only-duplicates / INTERSECT ALL / lr | ANSI false | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [[7], [7], [7], [7]] | EQUAL |
| edge / only-duplicates / MINUS ALL / lr | ANSI false | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| error / column-count / EXCEPT ALL | ANSI false | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-count / INTERSECT ALL | ANSI false | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-count / MINUS ALL | ANSI false | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / EXCEPT ALL | ANSI false | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / INTERSECT ALL | ANSI false | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / MINUS ALL | ANSI false | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| fragment / fexpr-filter-col / EXCEPT ALL | ANSI false | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-filter-col / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| fragment / fexpr-filter-col / MINUS ALL | ANSI false | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-filter-in / EXCEPT ALL | ANSI false | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-filter-in / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| fragment / fexpr-filter-in / MINUS ALL | ANSI false | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| fragment / fexpr-scalar / EXCEPT ALL | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| fragment / fexpr-scalar / INTERSECT ALL | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[5]] | EQUAL |
| fragment / fexpr-scalar / MINUS ALL | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| fragment / selectexpr-scalar / EXCEPT ALL | ANSI false | [[3], [3], [3], [3]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [1]] | EQUAL |
| fragment / selectexpr-scalar / INTERSECT ALL | ANSI false | [[3], [3], [3], [3]] | REFUSED: UnsupportedOperationException | WRONG: [[5], [5], [5], [5]] | EQUAL |
| fragment / selectexpr-scalar / MINUS ALL | ANSI false | [[3], [3], [3], [3]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [1]] | EQUAL |
| guard / df / decimal / anti | ANSI false | [["d2"]] | EQUAL | EQUAL | EQUAL |
| guard / df / decimal / semi | ANSI false | [["d1.1"], ["d1.1"], [null]] | EQUAL | EQUAL | EQUAL |
| guard / df / one / anti | ANSI false | [[2]] | EQUAL | EQUAL | EQUAL |
| guard / df / one / semi | ANSI false | [[1], [1], [null], [null]] | EQUAL | EQUAL | EQUAL |
| guard / df / two / anti | ANSI false | [[2, "y"]] | EQUAL | EQUAL | EQUAL |
| guard / df / two / semi | ANSI false | [[1, "x"], [1, "x"], [null, null], [null, null]] | EQUAL | EQUAL | EQUAL |
| guard / sql / decimal / anti | ANSI false | [["d2"]] | EQUAL | EQUAL | EQUAL |
| guard / sql / decimal / semi | ANSI false | [["d1.1"], ["d1.1"], [null]] | EQUAL | EQUAL | EQUAL |
| guard / sql / one / anti | ANSI false | [[2]] | EQUAL | EQUAL | EQUAL |
| guard / sql / one / semi | ANSI false | [[1], [1], [null], [null]] | EQUAL | EQUAL | EQUAL |
| guard / sql / two / anti | ANSI false | [[2, "y"]] | EQUAL | EQUAL | EQUAL |
| guard / sql / two / semi | ANSI false | [[1, "x"], [1, "x"], [null, null], [null, null]] | EQUAL | EQUAL | EQUAL |
| shape / ctas / EXCEPT ALL | ANSI false | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / ctas / INTERSECT ALL | ANSI false | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / ctas / MINUS ALL | ANSI false | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / cte / EXCEPT ALL | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / cte / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / cte / MINUS ALL | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / explain / EXCEPT ALL | ANSI false | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / explain / INTERSECT ALL | ANSI false | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / explain / MINUS ALL | ANSI false | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / iceberg-source / EXCEPT ALL | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / iceberg-source / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / iceberg-source / MINUS ALL | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / insert / EXCEPT ALL | ANSI false | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / insert / INTERSECT ALL | ANSI false | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / insert / MINUS ALL | ANSI false | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / in / EXCEPT ALL | ANSI false | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| shape / in / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / in / MINUS ALL | ANSI false | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| shape / merge / EXCEPT ALL | ANSI false | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / merge / INTERSECT ALL | ANSI false | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / merge / MINUS ALL | ANSI false | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / persistent-view / EXCEPT ALL | ANSI false | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / persistent-view / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: AnalysisException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / persistent-view / MINUS ALL | ANSI false | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / scalar / EXCEPT ALL | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| shape / scalar / INTERSECT ALL | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[5]] | EQUAL |
| shape / scalar / MINUS ALL | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| shape / temp-view / EXCEPT ALL | ANSI false | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / temp-view / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: AnalysisException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / temp-view / MINUS ALL | ANSI false | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / top / EXCEPT ALL | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / top / INTERSECT ALL | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / top / MINUS ALL | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / union-nested / EXCEPT ALL | ANSI false | [[1], [1], [4], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[4], [9]] | EQUAL |
| shape / union-nested / INTERSECT ALL | ANSI false | [[1], [2], [2], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [9]] | EQUAL |
| shape / union-nested / MINUS ALL | ANSI false | [[1], [1], [4], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[4], [9]] | EQUAL |
| spelling / 0 | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 1 | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 2 | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 3 | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 4 | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 5 | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 6 | ANSI false | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 7 | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 8 | ANSI false | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / EXCEPT ALL / lr | ANSI false | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / EXCEPT ALL / rl | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / bigint / INTERSECT ALL / lr | ANSI false | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| type / bigint / INTERSECT ALL / rl | ANSI false | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / bigint / MINUS ALL / lr | ANSI false | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / MINUS ALL / rl | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / columns / EXCEPT ALL / lr | ANSI false | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| type / columns / EXCEPT ALL / rl | ANSI false | [[1, null, "f0000000000000000"], [2, "a", "f7ff8000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [[1, null, "f0000000000000000"]] | EQUAL |
| type / columns / INTERSECT ALL / lr | ANSI false | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| type / columns / INTERSECT ALL / rl | ANSI false | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | EQUAL |
| type / columns / MINUS ALL / lr | ANSI false | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| type / columns / MINUS ALL / rl | ANSI false | [[1, null, "f0000000000000000"], [2, "a", "f7ff8000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [[1, null, "f0000000000000000"]] | EQUAL |
| type / date / EXCEPT ALL / lr | ANSI false | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| type / date / EXCEPT ALL / rl | ANSI false | [["t2000-01-01"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / date / INTERSECT ALL / lr | ANSI false | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| type / date / INTERSECT ALL / rl | ANSI false | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / date / MINUS ALL / lr | ANSI false | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| type / date / MINUS ALL / rl | ANSI false | [["t2000-01-01"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / EXCEPT ALL / lr | ANSI false | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| type / decimal / EXCEPT ALL / rl | ANSI false | [["d3"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / INTERSECT ALL / lr | ANSI false | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| type / decimal / INTERSECT ALL / rl | ANSI false | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / MINUS ALL / lr | ANSI false | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| type / decimal / MINUS ALL / rl | ANSI false | [["d3"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / EXCEPT ALL / lr | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / double-signs / EXCEPT ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / INTERSECT ALL / lr | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| type / double-signs / INTERSECT ALL / rl | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / MINUS ALL / lr | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / double-signs / MINUS ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double / EXCEPT ALL / lr | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / double / EXCEPT ALL / rl | ANSI false | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double / INTERSECT ALL / lr | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| type / double / INTERSECT ALL / rl | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f7ff8000000000000"], ["f8000000000000000"],  | EQUAL |
| type / double / MINUS ALL / lr | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / double / MINUS ALL / rl | ANSI false | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / EXCEPT ALL / lr | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / float-signs / EXCEPT ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / INTERSECT ALL / lr | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| type / float-signs / INTERSECT ALL / rl | ANSI false | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / MINUS ALL / lr | ANSI false | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / float-signs / MINUS ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float / EXCEPT ALL / lr | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / float / EXCEPT ALL / rl | ANSI false | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float / INTERSECT ALL / lr | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| type / float / INTERSECT ALL / rl | ANSI false | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f7ff8000000000000"], ["f8000000000000000"],  | EQUAL |
| type / float / MINUS ALL / lr | ANSI false | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / float / MINUS ALL / rl | ANSI false | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / EXCEPT ALL / lr | ANSI false | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / int / EXCEPT ALL / rl | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / INTERSECT ALL / lr | ANSI false | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| type / int / INTERSECT ALL / rl | ANSI false | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / MINUS ALL / lr | ANSI false | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / int / MINUS ALL / rl | ANSI false | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / EXCEPT ALL / lr | ANSI false | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / null / EXCEPT ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / INTERSECT ALL / lr | ANSI false | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| type / null / INTERSECT ALL / rl | ANSI false | [[null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / MINUS ALL / lr | ANSI false | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / null / MINUS ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / EXCEPT ALL / lr | ANSI false | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| type / string / EXCEPT ALL / rl | ANSI false | [[" a"], ["c"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / INTERSECT ALL / lr | ANSI false | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| type / string / INTERSECT ALL / rl | ANSI false | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / MINUS ALL / lr | ANSI false | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| type / string / MINUS ALL / rl | ANSI false | [[" a"], ["c"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / EXCEPT ALL / lr | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| type / struct / EXCEPT ALL / rl | ANSI false | [[[["x", 3], ["y", "c"]]]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / INTERSECT ALL / lr | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| type / struct / INTERSECT ALL / rl | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / MINUS ALL / lr | ANSI false | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| type / struct / MINUS ALL / rl | ANSI false | [[[["x", 3], ["y", "c"]]]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / EXCEPT ALL / lr | ANSI false | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| type / timestamp / EXCEPT ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / INTERSECT ALL / lr | ANSI false | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| type / timestamp / INTERSECT ALL / rl | ANSI false | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / MINUS ALL / lr | ANSI false | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| type / timestamp / MINUS ALL / rl | ANSI false | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| control / EXCEPT | native | [[4]] | EQUAL | EQUAL | EQUAL |
| control / EXCEPT DISTINCT | native | [[4]] | EQUAL | EQUAL | EQUAL |
| control / INTERSECT | native | [[1], [2]] | EQUAL | EQUAL | EQUAL |
| control / INTERSECT DISTINCT | native | [[1], [2]] | EQUAL | EQUAL | EQUAL |
| control / UNION | native | [[1], [2], [3], [4]] | EQUAL | EQUAL | EQUAL |
| control / UNION ALL | native | [[1], [1], [1], [1], [2], [2], [2], [2], [3], [4]] | EQUAL | EQUAL | EQUAL |
| edge / both-empty / EXCEPT ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / both-empty / INTERSECT ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / both-empty / MINUS ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| edge / empty-left / EXCEPT ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / INTERSECT ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-left / MINUS ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| edge / empty-right / EXCEPT ALL / lr | native | [[1], [1], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / INTERSECT ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / empty-right / MINUS ALL / lr | native | [[1], [1], [2]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| edge / only-duplicates-right-more / EXCEPT ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / INTERSECT ALL / lr | native | [[7], [7]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| edge / only-duplicates-right-more / MINUS ALL / lr | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| edge / only-duplicates / EXCEPT ALL / lr | native | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| edge / only-duplicates / INTERSECT ALL / lr | native | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [[7], [7], [7], [7]] | EQUAL |
| edge / only-duplicates / MINUS ALL / lr | native | [[7], [7]] | REFUSED: UnsupportedOperationException | WRONG: [] | REFUSED: UnsupportedOperationException |
| error / column-count / EXCEPT ALL | native | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-count / INTERSECT ALL | native | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-count / MINUS ALL | native | AnalysisException NUM_COLUMNS_MISMATCH 42826 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / EXCEPT ALL | native | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / INTERSECT ALL | native | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-CLASS: AnalysisException |
| error / column-type / MINUS ALL | native | AnalysisException INCOMPATIBLE_COLUMN_TYPE 42825 | ERR-DIFF: UnsupportedOperationException | ERR-CLASS: AnalysisException | ERR-DIFF: UnsupportedOperationException |
| shape / ctas / EXCEPT ALL | native | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException |
| shape / ctas / INTERSECT ALL | native | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException |
| shape / ctas / MINUS ALL | native | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException |
| shape / cte / EXCEPT ALL | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / cte / INTERSECT ALL | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / cte / MINUS ALL | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| shape / explain / EXCEPT ALL | native | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / explain / INTERSECT ALL | native | "rows" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / explain / MINUS ALL | native | "rows" | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| shape / iceberg-source / EXCEPT ALL | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / iceberg-source / INTERSECT ALL | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / iceberg-source / MINUS ALL | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| shape / insert / EXCEPT ALL | native | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | EQUAL |
| shape / insert / INTERSECT ALL | native | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[1], [1], [1], [2], [2]] | EQUAL |
| shape / insert / MINUS ALL | native | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | WRONG: stored [[4]] | REFUSED: refused UnsupportedOperationException |
| shape / in / EXCEPT ALL | native | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| shape / in / INTERSECT ALL | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| shape / in / MINUS ALL | native | [[1]] | REFUSED: UnsupportedOperationException | WRONG: [] | REFUSED: UnsupportedOperationException |
| shape / merge / EXCEPT ALL | native | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException |
| shape / merge / INTERSECT ALL | native | stored [[1], [2], [2]] | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException |
| shape / merge / MINUS ALL | native | stored [[1], [1], [4]] | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException | REFUSED: refused UnsupportedOperationException |
| shape / persistent-view / EXCEPT ALL | native | [[1], [1], [4]] | REFUSED: AnalysisException | REFUSED: AnalysisException | REFUSED: AnalysisException |
| shape / persistent-view / INTERSECT ALL | native | [[1], [2], [2]] | REFUSED: AnalysisException | REFUSED: AnalysisException | REFUSED: AnalysisException |
| shape / persistent-view / MINUS ALL | native | [[1], [1], [4]] | REFUSED: AnalysisException | REFUSED: AnalysisException | REFUSED: AnalysisException |
| shape / scalar / EXCEPT ALL | native | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | EQUAL |
| shape / scalar / INTERSECT ALL | native | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[5]] | EQUAL |
| shape / scalar / MINUS ALL | native | [[3]] | REFUSED: UnsupportedOperationException | WRONG: [[1]] | REFUSED: UnsupportedOperationException |
| shape / temp-view / EXCEPT ALL | native | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | EQUAL |
| shape / temp-view / INTERSECT ALL | native | [[1], [2], [2]] | REFUSED: AnalysisException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / temp-view / MINUS ALL | native | [[1], [1], [4]] | REFUSED: AnalysisException | WRONG: [[4]] | REFUSED: AnalysisException |
| shape / top / EXCEPT ALL | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| shape / top / INTERSECT ALL | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| shape / top / MINUS ALL | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| shape / union-nested / EXCEPT ALL | native | [[1], [1], [4], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[4], [9]] | EQUAL |
| shape / union-nested / INTERSECT ALL | native | [[1], [2], [2], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [9]] | EQUAL |
| shape / union-nested / MINUS ALL | native | [[1], [1], [4], [9]] | REFUSED: UnsupportedOperationException | WRONG: [[4], [9]] | REFUSED: UnsupportedOperationException |
| spelling / 0 | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 1 | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 2 | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 3 | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 4 | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| spelling / 5 | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| spelling / 6 | native | [[1], [2], [2]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2]] | EQUAL |
| spelling / 7 | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| spelling / 8 | native | [[1], [1], [4]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| type / bigint / EXCEPT ALL / lr | native | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / bigint / EXCEPT ALL / rl | native | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / bigint / INTERSECT ALL / lr | native | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[-7], [-7], [9000000000], [9000000000], [9000000000], [null], [null], | EQUAL |
| type / bigint / INTERSECT ALL / rl | native | [[-7], [-7], [9000000000], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / bigint / MINUS ALL / lr | native | [[4], [9000000000], [9000000000], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| type / bigint / MINUS ALL / rl | native | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / columns / EXCEPT ALL / lr | native | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | EQUAL |
| type / columns / EXCEPT ALL / rl | native | [[1, null, "f0000000000000000"], [2, "a", "f7ff8000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [[1, null, "f0000000000000000"]] | EQUAL |
| type / columns / INTERSECT ALL / lr | native | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [1, "a" | EQUAL |
| type / columns / INTERSECT ALL / rl | native | [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | REFUSED: UnsupportedOperationException | WRONG: [[1, "a", "f0000000000000000"], [1, "a", "f0000000000000000"], [2, "a" | EQUAL |
| type / columns / MINUS ALL / lr | native | [[1, "a", "f0000000000000000"], [1, "b", "f0000000000000000"], [null,  | REFUSED: UnsupportedOperationException | WRONG: [[1, "b", "f0000000000000000"]] | REFUSED: UnsupportedOperationException |
| type / columns / MINUS ALL / rl | native | [[1, null, "f0000000000000000"], [2, "a", "f7ff8000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [[1, null, "f0000000000000000"]] | REFUSED: UnsupportedOperationException |
| type / date / EXCEPT ALL / lr | native | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | EQUAL |
| type / date / EXCEPT ALL / rl | native | [["t2000-01-01"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / date / INTERSECT ALL / lr | native | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], ["t2024-01-01"], [ | EQUAL |
| type / date / INTERSECT ALL / rl | native | [["t1970-01-01"], ["t1970-01-01"], ["t2024-01-01"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / date / MINUS ALL / lr | native | [["t2024-01-01"], ["t2024-01-01"], ["t2024-02-29"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29"]] | REFUSED: UnsupportedOperationException |
| type / date / MINUS ALL / rl | native | [["t2000-01-01"]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / decimal / EXCEPT ALL / lr | native | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | EQUAL |
| type / decimal / EXCEPT ALL / rl | native | [["d3"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / INTERSECT ALL / lr | native | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d-2.2"], ["d-2.2"], ["d1.1"], ["d1.1"], ["d1.1"], [null], [null], [ | EQUAL |
| type / decimal / INTERSECT ALL / rl | native | [["d-2.2"], ["d-2.2"], ["d1.1"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / decimal / MINUS ALL / lr | native | [["d1.1"], ["d1.1"], ["d4"], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["d4"]] | REFUSED: UnsupportedOperationException |
| type / decimal / MINUS ALL / rl | native | [["d3"]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / double-signs / EXCEPT ALL / lr | native | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / double-signs / EXCEPT ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / INTERSECT ALL / lr | native | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| type / double-signs / INTERSECT ALL / rl | native | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double-signs / MINUS ALL / lr | native | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | REFUSED: UnsupportedOperationException |
| type / double-signs / MINUS ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / double / EXCEPT ALL / lr | native | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / double / EXCEPT ALL / rl | native | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / double / INTERSECT ALL / lr | native | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| type / double / INTERSECT ALL / rl | native | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f7ff8000000000000"], ["f8000000000000000"],  | EQUAL |
| type / double / MINUS ALL / lr | native | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | REFUSED: UnsupportedOperationException |
| type / double / MINUS ALL / rl | native | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / float-signs / EXCEPT ALL / lr | native | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / float-signs / EXCEPT ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / INTERSECT ALL / lr | native | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [["f8000000000000000"], ["f8000000000000000"], ["f8000000000000000"]] | EQUAL |
| type / float-signs / INTERSECT ALL / rl | native | [["f0000000000000000"], ["f0000000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float-signs / MINUS ALL / lr | native | [["f0000000000000000"]] | REFUSED: UnsupportedOperationException | WRONG: [] | REFUSED: UnsupportedOperationException |
| type / float-signs / MINUS ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / float / EXCEPT ALL / lr | native | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | EQUAL |
| type / float / EXCEPT ALL / rl | native | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / float / INTERSECT ALL / lr | native | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | EQUAL |
| type / float / INTERSECT ALL / rl | native | [["f0000000000000000"], ["f0000000000000000"], ["f7ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f0000000000000000"], ["f7ff8000000000000"], ["f8000000000000000"],  | EQUAL |
| type / float / MINUS ALL / lr | native | [["f0000000000000000"], ["f3ff8000000000000"], ["f3ff8000000000000"],  | REFUSED: UnsupportedOperationException | WRONG: [["f3ff8000000000000"], ["f3ff8000000000000"]] | REFUSED: UnsupportedOperationException |
| type / float / MINUS ALL / rl | native | [["f4004000000000000"]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / int / EXCEPT ALL / lr | native | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | EQUAL |
| type / int / EXCEPT ALL / rl | native | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / INTERSECT ALL / lr | native | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[1], [1], [1], [2], [2], [null], [null], [null]] | EQUAL |
| type / int / INTERSECT ALL / rl | native | [[1], [2], [2], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / int / MINUS ALL / lr | native | [[1], [1], [4], [null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[4]] | REFUSED: UnsupportedOperationException |
| type / int / MINUS ALL / rl | native | [[3]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / null / EXCEPT ALL / lr | native | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | EQUAL |
| type / null / EXCEPT ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / INTERSECT ALL / lr | native | [[null]] | REFUSED: UnsupportedOperationException | WRONG: [[null], [null], [null]] | EQUAL |
| type / null / INTERSECT ALL / rl | native | [[null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / null / MINUS ALL / lr | native | [[null], [null]] | REFUSED: UnsupportedOperationException | WRONG: [] | REFUSED: UnsupportedOperationException |
| type / null / MINUS ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / string / EXCEPT ALL / lr | native | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | EQUAL |
| type / string / EXCEPT ALL / rl | native | [[" a"], ["c"]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / INTERSECT ALL / lr | native | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [["a"], ["a"], ["a"], ["b"], ["b"], [null], [null]] | EQUAL |
| type / string / INTERSECT ALL / rl | native | [["a"], ["b"], ["b"], [null]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / string / MINUS ALL / lr | native | [[""], ["A"], ["a"], ["a"], [null]] | REFUSED: UnsupportedOperationException | WRONG: [[""], ["A"]] | REFUSED: UnsupportedOperationException |
| type / string / MINUS ALL / rl | native | [[" a"], ["c"]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / struct / EXCEPT ALL / lr | native | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | EQUAL |
| type / struct / EXCEPT ALL / rl | native | [[[["x", 3], ["y", "c"]]]] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / INTERSECT ALL / lr | native | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", 1], ["y", | EQUAL |
| type / struct / INTERSECT ALL / rl | native | [[[["x", 1], ["y", "a"]]], [[["x", 2], ["y", null]]], [[["x", 2], ["y" | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / struct / MINUS ALL / lr | native | [[[["x", 1], ["y", "a"]]], [[["x", 1], ["y", "a"]]], [[["x", null], [" | REFUSED: UnsupportedOperationException | WRONG: [[[["x", null], ["y", "b"]]]] | REFUSED: UnsupportedOperationException |
| type / struct / MINUS ALL / rl | native | [[[["x", 3], ["y", "c"]]]] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
| type / timestamp / EXCEPT ALL / lr | native | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | EQUAL |
| type / timestamp / EXCEPT ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / INTERSECT ALL / lr | native | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | WRONG: [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | EQUAL |
| type / timestamp / INTERSECT ALL / rl | native | [["t1970-01-01T00:00:00"], ["t1970-01-01T00:00:00"], ["t2024-01-01T10: | REFUSED: UnsupportedOperationException | EQUAL | EQUAL |
| type / timestamp / MINUS ALL / lr | native | [["t2024-01-01T10:00:00"], ["t2024-01-01T10:00:00"], ["t2024-02-29T23: | REFUSED: UnsupportedOperationException | WRONG: [["t2024-02-29T23:59:59.123456"]] | REFUSED: UnsupportedOperationException |
| type / timestamp / MINUS ALL / rl | native | [] | REFUSED: UnsupportedOperationException | EQUAL | REFUSED: UnsupportedOperationException |
