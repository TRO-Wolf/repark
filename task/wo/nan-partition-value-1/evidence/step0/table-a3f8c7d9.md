## Writes

| Cell | RePark on its own table | Spark on its own table | Verdict |
|---|---|---|---|
| `rows` DOUBLE part (insert) | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` DOUBLE part (insert) | [] | [[3]] | DIFFERS |
| `eqnan` DOUBLE part (insert) | [] | [[3]] | DIFFERS |
| `gt1` DOUBLE part (insert) | [[5]] | [[5]] | EQUAL |
| `isnull` DOUBLE part (insert) | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE part (insert) | [[5]] | [[5]] | EQUAL |
| `files` DOUBLE part (insert) | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `partitions` DOUBLE part (insert) | [[{"d": null}, 2, 2], [{"d": 0.0}, 2, 1], [{"d": 1.5}, 1, 1]] | — | — |
| `rows` DOUBLE flat (insert) | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` DOUBLE flat (insert) | [[3]] | [[3]] | EQUAL |
| `eqnan` DOUBLE flat (insert) | [[3]] | [[3]] | EQUAL |
| `gt1` DOUBLE flat (insert) | [[3], [5]] | [[3], [5]] | EQUAL |
| `isnull` DOUBLE flat (insert) | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE flat (insert) | [[5]] | [[5]] | EQUAL |
| `rows` FLOAT part (insert) | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` FLOAT part (insert) | [] | [[3]] | DIFFERS |
| `eqnan` FLOAT part (insert) | [] | [[3]] | DIFFERS |
| `gt1` FLOAT part (insert) | [[5]] | [[5]] | EQUAL |
| `isnull` FLOAT part (insert) | [[4]] | [[4]] | EQUAL |
| `count` FLOAT part (insert) | [[5]] | [[5]] | EQUAL |
| `files` FLOAT part (insert) | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `partitions` FLOAT part (insert) | [[{"d": null}, 2, 2], [{"d": 0.0}, 2, 1], [{"d": 1.5}, 1, 1]] | — | — |
| `rows` FLOAT flat (insert) | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` FLOAT flat (insert) | [[3]] | [[3]] | EQUAL |
| `eqnan` FLOAT flat (insert) | [[3]] | [[3]] | EQUAL |
| `gt1` FLOAT flat (insert) | [[3], [5]] | [[3], [5]] | EQUAL |
| `isnull` FLOAT flat (insert) | [[4]] | [[4]] | EQUAL |
| `count` FLOAT flat (insert) | [[5]] | [[5]] | EQUAL |
| `isnan` DOUBLE ctas (ctas) | [[3]] | [[3]] | EQUAL |
| `files` DOUBLE ctas (ctas) | [[{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1], [{"d": "NaN"}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | EQUAL |
| `isnan` DOUBLE writeto (writeto) | [] | [[3]] | DIFFERS |
| `files` DOUBLE writeto (writeto) | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `isnan` DOUBLE saveastable (saveastable) | [] | [[3]] | DIFFERS |
| `files` DOUBLE saveastable (saveastable) | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `isnan` FLOAT ctas (ctas) | [[3]] | [[3]] | EQUAL |
| `files` FLOAT ctas (ctas) | [[{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1], [{"d": "NaN"}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | EQUAL |
| `isnan` FLOAT writeto (writeto) | [] | [[3]] | DIFFERS |
| `files` FLOAT writeto (writeto) | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `isnan` FLOAT saveastable (saveastable) | [] | [[3]] | DIFFERS |
| `files` FLOAT saveastable (saveastable) | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 0.0}, 2], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` DOUBLE inf (insert) | [[6, "Infinity"], [7, "-Infinity"]] | [[6, "Infinity"], [7, "-Infinity"]] | EQUAL |
| `files` DOUBLE inf (insert) | [[{"d": null}, 1], [{"d": null}, 1]] | [[{"d": "-Infinity"}, 1], [{"d": "Infinity"}, 1]] | DIFFERS |
| `rows` INT part (insert) | [[1, 7], [2, null]] | [[1, 7], [2, null]] | EQUAL |
| `isnull` INT part (insert) | [[2]] | — | — |
| `files` INT part (insert) | [[{"d": 7}, 1], [{"d": null}, 1]] | [[{"d": null}, 1], [{"d": 7}, 1]] | EQUAL |
| `rows` STRING part (insert) | [[1, "a"], [2, null]] | [[1, "a"], [2, null]] | EQUAL |
| `isnull` STRING part (insert) | [[2]] | — | — |
| `files` STRING part (insert) | [[{"d": null}, 1], [{"d": "a"}, 1]] | [[{"d": null}, 1], [{"d": "a"}, 1]] | EQUAL |

## Spark reading RePark's tables

| Cell | Spark reading RePark's table | Spark on its own table | Verdict |
|---|---|---|---|
| `rows` DOUBLE ctas | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` DOUBLE ctas | [[3]] | [[3]] | EQUAL |
| `eqnan` DOUBLE ctas | [[3]] | [[3]] | EQUAL |
| `gt1` DOUBLE ctas | [[5]] | [[5]] | EQUAL |
| `isnull` DOUBLE ctas | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE ctas | [[5]] | [[5]] | EQUAL |
| `files` DOUBLE ctas | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | EQUAL |
| `rows` DOUBLE flat | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` DOUBLE flat | [[3]] | [[3]] | EQUAL |
| `eqnan` DOUBLE flat | [[3]] | [[3]] | EQUAL |
| `gt1` DOUBLE flat | [[3], [5]] | [[3], [5]] | EQUAL |
| `isnull` DOUBLE flat | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE flat | [[5]] | [[5]] | EQUAL |
| `rows` DOUBLE inf | [[6, null], [7, null]] | [[6, "Infinity"], [7, "-Infinity"]] | DIFFERS |
| `files` DOUBLE inf | [[{"d": null}, 1], [{"d": null}, 1]] | [[{"d": "-Infinity"}, 1], [{"d": "Infinity"}, 1]] | DIFFERS |
| `rows` DOUBLE part | [[1, 0.0], [2, 0.0], [3, null], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | DIFFERS |
| `isnan` DOUBLE part | [] | [[3]] | DIFFERS |
| `eqnan` DOUBLE part | [] | [[3]] | DIFFERS |
| `gt1` DOUBLE part | [[5]] | [[5]] | EQUAL |
| `isnull` DOUBLE part | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE part | [[5]] | [[5]] | EQUAL |
| `files` DOUBLE part | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` DOUBLE saveastable | [[1, 0.0], [2, 0.0], [3, null], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | DIFFERS |
| `isnan` DOUBLE saveastable | [] | [[3]] | DIFFERS |
| `eqnan` DOUBLE saveastable | [] | [[3]] | DIFFERS |
| `gt1` DOUBLE saveastable | [[5]] | [[5]] | EQUAL |
| `isnull` DOUBLE saveastable | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE saveastable | [[5]] | [[5]] | EQUAL |
| `files` DOUBLE saveastable | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` DOUBLE writeto | [[1, 0.0], [2, 0.0], [3, null], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | DIFFERS |
| `isnan` DOUBLE writeto | [] | [[3]] | DIFFERS |
| `eqnan` DOUBLE writeto | [] | [[3]] | DIFFERS |
| `gt1` DOUBLE writeto | [[5]] | [[5]] | EQUAL |
| `isnull` DOUBLE writeto | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE writeto | [[5]] | [[5]] | EQUAL |
| `files` DOUBLE writeto | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` FLOAT ctas | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` FLOAT ctas | [[3]] | [[3]] | EQUAL |
| `eqnan` FLOAT ctas | [[3]] | [[3]] | EQUAL |
| `gt1` FLOAT ctas | [[5]] | [[5]] | EQUAL |
| `isnull` FLOAT ctas | [[4]] | [[4]] | EQUAL |
| `count` FLOAT ctas | [[5]] | [[5]] | EQUAL |
| `files` FLOAT ctas | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | EQUAL |
| `rows` FLOAT flat | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` FLOAT flat | [[3]] | [[3]] | EQUAL |
| `eqnan` FLOAT flat | [[3]] | [[3]] | EQUAL |
| `gt1` FLOAT flat | [[3], [5]] | [[3], [5]] | EQUAL |
| `isnull` FLOAT flat | [[4]] | [[4]] | EQUAL |
| `count` FLOAT flat | [[5]] | [[5]] | EQUAL |
| `rows` FLOAT part | [[1, 0.0], [2, 0.0], [3, null], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | DIFFERS |
| `isnan` FLOAT part | [] | [[3]] | DIFFERS |
| `eqnan` FLOAT part | [] | [[3]] | DIFFERS |
| `gt1` FLOAT part | [[5]] | [[5]] | EQUAL |
| `isnull` FLOAT part | [[4]] | [[4]] | EQUAL |
| `count` FLOAT part | [[5]] | [[5]] | EQUAL |
| `files` FLOAT part | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` FLOAT saveastable | [[1, 0.0], [2, 0.0], [3, null], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | DIFFERS |
| `isnan` FLOAT saveastable | [] | [[3]] | DIFFERS |
| `eqnan` FLOAT saveastable | [] | [[3]] | DIFFERS |
| `gt1` FLOAT saveastable | [[5]] | [[5]] | EQUAL |
| `isnull` FLOAT saveastable | [[4]] | [[4]] | EQUAL |
| `count` FLOAT saveastable | [[5]] | [[5]] | EQUAL |
| `files` FLOAT saveastable | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` FLOAT writeto | [[1, 0.0], [2, 0.0], [3, null], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | DIFFERS |
| `isnan` FLOAT writeto | [] | [[3]] | DIFFERS |
| `eqnan` FLOAT writeto | [] | [[3]] | DIFFERS |
| `gt1` FLOAT writeto | [[5]] | [[5]] | EQUAL |
| `isnull` FLOAT writeto | [[4]] | [[4]] | EQUAL |
| `count` FLOAT writeto | [[5]] | [[5]] | EQUAL |
| `files` FLOAT writeto | [[{"d": null}, 1], [{"d": null}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | DIFFERS |
| `rows` INT part | [[1, 7], [2, null]] | [[1, 7], [2, null]] | EQUAL |
| `files` INT part | [[{"d": null}, 1], [{"d": 7}, 1]] | [[{"d": null}, 1], [{"d": 7}, 1]] | EQUAL |
| `rows` STRING part | [[1, "a"], [2, null]] | [[1, "a"], [2, null]] | EQUAL |
| `files` STRING part | [[{"d": null}, 1], [{"d": "a"}, 1]] | [[{"d": null}, 1], [{"d": "a"}, 1]] | EQUAL |

## The read half

| Cell | RePark reading Spark's table | Spark on its own table | Verdict |
|---|---|---|---|
| `rows` DOUBLE flat | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` DOUBLE flat | [[3]] | [[3]] | EQUAL |
| `eqnan` DOUBLE flat | [[3]] | [[3]] | EQUAL |
| `gt1` DOUBLE flat | [[3], [5]] | [[3], [5]] | EQUAL |
| `isnull` DOUBLE flat | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE flat | [[5]] | [[5]] | EQUAL |
| `rows` DOUBLE inf | [[6, "Infinity"], [7, "-Infinity"]] | [[6, "Infinity"], [7, "-Infinity"]] | EQUAL |
| `isnan` DOUBLE inf | [] | — | — |
| `eqnan` DOUBLE inf | [] | — | — |
| `gt1` DOUBLE inf | [[6]] | — | — |
| `isnull` DOUBLE inf | [] | — | — |
| `count` DOUBLE inf | [[2]] | — | — |
| `files` DOUBLE inf | [[{"d": "Infinity"}, 1], [{"d": "-Infinity"}, 1]] | [[{"d": "-Infinity"}, 1], [{"d": "Infinity"}, 1]] | EQUAL |
| `rows` DOUBLE part | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` DOUBLE part | [[3]] | [[3]] | EQUAL |
| `eqnan` DOUBLE part | [[3]] | [[3]] | EQUAL |
| `gt1` DOUBLE part | [[5]] | [[5]] | EQUAL |
| `isnull` DOUBLE part | [[4]] | [[4]] | EQUAL |
| `count` DOUBLE part | [[5]] | [[5]] | EQUAL |
| `files` DOUBLE part | [[{"d": "NaN"}, 1], [{"d": 1.5}, 1], [{"d": 0.0}, 2], [{"d": null}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | EQUAL |
| `rows` FLOAT flat | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` FLOAT flat | [[3]] | [[3]] | EQUAL |
| `eqnan` FLOAT flat | [[3]] | [[3]] | EQUAL |
| `gt1` FLOAT flat | [[3], [5]] | [[3], [5]] | EQUAL |
| `isnull` FLOAT flat | [[4]] | [[4]] | EQUAL |
| `count` FLOAT flat | [[5]] | [[5]] | EQUAL |
| `rows` FLOAT part | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | [[1, 0.0], [2, 0.0], [3, "NaN"], [4, null], [5, 1.5]] | EQUAL |
| `isnan` FLOAT part | [[3]] | [[3]] | EQUAL |
| `eqnan` FLOAT part | [[3]] | [[3]] | EQUAL |
| `gt1` FLOAT part | [[5]] | [[5]] | EQUAL |
| `isnull` FLOAT part | [[4]] | [[4]] | EQUAL |
| `count` FLOAT part | [[5]] | [[5]] | EQUAL |
| `files` FLOAT part | [[{"d": 0.0}, 2], [{"d": "NaN"}, 1], [{"d": null}, 1], [{"d": 1.5}, 1]] | [[{"d": null}, 1], [{"d": 1.5}, 1], [{"d": "NaN"}, 1], [{"d": 0.0}, 2]] | EQUAL |
| `rows` INT part | [[1, 7], [2, null]] | [[1, 7], [2, null]] | EQUAL |
| `files` INT part | [[{"d": 7}, 1], [{"d": null}, 1]] | [[{"d": null}, 1], [{"d": 7}, 1]] | EQUAL |
| `rows` STRING part | [[1, "a"], [2, null]] | [[1, "a"], [2, null]] | EQUAL |
| `files` STRING part | [[{"d": "a"}, 1], [{"d": null}, 1]] | [[{"d": null}, 1], [{"d": "a"}, 1]] | EQUAL |

## Java's JSON for non-finite doubles

| Java call | Result |
|---|---|
| `java-json|DOUBLE|NaN` | {"toJson": "\"NaN\"", "fromJson_error": "IllegalArgumentException: Cannot parse default as a double value: \"NaN\""} |
| `java-json|DOUBLE|Infinity` | {"toJson": "\"Infinity\"", "fromJson_error": "IllegalArgumentException: Cannot parse default as a double value: \"Infinity\""} |
| `java-json|DOUBLE|-Infinity` | {"toJson": "\"-Infinity\"", "fromJson_error": "IllegalArgumentException: Cannot parse default as a double value: \"-Infinity\""} |
| `java-json-parse|DOUBLE|"NaN"` | {"fromJson_error": "IllegalArgumentException: Cannot parse default as a double value: \"NaN\""} |
| `java-json-parse|DOUBLE|NaN` | {"fromJson_error": "Py4JJavaError: An error occurred while calling z:org.apache.iceberg.SingleValueParser.fromJson.\n: java.io.UncheckedIOException: org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonParseException: Non-standard token 'NaN': enable `JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS` to allow\n at [Source: (String)\"NaN\"; "} |
