# Card STRUCT-COMPARE-PLANNER-CLASS-1: a refusal of struct comparisons names DataFusion's class where Spark names its own

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3, class only. **Pre-existing:** not stated in the records; the cells sit on the step-0 table of PR #1292.

**Measured:**

- 32 step-0 `sql` cells: 01147 01150 01166 01169 01185 01188 01204 01207 01223 01226 01242 01245 01261 01264 03969 03972 03988 03991 04007 04010 04045 04048 04064 04067 04083 04086 04121 04124 04140 04143 04159 04162. Both sides refuse; RePark's refusal carries DataFusion's class, Spark's carries its own.
- 48 DataFrame-door cells: 01159 01164 01165 01178 01183 01184 01197 01202 01203 01216 01221 01222 01235 01240 01241 01254 01259 01260 01273 01278 01279 03981 03986 03987 04000 04005 04006 04019 04024 04025 04057 04062 04063 04076 04081 04082 04095 04100 04101 04133 04138 04139 04152 04157 04158 04171 04176 04177. The same class difference.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
