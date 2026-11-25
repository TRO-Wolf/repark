from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

REWRITES = Path("crates/repark-spark/src/spark_rewrites/mod.rs")
LITERALS = Path("crates/repark-spark/src/spark_literals.rs")


def main_function(base: str, signature: str, renamed: str) -> str:
    text = subprocess.run(
        ["git", "show", f"{base}:{REWRITES}"], capture_output=True, text=True, check=True
    ).stdout
    start = text.index(signature)
    end = text.index("\n}\n", start) + 3
    body = text[start:end]
    name = signature.split("fn ")[1]
    renamed_body = body.replace(name, renamed, 1).replace("fn ", "pub(crate) fn ", 1)
    return "\n" + renamed_body.replace("pub(crate) pub(crate)", "pub(crate)")


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if text.count(old) != 1:
        raise SystemExit(f"{path}: expected one match for {old[:60]!r}")
    path.write_text(text.replace(old, new))


def mutate_exponent(_base: str) -> None:
    replace_once(
        REWRITES,
        "                Some(found)\n"
        "                    if !found.via_period\n"
        "                        && is_bare_word(&tokens[found.index].token)\n"
        "                        && (is_identifying_integer_suffix(&tokens[found.index].token)\n"
        "                            || suffix_target(&tokens[found.index].token).is_none()) =>\n",
        "                Some(found)"
        " if is_identifying_integer_suffix(&tokens[found.index].token) =>\n",
    )
    replace_once(
        REWRITES,
        "                Some(found)\n"
        "                    if !found.via_period"
        " && suffix_target(&tokens[found.index].token).is_some() =>\n",
        "                Some(found) =>\n",
    )


def mutate_wildcard(base: str) -> None:
    function = main_function(base, "fn plan_wildcard_except_regions", "mutant_wildcard")
    REWRITES.write_text(REWRITES.read_text() + function)
    replace_once(
        LITERALS,
        "    crate::spark_rewrites::refuse_wildcard_exclude(&tokens)?;\n",
        "    regions.extend(crate::spark_rewrites::mutant_wildcard(&tokens));\n",
    )


def mutate_delete(base: str) -> None:
    function = main_function(base, "fn plan_delete_from_regions", "mutant_delete")
    REWRITES.write_text(REWRITES.read_text() + function)
    replace_once(
        LITERALS,
        "    crate::spark_rewrites::refuse_fromless_delete(&tokens)?;\n",
        "    regions.extend(crate::spark_rewrites::mutant_delete(&tokens));\n",
    )


def mutate_drop(base: str) -> None:
    function = main_function(base, "fn plan_drop_temporary_regions", "mutant_drop")
    REWRITES.write_text(REWRITES.read_text() + function)
    replace_once(
        REWRITES,
        "    let mut regions = plan_drop_temporary_regions(tokens)?;\n",
        "    let mut regions = mutant_drop(tokens);\n",
    )


MUTANTS = {
    "exponent": mutate_exponent,
    "wildcard": mutate_wildcard,
    "delete": mutate_delete,
    "drop": mutate_drop,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mutant", choices=sorted(MUTANTS))
    parser.add_argument("--base", default="14d2404a6")
    arguments = parser.parse_args()
    MUTANTS[arguments.mutant](arguments.base)


if __name__ == "__main__":
    main()
