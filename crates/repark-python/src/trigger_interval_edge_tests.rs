use super::tests::{SweepExpect, check_row};

const SWEEP_T1C: &[(&str, SweepExpect)] = &[
    (
        r"  bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r" 1 month ",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: 1 month",
            params: None,
        },
    ),
    (r"+ 5 seconds", SweepExpect::Millis(5_000)),
    (
        r"+ bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '+ bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"+5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '+5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after +5 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"-",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_NUMBER] Error parsing '-' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a number after - but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"- - 5 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '- - 5 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value -. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"- 5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '- 5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after 5 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"- 5 seconds",
        SweepExpect::Refused {
            condition: None,
            text: r"requirement failed: the interval of trigger should not be negative",
            params: None,
        },
    ),
    (
        r"- bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '- bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"- seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '- seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value seconds. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"-.5 seconds",
        SweepExpect::Refused {
            condition: None,
            text: r"requirement failed: the interval of trigger should not be negative",
            params: None,
        },
    ),
    (r"-0.0000005 seconds", SweepExpect::Millis(0)),
    (
        r"-0.0005 seconds",
        SweepExpect::Refused {
            condition: None,
            text: r"requirement failed: the interval of trigger should not be negative",
            params: None,
        },
    ),
    (
        r"-1 months",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: -1 months",
            params: None,
        },
    ),
    (
        r"-5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '-5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after -5 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"-bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '-bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value -bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r".5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '.5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after .5 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r".5 seconds", SweepExpect::Millis(500)),
    (r"0 months", SweepExpect::Millis(0)),
    (r"0.0015 seconds", SweepExpect::Millis(1)),
    (r"1 second 1 day", SweepExpect::Millis(86_401_000)),
    (r"1 second 1 second", SweepExpect::Millis(2_000)),
    (
        r"1.0 minutes",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.0 minutes' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. minutes cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1.5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '1.5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after 1.5 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1.5 minute",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 minute' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. minute cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5	bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5	bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"5	seconds", SweepExpect::Millis(5_000)),
    (
        r"5  bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5  bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"5  seconds", SweepExpect::Millis(5_000)),
    (
        r"5 + seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 + seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit +. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5 -",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 -' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit -. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5 6 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 6 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit 6. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5 SECS",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 SECS' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit secs. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5 seconds +",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_NUMBER] Error parsing '5 seconds +' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a number after + but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5 seconds -",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_NUMBER] Error parsing '5 seconds -' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a number after - but hit EOL. SQLSTATE: 22006",
            params: Some(&[(r"input", r"5 seconds -"), (r"word", r"-")]),
        },
    ),
    (
        r"5 seconds 1 month",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: 5 seconds 1 month",
            params: None,
        },
    ),
    (
        r"5 seconds interval",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing '5 seconds interval' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number interval. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5 séconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 séconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit séconds. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5.",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '5.' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after 5. but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"5. days",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '5. days' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. days cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"5. seconds", SweepExpect::Millis(5_000)),
    (
        r"99999999999999999999 months",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '99999999999999999999 months' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '99999999999999999999 months'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"99999999999999999999999 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '99999999999999999999999 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '99999999999999999999999 seconds'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"99999999999999999999999.5 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '99999999999999999999999.5 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '99999999999999999999999.5 seconds'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"BOGUS",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'BOGUS' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"INTERVAL   5 seconds", SweepExpect::Millis(5_000)),
    (
        r"bogus  ",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"interval",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY"),
            text: r"[INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY] Error parsing 'interval' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Interval string cannot be empty. SQLSTATE: 22006",
            params: Some(&[(r"input", r"interval")]),
        },
    ),
    (
        r"interval 5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing 'interval 5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after 5 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"interval5 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_PREFIX"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_PREFIX] Error parsing 'interval5 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid interval prefix interval5. SQLSTATE: 22006",
            params: Some(&[(r"input", r"interval5 seconds"), (r"prefix", r"interval5")]),
        },
    ),
    (
        r"５ seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing '５ seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number ５. SQLSTATE: 22006",
            params: None,
        },
    ),
];

#[test]
fn sweep_t1c_matches_mb0c_t1c() {
    assert_eq!(SWEEP_T1C.len(), 52, "T1C rows drifted from the oracle");
    for (input, expected) in SWEEP_T1C {
        check_row(input, expected);
    }
}
