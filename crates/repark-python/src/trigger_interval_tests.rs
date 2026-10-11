use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SweepExpect {
    Millis(u64),
    Refused {
        condition: Option<&'static str>,
        text: &'static str,
        params: Option<&'static [(&'static str, &'static str)]>,
    },
}

const SWEEP_T1: &[(&str, SweepExpect)] = &[
    (r" 5 seconds ", SweepExpect::Millis(5_000)),
    (r"+5 seconds", SweepExpect::Millis(5_000)),
    (r"-0 seconds", SweepExpect::Millis(0)),
    (
        r"-1 seconds",
        SweepExpect::Refused {
            condition: None,
            text: r"requirement failed: the interval of trigger should not be negative",
            params: Some(&[]),
        },
    ),
    (
        r"0",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '0' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after 0 but hit EOL. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"0 days", SweepExpect::Millis(0)),
    (r"0 seconds", SweepExpect::Millis(0)),
    (r"0.0005 seconds", SweepExpect::Millis(0)),
    (
        r"0.5 milliseconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '0.5 milliseconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. milliseconds cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"0.5 seconds", SweepExpect::Millis(500)),
    (
        r"1 d",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 d' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit d. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1 day", SweepExpect::Millis(86_400_000)),
    (r"1 hour", SweepExpect::Millis(3_600_000)),
    (r"1 hour 30 minutes", SweepExpect::Millis(5_400_000)),
    (
        r"1 hr",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 hr' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit hr. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 m",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 m' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit m. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 micros",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 micros' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit micros. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1 microsecond", SweepExpect::Millis(0)),
    (
        r"1 millis",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 millis' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit millis. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1 millisecond", SweepExpect::Millis(1)),
    (
        r"1 min",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 min' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit min. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1 minute", SweepExpect::Millis(60_000)),
    (r"1 minute 30.5 seconds", SweepExpect::Millis(90_500)),
    (
        r"1 month",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: 1 month",
            params: Some(&[(r"interval", r"1 month")]),
        },
    ),
    (
        r"1 month 1 second",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: 1 month 1 second",
            params: None,
        },
    ),
    (
        r"1 ms",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 ms' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit ms. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 nanos",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 nanos' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit nanos. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 nanosecond",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 nanosecond' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit nanosecond. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 ns",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 ns' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit ns. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 s",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 s' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit s. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 sec",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 sec' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit sec. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1 second", SweepExpect::Millis(1_000)),
    (
        r"1 us",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 us' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit us. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 w",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 w' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit w. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1 week", SweepExpect::Millis(604_800_000)),
    (
        r"1 y",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '1 y' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit y. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 year",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: 1 year",
            params: None,
        },
    ),
    (
        r"1.5 days",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 days' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. days cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1.5 hours",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 hours' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. hours cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1.5 microseconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 microseconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. microseconds cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1.5 milliseconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 milliseconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. milliseconds cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1.5 minutes",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 minutes' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. minutes cannot have fractional part. SQLSTATE: 22006",
            params: Some(&[(r"input", r"1.5 minutes"), (r"unit", r"minutes")]),
        },
    ),
    (
        r"1.5 months",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '1.5 months' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. months cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"1.5 seconds", SweepExpect::Millis(1_500)),
    (r"100 milliseconds", SweepExpect::Millis(100)),
    (r"100000 days", SweepExpect::Millis(8_640_000_000_000)),
    (r"106751 days", SweepExpect::Millis(9_223_286_400_000)),
    (r"106752 days", SweepExpect::Millis(9_223_372_800_000)),
    (
        r"2 days 3 hours 4 minutes 5 seconds 6 milliseconds 7 microseconds",
        SweepExpect::Millis(183_845_006),
    ),
    (r"2 hours", SweepExpect::Millis(7_200_000)),
    (
        r"2 years",
        SweepExpect::Refused {
            condition: Some(r"_LEGACY_ERROR_TEMP_3262"),
            text: r"Doesn't support month or year interval: 2 years",
            params: None,
        },
    ),
    (
        r"5",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.MISSING_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.MISSING_UNIT] Error parsing '5' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Expect a unit name after 5 but hit EOL. SQLSTATE: 22006",
            params: Some(&[(r"input", r"5"), (r"word", r"5")]),
        },
    ),
    (r"5 Seconds", SweepExpect::Millis(5_000)),
    (r"5 microseconds", SweepExpect::Millis(0)),
    (
        r"5 millisecondss",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 millisecondss' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit millisecondss. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"5 seconds", SweepExpect::Millis(5_000)),
    (
        r"5 seconds -1 minute",
        SweepExpect::Refused {
            condition: None,
            text: r"requirement failed: the interval of trigger should not be negative",
            params: None,
        },
    ),
    (
        r"5 secs",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_UNIT"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_UNIT] Error parsing '5 secs' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid unit secs. SQLSTATE: 22006",
            params: Some(&[(r"input", r"5 secs"), (r"unit", r"secs")]),
        },
    ),
    (
        r"5.5.5 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '5.5.5 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value 5.5.5. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"500 milliseconds", SweepExpect::Millis(500)),
    (
        r"5s",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_VALUE"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_VALUE] Error parsing '5s' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Invalid value 5s. SQLSTATE: 22006",
            params: Some(&[(r"input", r"5s"), (r"value", r"5s")]),
        },
    ),
    (
        r"999999999999 days",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '999999999999 days' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '999999999999 days'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"INTERVAL '5' SECOND",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'INTERVAL '5' SECOND' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number '5'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"INTERVAL 5 seconds", SweepExpect::Millis(5_000)),
    (
        r"PT5S",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'PT5S' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number pt5s. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number bogus. SQLSTATE: 22006",
            params: Some(&[(r"input", r"bogus"), (r"number", r"bogus")]),
        },
    ),
    (r"interval 5 seconds", SweepExpect::Millis(5_000)),
    (
        r"interval interval 5 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'interval interval 5 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number interval. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number seconds. SQLSTATE: 22006",
            params: None,
        },
    ),
];

const SWEEP_T1B: &[(&str, SweepExpect)] = &[
    (
        r"-100000000 days -200000 hours",
        SweepExpect::Refused {
            condition: None,
            text: r"requirement failed: the interval of trigger should not be negative",
            params: None,
        },
    ),
    (
        r"-106700000 days -2000000 hours",
        SweepExpect::Refused {
            condition: None,
            text: r"long overflow",
            params: None,
        },
    ),
    (
        r"-2147483648 months -1 month",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '-2147483648 months -1 month' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '-2147483648 months -1 month'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 day 2147483648 days",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '1 day 2147483648 days' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '1 day 2147483648 days'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"1 day 2600000000 hours",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '1 day 2600000000 hours' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '1 day 2600000000 hours'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"100000000 days 200000 hours",
        SweepExpect::Millis(8_640_720_000_000_000),
    ),
    (
        r"10000000000000 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '10000000000000 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '10000000000000 seconds'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"10000000000000000000 milliseconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '10000000000000000000 milliseconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '10000000000000000000 milliseconds'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"10000000000000000000000 microseconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '10000000000000000000000 microseconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '10000000000000000000000 microseconds'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"106700000 days",
        SweepExpect::Millis(9_218_880_000_000_000),
    ),
    (
        r"106700000 days 2000000 hours",
        SweepExpect::Refused {
            condition: None,
            text: r"long overflow",
            params: None,
        },
    ),
    (
        r"106751991167 days",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '106751991167 days' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '106751991167 days'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (r"2000000 hours", SweepExpect::Millis(7_200_000_000_000)),
    (
        r"200000000000 minutes",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '200000000000 minutes' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '200000000000 minutes'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"2147483647 days",
        SweepExpect::Refused {
            condition: None,
            text: r"long overflow",
            params: Some(&[]),
        },
    ),
    (
        r"2147483647 months 1 month",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '2147483647 months 1 month' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '2147483647 months 1 month'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"2147483648 days",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '2147483648 days' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '2147483648 days'. SQLSTATE: 22006",
            params: Some(&[(r"input", r"2147483648 days")]),
        },
    ),
    (
        r"2147483648 months",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '2147483648 months' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '2147483648 months'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"2200000000 hours",
        SweepExpect::Millis(7_920_000_000_000_000),
    ),
    (
        r"2600000000 hours",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '2600000000 hours' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '2600000000 hours'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"2600000000 hours bogus",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '2600000000 hours bogus' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '2600000000 hours bogus'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"2600000000.5 hours",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.INVALID_FRACTION"),
            text: r"[INVALID_INTERVAL_FORMAT.INVALID_FRACTION] Error parsing '2600000000.5 hours' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. hours cannot have fractional part. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"3000000000 hours",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '3000000000 hours' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '3000000000 hours'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"3000000000 minutes",
        SweepExpect::Millis(180_000_000_000_000),
    ),
    (
        r"3000000000 months",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '3000000000 months' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '3000000000 months'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"3000000000 seconds",
        SweepExpect::Millis(3_000_000_000_000),
    ),
    (
        r"3000000000 weeks",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '3000000000 weeks' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '3000000000 weeks'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"306783378 weeks",
        SweepExpect::Refused {
            condition: None,
            text: r"long overflow",
            params: None,
        },
    ),
    (
        r"306783379 weeks",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '306783379 weeks' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '306783379 weeks'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"9999999999999999999.5 seconds",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION"),
            text: r"[INVALID_INTERVAL_FORMAT.ARITHMETIC_EXCEPTION] Error parsing '9999999999999999999.5 seconds' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Uncaught arithmetic exception while parsing '9999999999999999999.5 seconds'. SQLSTATE: 22006",
            params: None,
        },
    ),
    (
        r"bogus 2600000000 hours",
        SweepExpect::Refused {
            condition: Some(r"INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"),
            text: r"[INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER] Error parsing 'bogus 2600000000 hours' to interval. Please ensure that the value provided is in a valid format for defining an interval. You can reference the documentation for the correct format. Unrecognized number bogus. SQLSTATE: 22006",
            params: None,
        },
    ),
];

fn refusal_shape(refusal: TriggerRefusal) -> (Option<String>, String, Vec<(String, String)>) {
    match refusal {
        TriggerRefusal::InvalidFormat {
            condition,
            text,
            params,
        } => (Some(condition.to_owned()), text, params),
        TriggerRefusal::Months { text, interval } => (
            Some(MONTHS_CONDITION.to_owned()),
            text,
            vec![("interval".to_owned(), interval)],
        ),
        TriggerRefusal::Negative => (None, NEGATIVE_TEXT.to_owned(), Vec::new()),
        TriggerRefusal::Overflow => (None, OVERFLOW_TEXT.to_owned(), Vec::new()),
    }
}

pub(crate) fn check_row(input: &str, expected: &SweepExpect) {
    match (parse_trigger_millis(input), expected) {
        (Ok(millis), SweepExpect::Millis(want)) => {
            assert_eq!(millis, *want, "millis for {input:?}");
        }
        (
            Err(refusal),
            SweepExpect::Refused {
                condition,
                text,
                params,
            },
        ) => {
            let (got_condition, got_text, got_params) = refusal_shape(refusal);
            assert_eq!(
                got_condition.as_deref(),
                *condition,
                "condition for {input:?}"
            );
            assert_eq!(got_text, *text, "text for {input:?}");
            if let Some(want_params) = params {
                let want: Vec<(String, String)> = want_params
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect();
                assert_eq!(got_params, want, "params for {input:?}");
            }
        }
        (Ok(millis), SweepExpect::Refused { .. }) => {
            panic!("{input:?} parsed to {millis} but the cell refuses");
        }
        (Err(refusal), SweepExpect::Millis(want)) => {
            panic!("{input:?} refused with {refusal:?} but the cell accepts {want}");
        }
    }
}

#[test]
fn sweep_t1_matches_mb0c_t1() {
    assert_eq!(SWEEP_T1.len(), 69, "T1 rows drifted from the oracle");
    for (input, expected) in SWEEP_T1 {
        check_row(input, expected);
    }
}

#[test]
fn sweep_t1b_matches_mb0c_t1b() {
    assert_eq!(SWEEP_T1B.len(), 31, "T1B rows drifted from the oracle");
    for (input, expected) in SWEEP_T1B {
        check_row(input, expected);
    }
}

#[test]
fn blank_input_refuses_empty() {
    let Err(refusal) = parse_trigger_millis("   ") else {
        panic!("blank input parsed");
    };
    let (condition, text, _) = refusal_shape(refusal);
    assert_eq!(
        condition.as_deref(),
        Some("INVALID_INTERVAL_FORMAT.INPUT_IS_EMPTY")
    );
    assert!(text.ends_with("Interval string cannot be empty. SQLSTATE: 22006"));
}

#[test]
fn lone_dot_refuses_invalid_value() {
    let Err(refusal) = parse_trigger_millis(".") else {
        panic!("lone dot parsed");
    };
    let (condition, text, _) = refusal_shape(refusal);
    assert_eq!(
        condition.as_deref(),
        Some("INVALID_INTERVAL_FORMAT.INVALID_VALUE")
    );
    assert!(text.ends_with("Invalid value .. SQLSTATE: 22006"));
}
