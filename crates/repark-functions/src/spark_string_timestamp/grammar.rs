const YEAR: usize = 0;
const DAY: usize = 2;
const HOUR: usize = 3;
const MINUTE: usize = 4;
const SECOND: usize = 5;
const FRACTION: usize = 6;
const ZONE: usize = 7;
const SEGMENT_COUNT: usize = 9;
const MIN_YEAR_DIGITS: usize = 4;
const MAX_YEAR_DIGITS: usize = 6;
const MAX_FIELD_DIGITS: usize = 2;
const FRACTION_DIGITS: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedTimestamp<'text> {
    pub(crate) year: i64,
    pub(crate) month: i64,
    pub(crate) day: i64,
    pub(crate) hour: i64,
    pub(crate) minute: i64,
    pub(crate) second: i64,
    pub(crate) micros: i64,
    pub(crate) zone: Option<&'text str>,
    pub(crate) just_time: bool,
}

fn is_trimmable(byte: u8) -> bool {
    byte <= b' ' || byte == 0x7f
}

fn is_valid_digits(segment: usize, digits: usize) -> bool {
    segment == FRACTION
        || (segment == YEAR && (MIN_YEAR_DIGITS..=MAX_YEAR_DIGITS).contains(&digits))
        || (segment == ZONE && digits <= MAX_FIELD_DIGITS)
        || (segment != YEAR
            && segment != FRACTION
            && segment != ZONE
            && (1..=MAX_FIELD_DIGITS).contains(&digits))
}

struct Segments {
    values: [i64; SEGMENT_COUNT],
    index: usize,
    value: i64,
    digits: usize,
}

impl Segments {
    fn close_into(&mut self, segment: usize) -> Option<()> {
        if !is_valid_digits(segment, self.digits) {
            return None;
        }
        *self.values.get_mut(segment)? = self.value;
        self.value = 0;
        self.digits = 0;
        Some(())
    }

    fn close_and_advance(&mut self) -> Option<()> {
        self.close_into(self.index)?;
        self.index += 1;
        Some(())
    }
}

#[must_use]
pub(crate) fn parse_timestamp_string(text: &str) -> Option<ParsedTimestamp<'_>> {
    let bytes = text.as_bytes();
    let mut position = bytes.iter().position(|byte| !is_trimmable(*byte))?;
    let end = bytes.iter().rposition(|byte| !is_trimmable(*byte))? + 1;
    let mut segments = Segments {
        values: [1, 1, 1, 0, 0, 0, 0, 0, 0],
        index: YEAR,
        value: 0,
        digits: 0,
    };
    let mut fraction_digits = 0_usize;
    let mut just_time = false;
    let mut zone = None;
    let mut year_is_negative = None;
    if bytes[position] == b'-' || bytes[position] == b'+' {
        year_is_negative = Some(bytes[position] == b'-');
        position += 1;
    }
    while position < end {
        let byte = bytes[position];
        if byte.is_ascii_digit() {
            if segments.index == FRACTION {
                fraction_digits += 1;
            }
            if segments.index != FRACTION || segments.digits < FRACTION_DIGITS {
                segments.value = segments
                    .value
                    .saturating_mul(10)
                    .saturating_add(i64::from(byte - b'0'));
            }
            segments.digits += 1;
        } else if position == 0 && byte == b'T' {
            just_time = true;
            segments.index = HOUR;
        } else if segments.index < DAY {
            if byte == b'-' {
                segments.close_and_advance()?;
            } else if segments.index == YEAR && byte == b':' && year_is_negative.is_none() {
                just_time = true;
                segments.close_into(HOUR)?;
                segments.index = MINUTE;
            } else {
                return None;
            }
        } else if segments.index == DAY {
            if byte != b' ' && byte != b'T' {
                return None;
            }
            segments.close_and_advance()?;
        } else if segments.index == HOUR || segments.index == MINUTE {
            if byte != b':' {
                return None;
            }
            segments.close_and_advance()?;
        } else if segments.index == SECOND || segments.index == FRACTION {
            let opens_fraction = byte == b'.' && segments.index == SECOND;
            segments.close_and_advance()?;
            if !opens_fraction {
                zone = Some(std::str::from_utf8(&bytes[position..end]).ok()?);
                position = end - 1;
            }
            if segments.index == FRACTION && byte != b'.' {
                segments.index += 1;
            }
        } else {
            return None;
        }
        position += 1;
    }
    segments.close_into(segments.index)?;
    let mut micros = segments.values[FRACTION];
    while fraction_digits < FRACTION_DIGITS {
        micros *= 10;
        fraction_digits += 1;
    }
    let [year, month, day, hour, minute, second, ..] = segments.values;
    Some(ParsedTimestamp {
        year: if year_is_negative == Some(true) {
            -year
        } else {
            year
        },
        month,
        day,
        hour,
        minute,
        second,
        micros,
        zone,
        just_time,
    })
}
