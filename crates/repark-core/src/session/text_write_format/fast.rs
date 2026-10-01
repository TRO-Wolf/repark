use chrono::{FixedOffset, NaiveDate};

pub const MICROS_PER_DAY: i64 = 86_400_000_000;
pub const SMALL_DELTA_MICROS: i64 = 120_000_000;

const PAIRS: &[u8; 200] = b"00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899";

pub struct DateCache {
    days: i64,
    bytes: [u8; 16],
    len: usize,
    civil: (i64, i64, i64),
    armed: bool,
    lowest: i64,
    highest: i64,
}

impl DateCache {
    pub fn new() -> Self {
        let lowest = NaiveDate::MIN
            .and_hms_opt(0, 0, 0)
            .unwrap_or_default()
            .and_utc()
            .timestamp()
            / 86_400;
        let highest = NaiveDate::MAX
            .and_hms_opt(0, 0, 0)
            .unwrap_or_default()
            .and_utc()
            .timestamp()
            / 86_400;
        Self {
            days: 0,
            bytes: [0u8; 16],
            len: 0,
            civil: (1970, 1, 1),
            armed: false,
            lowest,
            highest,
        }
    }

    pub fn in_naive_range(&self, days: i64) -> bool {
        days >= self.lowest && days <= self.highest
    }

    pub fn resolve(&mut self, days: i64) -> (&[u8], (i64, i64, i64)) {
        if !self.armed || days != self.days {
            let (year, month, day) = civil_from_days(days);
            let month_narrow = usize::try_from(month).unwrap_or(1);
            let day_narrow = usize::try_from(day).unwrap_or(1);
            self.len = emit_date_bytes(year, month_narrow, day_narrow, &mut self.bytes);
            self.days = days;
            self.civil = (year, month, day);
            self.armed = true;
        }
        (&self.bytes[..self.len], self.civil)
    }

    pub fn store(&mut self, days: i64, civil: (i64, i64, i64), bytes: &[u8]) {
        let len = bytes.len().min(16);
        self.bytes[..len].copy_from_slice(&bytes[..len]);
        self.len = len;
        self.days = days;
        self.civil = civil;
        self.armed = true;
    }
}

impl Default for DateCache {
    fn default() -> Self {
        Self::new()
    }
}

pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_pair = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_pair + 2) / 5 + 1;
    let month = if month_pair < 10 {
        month_pair + 3
    } else {
        month_pair - 9
    };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

pub fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

pub fn month_length(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
    }
}

fn pair_at(value: usize) -> usize {
    value * 2
}

pub fn emit_date_bytes(year: i64, month: usize, day: usize, out: &mut [u8; 16]) -> usize {
    if (0..10_000).contains(&year) {
        let year = usize::try_from(year).unwrap_or(0);
        out[0..2].copy_from_slice(&PAIRS[pair_at(year / 100)..pair_at(year / 100) + 2]);
        out[2..4].copy_from_slice(&PAIRS[pair_at(year % 100)..pair_at(year % 100) + 2]);
        out[4] = b'-';
        out[5..7].copy_from_slice(&PAIRS[pair_at(month)..pair_at(month) + 2]);
        out[7] = b'-';
        out[8..10].copy_from_slice(&PAIRS[pair_at(day)..pair_at(day) + 2]);
        10
    } else {
        let text = format!("{year:+05}");
        let bytes = text.as_bytes();
        let mut len = bytes.len();
        out[..len].copy_from_slice(bytes);
        out[len] = b'-';
        len += 1;
        out[len..len + 2].copy_from_slice(&PAIRS[pair_at(month)..pair_at(month) + 2]);
        out[len + 2] = b'-';
        out[len + 3..len + 5].copy_from_slice(&PAIRS[pair_at(day)..pair_at(day) + 2]);
        len + 5
    }
}

pub fn split_day_micros(day_micros: i64) -> (usize, usize, usize, usize) {
    let seconds = day_micros / 1_000_000;
    let millis = (day_micros - seconds * 1_000_000) / 1000;
    let hour = seconds / 3600;
    let rest = seconds - hour * 3600;
    let minute = rest / 60;
    let second = rest - minute * 60;
    (
        usize::try_from(hour).unwrap_or(0),
        usize::try_from(minute).unwrap_or(0),
        usize::try_from(second).unwrap_or(0),
        usize::try_from(millis).unwrap_or(0),
    )
}

pub fn increment_day(year: i64, month: i64, day: i64) -> (i64, i64, i64) {
    if day < month_length(year, month) {
        (year, month, day + 1)
    } else if month < 12 {
        (year, month + 1, 1)
    } else {
        (year + 1, 1, 1)
    }
}

pub fn emit_offset_suffix(offset: FixedOffset, out: &mut [u8; 9]) -> usize {
    let total = offset.local_minus_utc();
    if total == 0 {
        out[0] = b'Z';
        return 1;
    }
    let sign = if total < 0 { b'-' } else { b'+' };
    let total = total.unsigned_abs();
    let hours = usize::try_from(total / 3600).unwrap_or(0);
    let minutes = usize::try_from((total % 3600) / 60).unwrap_or(0);
    let seconds = usize::try_from(total % 60).unwrap_or(0);
    out[0] = sign;
    out[1..3].copy_from_slice(&PAIRS[pair_at(hours)..pair_at(hours) + 2]);
    out[3] = b':';
    out[4..6].copy_from_slice(&PAIRS[pair_at(minutes)..pair_at(minutes) + 2]);
    if seconds == 0 {
        6
    } else {
        out[6] = b':';
        out[7..9].copy_from_slice(&PAIRS[pair_at(seconds)..pair_at(seconds) + 2]);
        9
    }
}

pub fn wall_micros_bounds() -> (i64, i64) {
    (
        NaiveDate::MIN
            .and_hms_opt(0, 0, 0)
            .unwrap_or_default()
            .and_utc()
            .timestamp_micros(),
        NaiveDate::MAX
            .and_hms_opt(23, 59, 59)
            .unwrap_or_default()
            .and_utc()
            .timestamp_micros()
            + 999_999,
    )
}

fn narrow_day_part(value: i64) -> usize {
    usize::try_from(value).unwrap_or(0)
}

pub struct TimestampDefaultState {
    date: DateCache,
    suffix: [u8; 9],
    suffix_len: usize,
    last_date: [u8; 16],
    last_date_len: usize,
    last_wall: i64,
    last_days: i64,
    last_day_micros: i64,
    last_civil: (i64, i64, i64),
    last_offset: Option<FixedOffset>,
    armed: bool,
}

impl TimestampDefaultState {
    pub fn new() -> Self {
        Self {
            date: DateCache::new(),
            suffix: [0u8; 9],
            suffix_len: 0,
            last_date: [0u8; 16],
            last_date_len: 0,
            last_wall: 0,
            last_days: 0,
            last_day_micros: 0,
            last_civil: (1970, 1, 1),
            last_offset: None,
            armed: false,
        }
    }

    fn stage_time(day_micros: i64, staged: &mut [u8; 48], at: usize) -> usize {
        let (hour, minute, second, millis) = split_day_micros(day_micros);
        staged[at] = b'T';
        staged[at + 1..at + 3].copy_from_slice(&PAIRS[pair_at(hour)..pair_at(hour) + 2]);
        staged[at + 3] = b':';
        staged[at + 4..at + 6].copy_from_slice(&PAIRS[pair_at(minute)..pair_at(minute) + 2]);
        staged[at + 6] = b':';
        staged[at + 7..at + 9].copy_from_slice(&PAIRS[pair_at(second)..pair_at(second) + 2]);
        staged[at + 9] = b'.';
        staged[at + 10..at + 12]
            .copy_from_slice(&PAIRS[pair_at(millis / 10)..pair_at(millis / 10) + 2]);
        staged[at + 12] = PAIRS[pair_at(millis % 10) + 1];
        at + 13
    }

    fn stage_suffix(&self, staged: &mut [u8; 48], at: usize) -> usize {
        staged[at..at + self.suffix_len].copy_from_slice(&self.suffix[..self.suffix_len]);
        at + self.suffix_len
    }

    pub fn render(
        &mut self,
        wall: i64,
        offset: Option<FixedOffset>,
        staged: &mut [u8; 48],
    ) -> usize {
        if self.last_offset != offset {
            self.suffix_len = match offset {
                Some(found) => emit_offset_suffix(found, &mut self.suffix),
                None => 0,
            };
            self.last_offset = offset;
        }
        if self.armed && wall > self.last_wall {
            let delta = wall.saturating_sub(self.last_wall);
            if delta < SMALL_DELTA_MICROS {
                let mut total = self.last_day_micros + delta;
                let mut days = self.last_days;
                let (mut year, mut month, mut day) = self.last_civil;
                let mut date_len = self.last_date_len;
                if total >= MICROS_PER_DAY {
                    total -= MICROS_PER_DAY;
                    days += 1;
                    (year, month, day) = increment_day(year, month, day);
                    let mut scratch = [0u8; 16];
                    date_len = emit_date_bytes(
                        year,
                        narrow_day_part(month),
                        narrow_day_part(day),
                        &mut scratch,
                    );
                    self.last_date[..date_len].copy_from_slice(&scratch[..date_len]);
                    self.last_date_len = date_len;
                    self.date
                        .store(days, (year, month, day), &scratch[..date_len]);
                }
                staged[..date_len].copy_from_slice(&self.last_date[..date_len]);
                let at = Self::stage_time(total, staged, date_len);
                self.last_wall = wall;
                self.last_days = days;
                self.last_day_micros = total;
                self.last_civil = (year, month, day);
                return self.stage_suffix(staged, at);
            }
        }
        let (days, day_micros) = if wall >= 0 {
            (wall / MICROS_PER_DAY, wall % MICROS_PER_DAY)
        } else {
            (
                wall.div_euclid(MICROS_PER_DAY),
                wall.rem_euclid(MICROS_PER_DAY),
            )
        };
        let (date_bytes, civil) = self.date.resolve(days);
        let date_len = date_bytes.len();
        staged[..date_len].copy_from_slice(date_bytes);
        self.last_date[..date_len].copy_from_slice(date_bytes);
        self.last_date_len = date_len;
        let at = Self::stage_time(day_micros, staged, date_len);
        self.last_wall = wall;
        self.last_days = days;
        self.last_day_micros = day_micros;
        self.last_civil = civil;
        self.armed = true;
        self.stage_suffix(staged, at)
    }
}

impl Default for TimestampDefaultState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DateDefaultState {
    date: DateCache,
    last_days: i64,
    last_civil: (i64, i64, i64),
    armed: bool,
}

impl DateDefaultState {
    pub fn new() -> Self {
        Self {
            date: DateCache::new(),
            last_days: 0,
            last_civil: (1970, 1, 1),
            armed: false,
        }
    }

    pub fn render(&mut self, days: i64, staged: &mut [u8; 48]) -> Option<usize> {
        if !self.date.in_naive_range(days) {
            return None;
        }
        if self.armed && days == self.last_days + 1 {
            let (year, month, day) =
                increment_day(self.last_civil.0, self.last_civil.1, self.last_civil.2);
            let mut scratch = [0u8; 16];
            let len = emit_date_bytes(
                year,
                narrow_day_part(month),
                narrow_day_part(day),
                &mut scratch,
            );
            staged[..len].copy_from_slice(&scratch[..len]);
            self.last_days = days;
            self.last_civil = (year, month, day);
            self.date.store(days, (year, month, day), &scratch[..len]);
            return Some(len);
        }
        let (bytes, civil) = self.date.resolve(days);
        let len = bytes.len();
        staged[..len].copy_from_slice(bytes);
        self.last_days = days;
        self.last_civil = civil;
        self.armed = true;
        Some(len)
    }
}

impl Default for DateDefaultState {
    fn default() -> Self {
        Self::new()
    }
}
