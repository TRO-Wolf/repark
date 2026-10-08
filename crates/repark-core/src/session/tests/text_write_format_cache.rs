use std::str::FromStr;

use arrow::array::timezone::Tz;
use chrono::TimeZone as _;

use crate::session::text_write_format::fast::wall_micros_bounds;
use crate::session::text_write_format::micros_to_wall_zone;
use crate::session::text_write_format::udf::{CACHED_WINDOW_MICROS, OffsetCache, ZoneResolver};

const ALL_ZONES: [&str; 597] = [
    "Africa/Abidjan",
    "Africa/Accra",
    "Africa/Addis_Ababa",
    "Africa/Algiers",
    "Africa/Asmara",
    "Africa/Asmera",
    "Africa/Bamako",
    "Africa/Bangui",
    "Africa/Banjul",
    "Africa/Bissau",
    "Africa/Blantyre",
    "Africa/Brazzaville",
    "Africa/Bujumbura",
    "Africa/Cairo",
    "Africa/Casablanca",
    "Africa/Ceuta",
    "Africa/Conakry",
    "Africa/Dakar",
    "Africa/Dar_es_Salaam",
    "Africa/Djibouti",
    "Africa/Douala",
    "Africa/El_Aaiun",
    "Africa/Freetown",
    "Africa/Gaborone",
    "Africa/Harare",
    "Africa/Johannesburg",
    "Africa/Juba",
    "Africa/Kampala",
    "Africa/Khartoum",
    "Africa/Kigali",
    "Africa/Kinshasa",
    "Africa/Lagos",
    "Africa/Libreville",
    "Africa/Lome",
    "Africa/Luanda",
    "Africa/Lubumbashi",
    "Africa/Lusaka",
    "Africa/Malabo",
    "Africa/Maputo",
    "Africa/Maseru",
    "Africa/Mbabane",
    "Africa/Mogadishu",
    "Africa/Monrovia",
    "Africa/Nairobi",
    "Africa/Ndjamena",
    "Africa/Niamey",
    "Africa/Nouakchott",
    "Africa/Ouagadougou",
    "Africa/Porto-Novo",
    "Africa/Sao_Tome",
    "Africa/Timbuktu",
    "Africa/Tripoli",
    "Africa/Tunis",
    "Africa/Windhoek",
    "America/Adak",
    "America/Anchorage",
    "America/Anguilla",
    "America/Antigua",
    "America/Araguaina",
    "America/Argentina/Buenos_Aires",
    "America/Argentina/Catamarca",
    "America/Argentina/ComodRivadavia",
    "America/Argentina/Cordoba",
    "America/Argentina/Jujuy",
    "America/Argentina/La_Rioja",
    "America/Argentina/Mendoza",
    "America/Argentina/Rio_Gallegos",
    "America/Argentina/Salta",
    "America/Argentina/San_Juan",
    "America/Argentina/San_Luis",
    "America/Argentina/Tucuman",
    "America/Argentina/Ushuaia",
    "America/Aruba",
    "America/Asuncion",
    "America/Atikokan",
    "America/Atka",
    "America/Bahia",
    "America/Bahia_Banderas",
    "America/Barbados",
    "America/Belem",
    "America/Belize",
    "America/Blanc-Sablon",
    "America/Boa_Vista",
    "America/Bogota",
    "America/Boise",
    "America/Buenos_Aires",
    "America/Cambridge_Bay",
    "America/Campo_Grande",
    "America/Cancun",
    "America/Caracas",
    "America/Catamarca",
    "America/Cayenne",
    "America/Cayman",
    "America/Chicago",
    "America/Chihuahua",
    "America/Ciudad_Juarez",
    "America/Coral_Harbour",
    "America/Cordoba",
    "America/Costa_Rica",
    "America/Coyhaique",
    "America/Creston",
    "America/Cuiaba",
    "America/Curacao",
    "America/Danmarkshavn",
    "America/Dawson",
    "America/Dawson_Creek",
    "America/Denver",
    "America/Detroit",
    "America/Dominica",
    "America/Edmonton",
    "America/Eirunepe",
    "America/El_Salvador",
    "America/Ensenada",
    "America/Fortaleza",
    "America/Fort_Nelson",
    "America/Fort_Wayne",
    "America/Glace_Bay",
    "America/Godthab",
    "America/Goose_Bay",
    "America/Grand_Turk",
    "America/Grenada",
    "America/Guadeloupe",
    "America/Guatemala",
    "America/Guayaquil",
    "America/Guyana",
    "America/Halifax",
    "America/Havana",
    "America/Hermosillo",
    "America/Indiana/Indianapolis",
    "America/Indiana/Knox",
    "America/Indiana/Marengo",
    "America/Indiana/Petersburg",
    "America/Indianapolis",
    "America/Indiana/Tell_City",
    "America/Indiana/Vevay",
    "America/Indiana/Vincennes",
    "America/Indiana/Winamac",
    "America/Inuvik",
    "America/Iqaluit",
    "America/Jamaica",
    "America/Jujuy",
    "America/Juneau",
    "America/Kentucky/Louisville",
    "America/Kentucky/Monticello",
    "America/Knox_IN",
    "America/Kralendijk",
    "America/La_Paz",
    "America/Lima",
    "America/Los_Angeles",
    "America/Louisville",
    "America/Lower_Princes",
    "America/Maceio",
    "America/Managua",
    "America/Manaus",
    "America/Marigot",
    "America/Martinique",
    "America/Matamoros",
    "America/Mazatlan",
    "America/Mendoza",
    "America/Menominee",
    "America/Merida",
    "America/Metlakatla",
    "America/Mexico_City",
    "America/Miquelon",
    "America/Moncton",
    "America/Monterrey",
    "America/Montevideo",
    "America/Montreal",
    "America/Montserrat",
    "America/Nassau",
    "America/New_York",
    "America/Nipigon",
    "America/Nome",
    "America/Noronha",
    "America/North_Dakota/Beulah",
    "America/North_Dakota/Center",
    "America/North_Dakota/New_Salem",
    "America/Nuuk",
    "America/Ojinaga",
    "America/Panama",
    "America/Pangnirtung",
    "America/Paramaribo",
    "America/Phoenix",
    "America/Port-au-Prince",
    "America/Porto_Acre",
    "America/Port_of_Spain",
    "America/Porto_Velho",
    "America/Puerto_Rico",
    "America/Punta_Arenas",
    "America/Rainy_River",
    "America/Rankin_Inlet",
    "America/Recife",
    "America/Regina",
    "America/Resolute",
    "America/Rio_Branco",
    "America/Rosario",
    "America/Santa_Isabel",
    "America/Santarem",
    "America/Santiago",
    "America/Santo_Domingo",
    "America/Sao_Paulo",
    "America/Scoresbysund",
    "America/Shiprock",
    "America/Sitka",
    "America/St_Barthelemy",
    "America/St_Johns",
    "America/St_Kitts",
    "America/St_Lucia",
    "America/St_Thomas",
    "America/St_Vincent",
    "America/Swift_Current",
    "America/Tegucigalpa",
    "America/Thule",
    "America/Thunder_Bay",
    "America/Tijuana",
    "America/Toronto",
    "America/Tortola",
    "America/Vancouver",
    "America/Virgin",
    "America/Whitehorse",
    "America/Winnipeg",
    "America/Yakutat",
    "America/Yellowknife",
    "Antarctica/Casey",
    "Antarctica/Davis",
    "Antarctica/DumontDUrville",
    "Antarctica/Macquarie",
    "Antarctica/Mawson",
    "Antarctica/McMurdo",
    "Antarctica/Palmer",
    "Antarctica/Rothera",
    "Antarctica/South_Pole",
    "Antarctica/Syowa",
    "Antarctica/Troll",
    "Antarctica/Vostok",
    "Arctic/Longyearbyen",
    "Asia/Aden",
    "Asia/Almaty",
    "Asia/Amman",
    "Asia/Anadyr",
    "Asia/Aqtau",
    "Asia/Aqtobe",
    "Asia/Ashgabat",
    "Asia/Ashkhabad",
    "Asia/Atyrau",
    "Asia/Baghdad",
    "Asia/Bahrain",
    "Asia/Baku",
    "Asia/Bangkok",
    "Asia/Barnaul",
    "Asia/Beirut",
    "Asia/Bishkek",
    "Asia/Brunei",
    "Asia/Calcutta",
    "Asia/Chita",
    "Asia/Choibalsan",
    "Asia/Chongqing",
    "Asia/Chungking",
    "Asia/Colombo",
    "Asia/Dacca",
    "Asia/Damascus",
    "Asia/Dhaka",
    "Asia/Dili",
    "Asia/Dubai",
    "Asia/Dushanbe",
    "Asia/Famagusta",
    "Asia/Gaza",
    "Asia/Harbin",
    "Asia/Hebron",
    "Asia/Ho_Chi_Minh",
    "Asia/Hong_Kong",
    "Asia/Hovd",
    "Asia/Irkutsk",
    "Asia/Istanbul",
    "Asia/Jakarta",
    "Asia/Jayapura",
    "Asia/Jerusalem",
    "Asia/Kabul",
    "Asia/Kamchatka",
    "Asia/Karachi",
    "Asia/Kashgar",
    "Asia/Kathmandu",
    "Asia/Katmandu",
    "Asia/Khandyga",
    "Asia/Kolkata",
    "Asia/Krasnoyarsk",
    "Asia/Kuala_Lumpur",
    "Asia/Kuching",
    "Asia/Kuwait",
    "Asia/Macao",
    "Asia/Macau",
    "Asia/Magadan",
    "Asia/Makassar",
    "Asia/Manila",
    "Asia/Muscat",
    "Asia/Nicosia",
    "Asia/Novokuznetsk",
    "Asia/Novosibirsk",
    "Asia/Omsk",
    "Asia/Oral",
    "Asia/Phnom_Penh",
    "Asia/Pontianak",
    "Asia/Pyongyang",
    "Asia/Qatar",
    "Asia/Qostanay",
    "Asia/Qyzylorda",
    "Asia/Rangoon",
    "Asia/Riyadh",
    "Asia/Saigon",
    "Asia/Sakhalin",
    "Asia/Samarkand",
    "Asia/Seoul",
    "Asia/Shanghai",
    "Asia/Singapore",
    "Asia/Srednekolymsk",
    "Asia/Taipei",
    "Asia/Tashkent",
    "Asia/Tbilisi",
    "Asia/Tehran",
    "Asia/Tel_Aviv",
    "Asia/Thimbu",
    "Asia/Thimphu",
    "Asia/Tokyo",
    "Asia/Tomsk",
    "Asia/Ujung_Pandang",
    "Asia/Ulaanbaatar",
    "Asia/Ulan_Bator",
    "Asia/Urumqi",
    "Asia/Ust-Nera",
    "Asia/Vientiane",
    "Asia/Vladivostok",
    "Asia/Yakutsk",
    "Asia/Yangon",
    "Asia/Yekaterinburg",
    "Asia/Yerevan",
    "Atlantic/Azores",
    "Atlantic/Bermuda",
    "Atlantic/Canary",
    "Atlantic/Cape_Verde",
    "Atlantic/Faeroe",
    "Atlantic/Faroe",
    "Atlantic/Jan_Mayen",
    "Atlantic/Madeira",
    "Atlantic/Reykjavik",
    "Atlantic/South_Georgia",
    "Atlantic/Stanley",
    "Atlantic/St_Helena",
    "Australia/ACT",
    "Australia/Adelaide",
    "Australia/Brisbane",
    "Australia/Broken_Hill",
    "Australia/Canberra",
    "Australia/Currie",
    "Australia/Darwin",
    "Australia/Eucla",
    "Australia/Hobart",
    "Australia/LHI",
    "Australia/Lindeman",
    "Australia/Lord_Howe",
    "Australia/Melbourne",
    "Australia/North",
    "Australia/NSW",
    "Australia/Perth",
    "Australia/Queensland",
    "Australia/South",
    "Australia/Sydney",
    "Australia/Tasmania",
    "Australia/Victoria",
    "Australia/West",
    "Australia/Yancowinna",
    "Brazil/Acre",
    "Brazil/DeNoronha",
    "Brazil/East",
    "Brazil/West",
    "Canada/Atlantic",
    "Canada/Central",
    "Canada/Eastern",
    "Canada/Mountain",
    "Canada/Newfoundland",
    "Canada/Pacific",
    "Canada/Saskatchewan",
    "Canada/Yukon",
    "CET",
    "Chile/Continental",
    "Chile/EasterIsland",
    "CST6CDT",
    "Cuba",
    "EET",
    "Egypt",
    "Eire",
    "EST",
    "EST5EDT",
    "Etc/GMT",
    "Etc/GMT+0",
    "Etc/GMT-0",
    "Etc/GMT0",
    "Etc/GMT+1",
    "Etc/GMT-1",
    "Etc/GMT+10",
    "Etc/GMT-10",
    "Etc/GMT+11",
    "Etc/GMT-11",
    "Etc/GMT+12",
    "Etc/GMT-12",
    "Etc/GMT-13",
    "Etc/GMT-14",
    "Etc/GMT+2",
    "Etc/GMT-2",
    "Etc/GMT+3",
    "Etc/GMT-3",
    "Etc/GMT+4",
    "Etc/GMT-4",
    "Etc/GMT+5",
    "Etc/GMT-5",
    "Etc/GMT+6",
    "Etc/GMT-6",
    "Etc/GMT+7",
    "Etc/GMT-7",
    "Etc/GMT+8",
    "Etc/GMT-8",
    "Etc/GMT+9",
    "Etc/GMT-9",
    "Etc/Greenwich",
    "Etc/UCT",
    "Etc/Universal",
    "Etc/UTC",
    "Etc/Zulu",
    "Europe/Amsterdam",
    "Europe/Andorra",
    "Europe/Astrakhan",
    "Europe/Athens",
    "Europe/Belfast",
    "Europe/Belgrade",
    "Europe/Berlin",
    "Europe/Bratislava",
    "Europe/Brussels",
    "Europe/Bucharest",
    "Europe/Budapest",
    "Europe/Busingen",
    "Europe/Chisinau",
    "Europe/Copenhagen",
    "Europe/Dublin",
    "Europe/Gibraltar",
    "Europe/Guernsey",
    "Europe/Helsinki",
    "Europe/Isle_of_Man",
    "Europe/Istanbul",
    "Europe/Jersey",
    "Europe/Kaliningrad",
    "Europe/Kiev",
    "Europe/Kirov",
    "Europe/Kyiv",
    "Europe/Lisbon",
    "Europe/Ljubljana",
    "Europe/London",
    "Europe/Luxembourg",
    "Europe/Madrid",
    "Europe/Malta",
    "Europe/Mariehamn",
    "Europe/Minsk",
    "Europe/Monaco",
    "Europe/Moscow",
    "Europe/Nicosia",
    "Europe/Oslo",
    "Europe/Paris",
    "Europe/Podgorica",
    "Europe/Prague",
    "Europe/Riga",
    "Europe/Rome",
    "Europe/Samara",
    "Europe/San_Marino",
    "Europe/Sarajevo",
    "Europe/Saratov",
    "Europe/Simferopol",
    "Europe/Skopje",
    "Europe/Sofia",
    "Europe/Stockholm",
    "Europe/Tallinn",
    "Europe/Tirane",
    "Europe/Tiraspol",
    "Europe/Ulyanovsk",
    "Europe/Uzhgorod",
    "Europe/Vaduz",
    "Europe/Vatican",
    "Europe/Vienna",
    "Europe/Vilnius",
    "Europe/Volgograd",
    "Europe/Warsaw",
    "Europe/Zagreb",
    "Europe/Zaporozhye",
    "Europe/Zurich",
    "GB",
    "GB-Eire",
    "GMT",
    "GMT+0",
    "GMT-0",
    "GMT0",
    "Greenwich",
    "Hongkong",
    "HST",
    "Iceland",
    "Indian/Antananarivo",
    "Indian/Chagos",
    "Indian/Christmas",
    "Indian/Cocos",
    "Indian/Comoro",
    "Indian/Kerguelen",
    "Indian/Mahe",
    "Indian/Maldives",
    "Indian/Mauritius",
    "Indian/Mayotte",
    "Indian/Reunion",
    "Iran",
    "Israel",
    "Jamaica",
    "Japan",
    "Kwajalein",
    "Libya",
    "MET",
    "Mexico/BajaNorte",
    "Mexico/BajaSur",
    "Mexico/General",
    "MST",
    "MST7MDT",
    "Navajo",
    "NZ",
    "NZ-CHAT",
    "Pacific/Apia",
    "Pacific/Auckland",
    "Pacific/Bougainville",
    "Pacific/Chatham",
    "Pacific/Chuuk",
    "Pacific/Easter",
    "Pacific/Efate",
    "Pacific/Enderbury",
    "Pacific/Fakaofo",
    "Pacific/Fiji",
    "Pacific/Funafuti",
    "Pacific/Galapagos",
    "Pacific/Gambier",
    "Pacific/Guadalcanal",
    "Pacific/Guam",
    "Pacific/Honolulu",
    "Pacific/Johnston",
    "Pacific/Kanton",
    "Pacific/Kiritimati",
    "Pacific/Kosrae",
    "Pacific/Kwajalein",
    "Pacific/Majuro",
    "Pacific/Marquesas",
    "Pacific/Midway",
    "Pacific/Nauru",
    "Pacific/Niue",
    "Pacific/Norfolk",
    "Pacific/Noumea",
    "Pacific/Pago_Pago",
    "Pacific/Palau",
    "Pacific/Pitcairn",
    "Pacific/Pohnpei",
    "Pacific/Ponape",
    "Pacific/Port_Moresby",
    "Pacific/Rarotonga",
    "Pacific/Saipan",
    "Pacific/Samoa",
    "Pacific/Tahiti",
    "Pacific/Tarawa",
    "Pacific/Tongatapu",
    "Pacific/Truk",
    "Pacific/Wake",
    "Pacific/Wallis",
    "Pacific/Yap",
    "Poland",
    "Portugal",
    "PRC",
    "PST8PDT",
    "ROC",
    "ROK",
    "Singapore",
    "Turkey",
    "UCT",
    "Universal",
    "US/Alaska",
    "US/Aleutian",
    "US/Arizona",
    "US/Central",
    "US/Eastern",
    "US/East-Indiana",
    "US/Hawaii",
    "US/Indiana-Starke",
    "US/Michigan",
    "US/Mountain",
    "US/Pacific",
    "US/Samoa",
    "UTC",
    "WET",
    "W-SU",
    "Zulu",
];

fn utc_micros(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
    chrono::Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .unwrap()
        .timestamp_micros()
}

fn zone_offset_at(micros: i64, zone: Tz) -> Option<chrono::FixedOffset> {
    micros_to_wall_zone(micros, zone).map(|(_, offset)| offset)
}

fn exact_transition(zone: Tz, low: i64, high: i64) -> i64 {
    let target = zone_offset_at(high, zone);
    let mut keep = low;
    let mut drop = high;
    while drop - keep > 1 {
        let middle = keep + (drop - keep) / 2;
        if zone_offset_at(middle, zone) == target {
            drop = middle;
        } else {
            keep = middle;
        }
    }
    drop
}

fn zone_transitions(zone: Tz) -> Vec<i64> {
    const STEP_MICROS: i64 = 3 * 86_400_000_000;
    let mut found = Vec::new();
    let mut previous = utc_micros(1840, 1, 1, 0, 0, 0);
    let mut previous_offset = zone_offset_at(previous, zone);
    let last = utc_micros(2100, 1, 1, 0, 0, 0);
    let mut cursor = previous + STEP_MICROS;
    while cursor <= last {
        let offset = zone_offset_at(cursor, zone);
        if offset != previous_offset {
            found.push(exact_transition(zone, previous, cursor));
            previous_offset = offset;
        }
        previous = cursor;
        cursor += STEP_MICROS;
    }
    found
}

fn assert_resolve_matches(zone_name: &str, zone: Tz, cache: &mut OffsetCache, micros: i64) {
    assert_eq!(
        cache.resolve(micros),
        micros_to_wall_zone(micros, zone),
        "zone {zone_name} micros {micros}"
    );
    assert!(
        cache
            .cached_span_micros()
            .is_none_or(|span| span <= CACHED_WINDOW_MICROS),
        "zone {zone_name} micros {micros}"
    );
}

#[test]
fn cached_offsets_match_direct_lookups_in_every_zone() {
    const DAY_MICROS: i64 = 86_400_000_000;
    const HOUR_MICROS: i64 = 3_600_000_000;
    const SECOND_MICROS: i64 = 1_000_000;
    const NEARBY: [i64; 9] = [
        -DAY_MICROS,
        -HOUR_MICROS,
        -SECOND_MICROS,
        -1,
        0,
        1,
        SECOND_MICROS,
        HOUR_MICROS,
        DAY_MICROS,
    ];
    for zone_name in ALL_ZONES {
        let zone = Tz::from_str(zone_name).expect("zone parses");
        for transition in zone_transitions(zone) {
            for warm in [
                transition - 2 * DAY_MICROS,
                transition - 31 * DAY_MICROS,
                transition + 2 * DAY_MICROS,
                transition + 31 * DAY_MICROS,
            ] {
                let mut cache = OffsetCache::new(zone);
                assert_resolve_matches(zone_name, zone, &mut cache, warm);
                for delta in NEARBY {
                    assert_resolve_matches(zone_name, zone, &mut cache, transition + delta);
                }
            }
            let mut cache = OffsetCache::new(zone);
            for delta in NEARBY {
                assert_resolve_matches(zone_name, zone, &mut cache, transition + delta);
            }
        }
        let mut cache = OffsetCache::new(zone);
        for year in (1..=9999).step_by(100) {
            assert_resolve_matches(
                zone_name,
                zone,
                &mut cache,
                utc_micros(year, 1, 1, 12, 0, 0),
            );
        }
        assert_resolve_matches(
            zone_name,
            zone,
            &mut cache,
            utc_micros(9999, 1, 1, 12, 0, 0),
        );
    }
}

#[test]
fn cached_micros_offsets_match_direct_lookups_on_sampled_years() {
    for zone_name in ALL_ZONES {
        let zone = Tz::from_str(zone_name).expect("zone parses");
        let mut cache = OffsetCache::new(zone);
        for year in (1..=9999).step_by(100) {
            let micros = utc_micros(year, 1, 1, 12, 0, 0);
            let direct = micros_to_wall_zone(micros, zone).map(|(_, offset)| offset);
            assert_eq!(
                cache.resolve_micros(micros).map(|(_, offset)| offset),
                direct,
                "zone {zone_name} micros {micros}"
            );
            assert!(
                cache
                    .cached_span_micros()
                    .is_none_or(|span| span <= CACHED_WINDOW_MICROS),
                "zone {zone_name} micros {micros}"
            );
        }
    }
}

#[test]
fn fixed_resolver_matches_direct_lookups() {
    const DAY_MICROS: i64 = 86_400_000_000;
    let (lowest, highest) = wall_micros_bounds();
    for name in [
        "UTC", "+05:30", "-08:00", "+00:00", "-00:00", "+14:00", "-12:00", "+18:00",
    ] {
        let zone = Tz::from_str(name).expect("zone parses");
        let (_, offset) =
            micros_to_wall_zone(utc_micros(2024, 1, 1, 0, 0, 0), zone).expect("instant resolves");
        let mut resolver = ZoneResolver::fixed(offset);
        let mut instants: Vec<i64> = (1..=9999)
            .step_by(500)
            .map(|year| utc_micros(year, 6, 15, 12, 0, 0))
            .collect();
        instants.extend([
            i64::MIN + 1,
            lowest - 2 * DAY_MICROS,
            lowest + 2 * DAY_MICROS,
            highest - 2 * DAY_MICROS,
            highest + 2 * DAY_MICROS,
            i64::MAX - 1,
        ]);
        for micros in instants {
            assert_eq!(
                resolver.resolve(micros),
                micros_to_wall_zone(micros, zone),
                "zone {name} micros {micros}"
            );
            let direct_micros = micros_to_wall_zone(micros, zone)
                .map(|(wall, offset)| (wall.and_utc().timestamp_micros(), offset));
            assert_eq!(
                resolver.resolve_micros(micros),
                direct_micros,
                "zone {name} micros {micros}"
            );
        }
    }
}

#[test]
fn an_instant_after_2099_is_written_at_the_final_rule() {
    let zone = Tz::from_str("America/New_York").expect("zone parses");
    for (micros, year) in [
        (4_087_802_096_000_000_i64, 2099),
        (4_119_338_096_000_000, 2100),
    ] {
        let (wall, offset) = micros_to_wall_zone(micros, zone).expect("instant resolves");
        assert_eq!(wall.to_string(), format!("{year}-07-15 08:34:56"), "{year}");
        assert_eq!(offset.local_minus_utc(), -4 * 3_600, "{year}");
    }
}
