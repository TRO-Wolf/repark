use super::*;

#[test]
fn plan_id_is_stable_per_pair_and_unique_per_candidate() {
    assert_eq!(plan_id(7, "days(ts)"), plan_id(7, "days(ts)"));
    assert_ne!(plan_id(7, "days(ts)"), plan_id(7, "months(ts)"));
    assert_ne!(plan_id(7, "days(ts)"), plan_id(8, "days(ts)"));
}

#[test]
fn extra_branch_refusal_names_the_branch() {
    assert!(refuse_extra_branches(&[], "sales.t").is_ok());
    let error = refuse_extra_branches(&["feat".to_string()], "sales.t").expect_err("branch");
    let message = error.to_string();
    assert!(message.contains("feat") && message.contains("main"));
}

const RP18_TARGET: u64 = 524_288;
const RP18_BASE_SECS: i64 = 1_672_531_200;
const RP18_SPLIT_SIZES: [(usize, f64, f64); 6] = [
    (32, 28_949.0, 8_609.0),
    (65, 20_247.0, 17_311.0),
    (98, 11_391.0, 26_247.0),
    (131, 2_733.0, 34_949.0),
    (163, 31_649.0, 5_981.0),
    (196, 22_947.0, 14_647.0),
];

fn rp18_stamp_secs(id: i64) -> i64 {
    RP18_BASE_SECS + id * 157_680_000 / 1_000_000
}

fn rp18_synthetic_bed(skewed: bool) -> (Vec<RatedColumn>, Vec<f64>) {
    let mut sizes = Vec::new();
    let mut ts = Vec::new();
    let mut grp = Vec::new();
    let mut id = Vec::new();
    for batch in 0..200usize {
        let low = 2_000 * i64::try_from(batch).expect("batch < 200");
        let high = low + 1_999;
        let group = if skewed {
            if batch % 2 == 0 {
                "g00".to_string()
            } else {
                format!("g{:02}", batch / 2 % 19 + 1)
            }
        } else {
            format!("g{:02}", batch % 20)
        };
        let pieces: Vec<(i64, i64, f64)> =
            match RP18_SPLIT_SIZES.iter().find(|(index, ..)| *index == batch) {
                Some((_, first, second)) => {
                    let boundary = (low / 65_536 + 1) * 65_536;
                    vec![(low, boundary - 1, *first), (boundary, high, *second)]
                }
                None => vec![(low, high, 37_649.0)],
            };
        for (file_low, file_high, size) in pieces {
            sizes.push(size);
            ts.push(Some((
                rp18_stamp_secs(file_low),
                rp18_stamp_secs(file_high),
            )));
            grp.push(Some((group.clone(), group.clone())));
            id.push(Some((file_low, file_high)));
        }
    }
    let count = sizes.len();
    (
        vec![
            RatedColumn {
                name: "ts".to_string(),
                kind: ColumnKind::Timestamp,
                numbers: ts,
                texts: vec![None; count],
            },
            RatedColumn {
                name: "grp".to_string(),
                kind: ColumnKind::Text,
                numbers: vec![None; count],
                texts: grp,
            },
            RatedColumn {
                name: "id".to_string(),
                kind: ColumnKind::Int,
                numbers: id,
                texts: vec![None; count],
            },
        ],
        sizes,
    )
}

type FuturesNumericColumn = (&'static str, ColumnKind, [(i64, i64); 3]);

type FuturesTextColumn = (&'static str, [(&'static str, &'static str); 3]);

const RP18_FUTURES_NUMERIC: [FuturesNumericColumn; 14] = [
    (
        "event_date",
        ColumnKind::Date,
        [
            (1_577_836_800, 1_607_817_600),
            (1_199_232_000, 1_230_681_600),
            (1_451_779_200, 1_483_056_000),
        ],
    ),
    (
        "event_date_utc",
        ColumnKind::Date,
        [
            (1_577_836_800, 1_607_904_000),
            (1_199_232_000, 1_230_681_600),
            (1_451_779_200, 1_483_056_000),
        ],
    ),
    (
        "event_timestamp",
        ColumnKind::Timestamp,
        [
            (1_577_901_660, 1_607_903_940),
            (1_199_253_660, 1_230_740_160),
            (1_451_844_060, 1_483_117_200),
        ],
    ),
    (
        "event_timestamp_utc",
        ColumnKind::Timestamp,
        [
            (1_577_919_660, 1_607_921_940),
            (1_199_271_660, 1_230_758_160),
            (1_451_862_060, 1_483_135_200),
        ],
    ),
    (
        "roll_in_date",
        ColumnKind::Date,
        [
            (1_576_454_400, 1_600_041_600),
            (1_197_849_600, 1_229_299_200),
            (1_450_051_200, 1_481_500_800),
        ],
    ),
    (
        "roll_out_date",
        ColumnKind::Date,
        [
            (1_584_316_800, 1_607_904_000),
            (1_205_712_000, 1_237_161_600),
            (1_457_913_600, 1_489_363_200),
        ],
    ),
    (
        "total_volume",
        ColumnKind::Int,
        [(1, 138_381), (1, 94_549), (1, 173_713)],
    ),
    (
        "total_ticks",
        ColumnKind::Int,
        [(1, 38_222), (1, 8_356), (1, 22_636)],
    ),
    (
        "up_ticks",
        ColumnKind::Int,
        [(0, 21_016), (0, 4_341), (0, 11_768)],
    ),
    (
        "down_ticks",
        ColumnKind::Int,
        [(0, 18_837), (0, 4_315), (0, 11_895)],
    ),
    (
        "up_volume",
        ColumnKind::Int,
        [(0, 76_699), (0, 49_499), (0, 77_998)],
    ),
    (
        "down_volume",
        ColumnKind::Int,
        [(0, 75_946), (0, 54_819), (0, 95_715)],
    ),
    ("open_interest", ColumnKind::Int, [(0, 0), (0, 0), (0, 0)]),
    (
        "interval_minutes",
        ColumnKind::Int,
        [(1, 1), (1, 1), (1, 1)],
    ),
];

const RP18_FUTURES_TEXT: [FuturesTextColumn; 3] = [
    (
        "contract_symbol",
        [("ESH20", "ESZ20"), ("ESH08", "ESZ08"), ("ESH16", "ESZ16")],
    ),
    (
        "ticker_epoch",
        [
            ("ESH20-1577919660000", "ESZ20-1607921940000"),
            ("ESH08-1199271660000", "ESZ08-1229317140000"),
            ("ESH16-1451862060000", "ESZ16-1481518740000"),
        ],
    ),
    ("ticker", [("ES", "ES"), ("ES", "ES"), ("ES", "ES")]),
];

fn rp18_futures_bed() -> (Vec<RatedColumn>, Vec<f64>) {
    let mut rated: Vec<RatedColumn> = RP18_FUTURES_NUMERIC
        .iter()
        .map(|(name, kind, pairs)| RatedColumn {
            name: name.to_string(),
            kind: *kind,
            numbers: pairs.map(Some).to_vec(),
            texts: vec![None; 3],
        })
        .collect();
    rated.extend(RP18_FUTURES_TEXT.iter().map(|(name, pairs)| {
        RatedColumn {
            name: name.to_string(),
            kind: ColumnKind::Text,
            numbers: vec![None; 3],
            texts: pairs
                .map(|(low, high)| Some((low.to_string(), high.to_string())))
                .to_vec(),
        }
    }));
    (rated, vec![18_374_156.0, 19_417_640.0, 19_851_301.0])
}

fn rp18_plan_labels(rated: &[RatedColumn], sizes: &[f64], byte_ratio: f64) -> Vec<PlanFrameRow> {
    let inputs = PlanInputs {
        table_arg: "ns.bed",
        catalog_name: "ap",
        snapshot_id: 1,
        target: RP18_TARGET,
        rated,
        sizes,
        unsupported: &[],
        spec_count: 1,
        byte_ratio,
        ratio_source: "footers",
    };
    build_plan(&inputs).expect("plan builds")
}

#[test]
fn rp18_beds_rank_like_the_live_frames() {
    let (uniform_rated, uniform_sizes) = rp18_synthetic_bed(false);
    let uniform = rp18_plan_labels(&uniform_rated, &uniform_sizes, 0.377_269);
    let uniform_top: Vec<&str> = uniform
        .iter()
        .take(3)
        .map(|row| row.label.as_str())
        .collect();
    assert_eq!(
        uniform_top,
        vec!["identity(grp)", "months(ts)", "years(ts)"],
        "RP-18 uniform top three"
    );
    let uniform_counts: Vec<(i64, i64)> = uniform
        .iter()
        .take(3)
        .map(|row| (row.partitions, row.projected))
        .collect();
    assert_eq!(uniform_counts, vec![(20, 20), (24, 24), (2, 6)]);
    let (skewed_rated, skewed_sizes) = rp18_synthetic_bed(true);
    let skewed = rp18_plan_labels(&skewed_rated, &skewed_sizes, 0.377_269);
    let skewed_top: Vec<&str> = skewed
        .iter()
        .take(3)
        .map(|row| row.label.as_str())
        .collect();
    assert_eq!(
        skewed_top,
        vec!["months(ts)", "identity(grp)", "years(ts)"],
        "RP-18 skewed top three"
    );
    let skewed_counts: Vec<(i64, i64)> = skewed
        .iter()
        .take(3)
        .map(|row| (row.partitions, row.projected))
        .collect();
    assert_eq!(skewed_counts, vec![(24, 24), (20, 22), (2, 6)]);
    let (futures_rated, futures_sizes) = rp18_futures_bed();
    let futures = rp18_plan_labels(&futures_rated, &futures_sizes, 0.462_675);
    let futures_top: Vec<&str> = futures
        .iter()
        .take(3)
        .map(|row| row.label.as_str())
        .collect();
    assert_eq!(
        futures_top,
        vec![
            "months(event_date)",
            "months(event_date_utc)",
            "months(event_timestamp)"
        ],
        "RP-18 futures top three"
    );
    let futures_counts: Vec<(i64, i64)> = futures
        .iter()
        .take(3)
        .map(|row| (row.partitions, row.projected))
        .collect();
    assert_eq!(futures_counts, vec![(36, 72), (36, 72), (36, 72)]);
}

#[test]
fn rp18_beds_projection_bounds_the_live_actual() {
    for (name, byte_ratio, actual) in [
        ("uniform", 0.377_269, 1_839_168_u64),
        ("skewed", 0.377_269, 1_755_749),
        ("futures", 0.462_675, 26_729_684),
    ] {
        let (rated, sizes) = if name == "futures" {
            rp18_futures_bed()
        } else {
            rp18_synthetic_bed(name == "skewed")
        };
        let rows = rp18_plan_labels(&rated, &sizes, byte_ratio);
        let unpartitioned = rows
            .iter()
            .find(|row| row.label == "unpartitioned")
            .expect("unpartitioned row");
        let bound = u64::try_from(unpartitioned.projected).expect("positive") * RP18_TARGET;
        assert!(
            bound >= actual && bound <= 2 * actual,
            "RP-18 {name}: projection x target {bound} bounds the live actual {actual} within 2x"
        );
    }
}
