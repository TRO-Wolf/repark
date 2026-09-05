"""Pin the session split compatibility contract.

pins: production-file-size/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import ast
import dis
import hashlib
import os
import subprocess
import sys
import types
from pathlib import Path

from repark.spark.session import _funcs

ROOT = Path(__file__).parents[3]
SESSION = ROOT / "python/repark/src/repark/spark/session"
MODULE_FILES = (
    "_funcs.py",
    "session_configuration.py",
    "catalog_resolution.py",
    "session_state.py",
    "reader_support.py",
    "create_dataframe_values.py",
    "create_dataframe_schema.py",
    "create_dataframe_rows.py",
    "create_dataframe_inference.py",
    "create_dataframe_arrow.py",
    "create_dataframe_tuples.py",
    "create_dataframe_columns.py",
    "sql_udf_parsing.py",
    "sql_udf_rewrite.py",
    "sql_udf_discovery.py",
    "sql_udf_residual.py",
    "sql_udf_materialization.py",
    "sql_relations.py",
)

EXPECTED_SYMBOL_HASHES = {
    "_ARRAY_TYPECODES_SUPPORTED": (
        "2b09eac56acf91b21c9e11e31b7418f207abc8a91701f028749d38fe8c7a532c"
    ),
    "_AUTO_MEMORY_CATALOG_KEY": (
        "c1adb93336e4928d779efbe15cc2807d8f0a9a944962f2df7dc2a50c368810c2"
    ),
    "_BATCH_SIZE_KEYS": ("c6e8e092205f84da0cd4a4a713f78aa2e15cbae0ce1ef81e4b48c91d7069eb96"),
    "_CONF_GET_UNSET": ("25ba042075760c94f2215645c784f07f44559548367e879535e7b5b56659e061"),
    "_CREATE_TABLE_PREFIX_RE": ("4418d81fb01dd3283bf35deb38db9cc6f6cedb1121d97504cfd21a66bbcd4ac0"),
    "_CREATE_TEMP_TABLE_SQL_RE": (
        "a12bc15fe92be778c5e4dcad3d4afec7f4c3dc9b703bc77502fad3634f4bca51"
    ),
    "_CREATE_VIEW_SQL_RE": ("103bd4882a26a6712d5b0eeb140dcd4c1dbf7d39ef272928bbe9dce905a90d6f"),
    "_CSV_NATIVE_OPTION_KEYS": ("d0f743f41e3a04767b0834d202bd7887a186b0566ad0406629338c52fa6d8e0d"),
    "_CSV_UNSUPPORTED_PARSE_OPTIONS": (
        "a682b3aa888458d288c0d36a45d7521e1ad24d013b19ab02e3c2a224fc8f73e7"
    ),
    "_DATAFUSION_CONF_KEY_RE": ("bb70d235333c0cf92b1025ab19b92212c2876c37860d7dca0eee82dcb9fca800"),
    "_DATAFUSION_CONF_PREFIX": ("4dd4ed9b9ee8cafd960ffa169e4bb628ce8a526c73ce8d55ca58ca26bcc0572a"),
    "_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY": (
        "9f4d5ebcef68db7d6560ed6210e15e1fccd00bc1c7e36f7afc203f6acee30db4"
    ),
    "_DECIMAL_MAX_ABS": ("87cc55b915e549287365e7d222518eeb9999b6e436e1b6ab37bb9d4d6b8c3d51"),
    "_DECIMAL_PRECISION": ("0883a8680597fd4423d1e3b1e379d3d4f49b41ce6399073c2ca553a0e0bc455e"),
    "_DECIMAL_SCALE": ("53d2b87b578a676002af32004fc0aaf05a7048d0d1edc17ed2a7c8bf96474d73"),
    "_DEFAULT_DISPLAY_STYLE": ("839025eb6f1fac45d1b72864dcdddd2ed22d1e5ec91322148e30f226927a7197"),
    "_DELETE_FROM_PREFIX_RE": ("b6438951a20e526aaf712b1bbd8017d43295d4fd3fe447215ba6d61997a6d25e"),
    "_DISPLAY_STYLE_KEY": ("8fc28d05fed3c3132f5b39716acc4f797826de45a7385a8bbd8e1d1f5eedc2e0"),
    "_DISPLAY_STYLE_VALUES": ("3514b179fdbe6716d5638336c545fbcd73ed0ecab523288a7721b9a463d40d6d"),
    "_DROP_TABLE_SQL_RE": ("9f1f715085140f3e60d91fbdbbf0cc5754bde758bcda0663332849f73cf06507"),
    "_EXCEL_NATIVE_OPTION_KEYS": (
        "971e7cb98537ecd46ec89e9d2e4acd54c0a5f945b959a3f3c47fbcf4d5f2cf8e"
    ),
    "_FROM_JOIN_NON_TABLE": ("68e99163e41df27d4e4aad14c79576f4691ad9c199b119febc6e2874894b7d4f"),
    "_I64_MAX": ("131a62fa4849d4a8aa52f0cdf4c33468c98cb6e679d1bb579a4113b0d8c56a8b"),
    "_I64_MIN": ("c660afc1bc657e3024405ead6359674d27d7a8d455b8a0f456b75e1cbdfc9b2c"),
    "_ICEBERG_TIME_TRAVEL_OPTIONS": (
        "8340b52c6f082f3b39c44d0855a4502d669dad22535f82c8c56266c35bf86475"
    ),
    "_INFER_NESTED_DICT_AS_STRUCT": (
        "cd8bfab6e2f35ada9a524f2fab48d809a561a8e25837ce5d1bed7ff0597758b9"
    ),
    "_INSERT_DIRECTORY_HEAD_RE": (
        "706df831aae8b6ce00d30c41a73bede03fdd209a37f78e7b0e60d276ef7e7d7d"
    ),
    "_INSERT_PREFIX_RE": ("5c737dc9b38f7d98e6440ea4b1c0f8ef6d55adffe84abd86e657fa2cc028c35b"),
    "_JSON_NATIVE_OPTION_KEYS": (
        "e81ca03b6c3b47ef0b1707a4bcdd5885d7df4c81685bff83a5be0b8dc095a68d"
    ),
    "_JSON_UNSUPPORTED_PARSE_OPTIONS": (
        "1b7db6e23acbbf454e43ecbfccf9b74132e158337088df31a2064556ead03809"
    ),
    "_LEGACY_FIRST_ELEMENT_COERCE": (
        "f9bad7971a392d2154df0a01255f6c571111998e082e3ff6d3de62f0bbce3dc3"
    ),
    "_MEMORY_LIMIT_KEYS": ("507db1ffcd2e1d3ef7354c7d96e7ce2b8d4ebf15e5eed8b8a48bf423de004258"),
    "_MEMORY_LIMIT_KEY_LOWER": ("d917500eae9a148793df59e3091f74beca14ba697a323b8ebcca9c0d95db6f33"),
    "_MERGE_INTO_SQL_RE": ("64434ba630c1191ca4f14a3af31348a966c3b62f9ca9e7bb527c4be9557785ef"),
    "_NUMPY_DATETIME64_DATE_UNITS": (
        "dac69949281e00f556bd5372ae6ff933776e1175093d3d21afac53aa3caf3c83"
    ),
    "_RELATION_FOLLOW_KEYWORDS": (
        "93037a5f95dc6071933ee6a083a58e4348315280fce73772c63d852c0759a92c"
    ),
    "_SELECT_OR_WITH_HEAD_RE": ("0f99601ec674e31813620a9b6994cd921b741ca00fabdc39caef61907861d5df"),
    "_SPARK_SCALAR_MERGE_KIND_ORDER": (
        "49ad093f9e7c844f8d28f1ba7e6fc076e06bae2425a82eb4302aa605a6568103"
    ),
    "_SPARK_SCALAR_MERGE_LABELS": (
        "3ec9222f7d117ea3c837932d15a5fc76da0e56ef7f90cea2c582149b6e8dc698"
    ),
    "_SQLCONF_DEFAULTS": ("609aab19b799795bdbe4eadb2281e23c273824f95660f6370105a59550b8229d"),
    "_SQLCONF_STATIC_KEYS": ("2c84e6ea547146fc355d10edcd2b83ae5c099c343f89c7df7ead1cfd68aa1d0b"),
    "_STOPPED_MESSAGE": ("ff5d7792959eab651b434834b392792dfdf05bcd0e5ccb61268e4ba4b6f18397"),
    "_TARGET_PARTITIONS_KEYS": ("1eccee64899f848c45eaad695a0eb745e6a9aef804baa7714261f5522c0eb131"),
    "_TYPED_NULL_SQL": ("72e1d887dca09d9a26216d1b81eaf676f43df92d98ed7697ecce0aa07622f0c1"),
    "_UNSUPPORTED_SEMANTIC_READER_OPTIONS": (
        "c13ff63f0948d3f06f3a065e67523815e551d0e1b7457ade06c914988644323e"
    ),
    "_UPDATE_PREFIX_RE": ("c4648cd4fe7322523964882afed556654dca844366eb3d9c2b556bcc968967b4"),
    "_active_session": ("42f1cae2431da56af3fdcc6c9e96134a73146a0e10492fe56a10912b8dcd4086"),
    "_alias_catalog_name": ("045854c59387461de24fa461c590f491b87e631c07abf65e7efc7b880246cbe5"),
    "_apply_builder_datafusion_conf": (
        "a45aebf3f9dbc663ea8893754b07aea6452e2d5edf769bb3488b94210cf1fad5"
    ),
    "_apply_permutation": ("51615a98da8a4613ffdf603ad85718ffcefadaa4af7a1bdaefbca5ddd895b1f4"),
    "_array_typecodes_supported": (
        "e6daad82bcaf3b78325991a331b728e8566c80cf2e9ec2c76fe60d2e10e0789b"
    ),
    "_arrow_null_sql_to_type": ("fd98afc017a68f70a198f4bd8b0fc67f84c8fdaee406e019865cd2a43b7fe35f"),
    "_arrow_table_from_pandas": (
        "b5d414309e2a623f1e260ba8a28b8a379cb215aab7b0dbfbcdd8946befe9e5d5"
    ),
    "_arrow_table_from_polars": (
        "2277a1cce4c5229beaa00ba669197a45a4ec1ef551bd50fabee387bb2aa61df8"
    ),
    "_arrow_table_from_raw_tuples": (
        "5e3ab99955bf4d73ed086b20eee010fc60403a4afbdbb059560dac0b6627fb43"
    ),
    "_arrow_table_from_raw_tuples_fast": (
        "101a7b75570ecef723dc96b8858757919a4df32ce0d9fe5c448d0d7e367b53a8"
    ),
    "_arrow_table_from_raw_tuples_legacy": (
        "075bb5efe8ce7da91bae93c336cc2b0586e98d2d37a57f2f079ec136bbc2f769"
    ),
    "_arrow_table_from_tuples": (
        "529869d54a0a960b62cd73714d58e3c40c0b3802bddbbdb286c45908397f87cc"
    ),
    "_arrow_type_for_typed_null_sql": (
        "4564f5c0897ae1969a4053fbd9a1b4f029b626ca9febe2613af8b83d34e8f975"
    ),
    "_arrow_type_is_nested": ("d444bf00f930013dddb015316cfa3b7b8407febbb23aa72149f7d865c525e6f6"),
    "_arrow_type_merge_label": ("92fac695bb138a1b6db0429bbef68d9aff58b17a493218eaa72c5ec997476c1e"),
    "_auto_memory_catalog_wanted": (
        "b9c26fc6f01a564a85f68939fbfd93158a2ee7306cf8fb283f86bb243d0c9bde"
    ),
    "_bind_named_row": ("7b718090cccb37aa44f49729e9f07f40f90e263f4ba1dbfb9e7f83b045e85f65"),
    "_builder_has_memory_limit_key": (
        "0e9a4c88189ff345fa6fc2069f533610dac813e001233009988c1dcba31fb9f2"
    ),
    "_catalog_names_from_builder_config": (
        "6ab06485bcd352be62a28a615e46a5d6c96419652c578e7618b7c2731b1739e7"
    ),
    "_coerce_schema_names": ("dd46fb2f33b15b065cb8fb3688eb2728b7bd86a17186884699e82dc91b72aa52"),
    "_collect_cte_names": ("bb8cd10df6e3bdf058a4524f279ef444fa7d5ec8331c21b95273e227c50ad5cd"),
    "_column_null_sql_from_raw_tuples": (
        "3343f53e64755500788d291cc68dd36223d99f02f0aedd523ebacc929384adb3"
    ),
    "_config_value_error": ("382e2fae1c4ba4641fea43c8aead9040749dab0b82054f8c1e2220ddb8b71ae5"),
    "_create_dataframe_from_rows": (
        "957e98392c344c4d3f8125a5f4282cb05f63d8ee27d1871b1476b19a9809f2a8"
    ),
    "_create_dataframe_from_rows_inner": (
        "db5ae911ff2aa17e40c2dc14714751331b87d238062ce759ec3e26579cace5fe"
    ),
    "_data_type_to_sql_type": ("1633f6159213794bb60b2c6c6bfb633273d9b9242ac991365d1685b8613a78a4"),
    "_datetime64_unit_from_dtype": (
        "addd824624710a22a898d6f94862268f5e06e8f6d3c482674ee44e425765e04f"
    ),
    "_default_catalog_from_builder_config": (
        "21f16722a4af5429a0766b90cb71d1a7f49862e79cdd4668498919c238d90776"
    ),
    "_default_namespace_from_builder_config": (
        "153a6db07dd94da4931b14c6708c46b99503bfea486e9260c893da402c6ed034"
    ),
    "_drop_cdf_temp_view": ("1a61e8fe22b5efc4e0c04253afea7a95a694824c4f05204345c6178acc5d4c4b"),
    "_empty_frame_sql": ("327584de2fd110c2e8c6425d2d2f094d0de4825d1aceb2a1e0eff3b4c4f3f3ac"),
    "_empty_typed_arrow_frame": (
        "08c3eb96462d159b1b6d681df3b1f1aa806b4a41d53b9159798c3fcaf3e363be"
    ),
    "_find_matching_paren": ("cf0e124bf16b10dedacbd7819863fcd4ab64fbe9fc878dfbeec574c468047e1d"),
    "_format_datafusion_set_sql": (
        "1d2e344dee78ff621e20d878701fd3da50599e3c4879c76485ef9dc93a71f9ff"
    ),
    "_forward_datafusion_conf": (
        "f538f4aa93cd6fff1585c5b200e651cefb86f61097b5264cc45d93a9a79f6026"
    ),
    "_infer_arrow_type_from_python_sample": (
        "03a76e73c5c2a06d49feb91028fd9ee2bf68b22cf12716700fbaaa741a38f157"
    ),
    "_infer_null_sql_from_raw_cells": (
        "3d27ce840db78865ebe6220fb834587cc8d0c08c0374978f0ec6d3a7c7fc5c02"
    ),
    "_infer_struct_arrow_from_dict_samples": (
        "78a3a73cac2f7f922df0125171e4f9847f6862c52087c9984fb0df7485ff007b"
    ),
    "_is_datafusion_conf_key": ("945f8502525c401e5b3f9cb4e618773fc7e1e689378d446e4425e6809df5a462"),
    "_is_pandas_dataframe": ("d17a98bd8fc0df22e4d5fe59a6839a3f92732570966e1a6465a7c416a29669a4"),
    "_is_polars_dataframe": ("773463ecd731420002b71110228c1c9f5c8389da1a22e0861144e2066e7080c1"),
    "_join_table_identifier_segments": (
        "1f40e00deb546d035c57a50636091a47c12141fca249daf2e1e96eb2f15d065e"
    ),
    "_json_input_nonempty": ("a44ccb975eef55e59dff5ef507d3ae146c3562a29326828fb6c498829c02e2f3"),
    "_json_multiline_empty_schema_is_mismatch": (
        "a0bf657b6a92ca06bd41cb5beff5b17ea6962aa67b9ee4e0fb3a494ff28770b9"
    ),
    "_late_catalog_names": ("dba968dc0a61a68278a051774cfc56e4611402dcd2b30b6e9599b901425f06af"),
    "_localize_naive_timestamp_column": (
        "84ce679d83169827568652608a507066a679387cebb9e52b054ccef0eea1b4fc"
    ),
    "_looks_like_datafusion_conf_key": (
        "e0cbe8882612065d791dc390a747eaf69ce80d865aab3f6764527791aeb145c4"
    ),
    "_master_warned": ("1d1dfb629085b4810080a912d37b236e3a46e39d39340efd26798e99ebd1db83"),
    "_match_from_or_join_keyword": (
        "536a4c6c4810c55736b8ca642f4189f39e811b2245bc7ddf0e1d7c58b4aa2af1"
    ),
    "_materialize_arrow_as_memtable_frame": (
        "701fce3a0cc294a2b919f3f97777d53128f742e2e03dfc82d07d1fc1d04e89dd"
    ),
    "_materialize_values_as_memtable_frame": (
        "6161ab242a59a6829c18f8cfc4e8a26cbebdd1ba667a4b49836e1a20ca32d0a6"
    ),
    "_merge_inferred_arrow_types": (
        "57700f5b94f7ceaf04c261e55f7a365b4a2453e9995775418cab2558a3b2f437"
    ),
    "_merge_struct_arrow_types": (
        "c108b14b3b9578f14060e1bbd3ef6390202ed0f463a8b9f682293843fd41b93a"
    ),
    "_normalize_create_dataframe_cell": (
        "8e31e7a9fbde9c3a49ae7ee75a850a3970ba6f293c6c79b75229a6d2dc08d4b6"
    ),
    "_normalize_frame_arrow_column": (
        "a3cccd45734740561e2f181b4b5bf8ac2439e0a68bd2330a8b3f87b2422a6ff0"
    ),
    "_normalize_nested_sql_type_aliases": (
        "d93a752be5747cda438db978e3100680dd19fd17ed00a16dd9a7b3d1a1742904"
    ),
    "_null_sql_for_pandas_dtype": (
        "082166d4238b31f072610f99862f89ad3898c87298c273b825640a1fa4fe88be"
    ),
    "_null_sql_for_polars_dtype": (
        "2513a697619e2b2a20b482c816b254012f67063611fa48115d87146328db5a32"
    ),
    "_numpy_datetime64_unit": ("7d3dc236af2ed8f4696271213f510d0626b4b0f9552843b549668aa03f248823"),
    "_pa_array_or_refuse": ("a70d70a7b5aa29e04adc8c56561724469135935e0c02542cf645a5142e048a0d"),
    "_pandas_dtype_needs_object_null_witness": (
        "f1ce66012fa5ec5d1d583d2bdf22490d4d5c0c19cb3eba3b7b2d4740ce89fb6f"
    ),
    "_parse_create_dataframe_schema": (
        "21e22aa6258c57cfaa589197c112e2d76e73d8d9debfe604ae2f0f45b3dba75f"
    ),
    "_parse_jdbc_int_option": ("3c2424b29ab3021d9d50d77b7a894ca61008974ad896a9d5705240a9807c4ebc"),
    "_parse_schema_ddl": ("f85655a15284092b9ba36ffad12139d4a802702274775be232aa769b1a37410f"),
    "_parse_simple_sql_udf_call": (
        "1487ad6ad0fd8406e55a935574d1af0dc7d65f3e808e6bbf55b0a1ee0e65d3ba"
    ),
    "_parse_table_identifier_segments": (
        "3a1f2288a57306e55c0fbaa2a589aab2dd0880763e5c6f876dc138efec4714f9"
    ),
    "_prepare_nested_cell": ("fe9a531e3994ae6480ad877931fd4d3b8d4542f66c1e3b31a2a324f564c754b6"),
    "_promote_csv_string_types": (
        "a9d6b9c3b3f1672d874075558dc2c1f189267dbd8d7ee63d69e2f48983f243b1"
    ),
    "_python_scalar_merge_kind": (
        "85a1a39e3f8170786cc246e35c3f263c32ec17365fa570d056fd62c330f93d3f"
    ),
    "_reader_path_to_str": ("5893ed798a89b11a31507fe9fe90771a417de3b07121710876a9ab5e265f956a"),
    "_refuse_dual_memory_pool_knobs": (
        "5ef838c25d3bfb46ac873cdaa661857916573cd76890cd64ba81a9aa8b6e45f1"
    ),
    "_refuse_duplicate_pandas_columns": (
        "0fb680bfb6526fb83cad0f31fb504570dc425a078db1f9213f3eb5ae913832ba"
    ),
    "_refuse_duplicate_tuple_column_names": (
        "68d139b7f2ec227338fecc5c155f28762253b99e0345fab3cca6bbbcf3de561b"
    ),
    "_refuse_incompatible_scalar_merge_kinds": (
        "ccb4fb30beab7c05b5cd24885790a230dc84c6227aea51e466c93da18cd6c38b"
    ),
    "_refuse_list_element_type_merge": (
        "eb44e9b735565ca0caeadf33f0a2a0151788f59b1002e688b68a17f1d251b2d9"
    ),
    "_refuse_long_double_merge": (
        "97b8a3fcffe27a57edaf6f2c6143233138d2670cffac4bab8546149718c79397"
    ),
    "_refuse_runtime_memory_limit_gb": (
        "e53158b309b2557330cb28b4c661ad78e79da3146a7b1e5e3fa46db1b46168d1"
    ),
    "_register_cdf_view_cleanup": (
        "e96e59d6e51deacca900e757fef11c50a8e7c0f7ee554919666ca240bd461c51"
    ),
    "_reset_active_session_for_tests": (
        "3c95dbd59232221cd2ccfe9fef426df679b6bddbb2915b53b896abf9b3a77ec9"
    ),
    "_reset_dropin_warnings_for_tests": (
        "2cf60e5958dd8ae526edceb5e99de10a6de447a278b141a6198978c474ee3ac6"
    ),
    "_rows_from_mapping_list": ("f0e6d804ec160f2d41f524f947d07676fa2f291fcfd0e46fef5b29896639a94d"),
    "_rows_from_pandas": ("793ee7519856503c01bf3afcd30b0aff09503a39a2e3c14c9b72e0c1ed665b69"),
    "_rows_from_polars": ("9b85f5efbe8418db0295ccd5e6e4c6f3350c6871c9146dd0bce26eef320f757c"),
    "_scan_sql_table_identifier_end": (
        "e87f6e939c24152fb4903a92a1910065936eaed955fad13eacb1186f32f46c7e"
    ),
    "_schema_fields": ("034ec372d6e657ac6e453b34586c51e03d96def8a112494f7c1146cdf6d13304"),
    "_schema_names_and_permutation": (
        "370d45b8a92d06f2204e0172140f0d96b7d2d9a208be39d85be223df6d09cd87"
    ),
    "_skip_sql_ws_and_comments": (
        "5354bccdaf08190a4f40b62839369af5baa1670396e4f805dfcb4cc861c9d095"
    ),
    "_spark_dict_key_union_order": (
        "f40640208b6599a9b17efadd68b390bfa1d5d6095b2eaa7b32d108392ed1f330"
    ),
    "_split_leading_sql_trivia": (
        "d3b3030b3f26659b58b735616ecdab85c017ca323af4e9b8ae4ec1439dc76da4"
    ),
    "_split_leading_table_ident": (
        "cf7bbcd62531d2b26ae1111d7b7f392b2bff77ac666f64b59fb3a3085a32c413"
    ),
    "_split_sql_select_list": ("e406776478b038aaa50860a11af47c53797bc6225e07be450741b880f9a8f552"),
    "_split_sql_table_name_list": (
        "efe1a97afc879352f1d6dec3470cf335946791487983e988c915ad90aa3c0c7a"
    ),
    "_sql_collect_registry_udf_hits": (
        "587e97d355706e88eabbb224e892896dfa3bd1990e9f23dc3f0500b1d77da421"
    ),
    "_sql_find_registry_udf_calls": (
        "332d5d9c491c9eafd0577da79e5d0299fb878205908cb5646c77d3e08b6bd6d1"
    ),
    "_sql_literal": ("714e8b7794873796e8808c8990e40825926ed8410e280609ec251587d663164b"),
    "_sql_mask_strings_and_comments": (
        "4b7f0e357d15f9ef25369f7221b95fb7c05a4db373acc03fcc00c860d360d8ed"
    ),
    "_sql_materialize_expr_udfs": (
        "b33af062b1aa008a29597d606d6de72fd02b1b62da69ebc73e41a0702f658559"
    ),
    "_sql_peel_select_trailing_clauses": (
        "b5b6b0609de791bd5cf79fec0c64cb3369a0145d2adc524b9f49f188983d5824"
    ),
    "_sql_plan_order_by_aliases": (
        "508d4a6115f06b60cd2d6b68439b2bfcc1b7d22486c14ddfafadce4e724f9bec"
    ),
    "_sql_residual_has_subquery": (
        "2dcde5ce00b36ca11eee0e3fbb37a82bb75a74109b682199953a417e78913070"
    ),
    "_sql_strip_comments_preserve_strings": (
        "992a00d5a057fc07c3c7c03ac209f07fcdf72c3f2d766e55ff7768f7e5b36bef"
    ),
    "_sql_table_ref": ("55d5693a8fc7f585736ba57f95b80cfbd92a4226e2f5e85dbd83c4f4b4853f29"),
    "_sql_top_level_keyword_index": (
        "8d33d0dfa976fc6e0b6140838507e8d5aaaadf6283ccae14c0927fe3c2baffff"
    ),
    "_sql_type_to_arrow": ("df5b483b8762fe442b4630d8e6bc66ab8f623d1789257aec5b1694f17578e73e"),
    "_sql_udf_arg_is_simple": ("db0b84f0ee3d3410bbb660ac228ab85a8be365f5d17f4ba9623fc7d7ff982bce"),
    "_sql_udf_call_match_key": ("e5d8292341262271b9099a26080982055c0ca1a77274f75f738e458a1e045e99"),
    "_sql_udf_clean_exception": (
        "3d2e48861e6677d9508e4a253bd812e655169979a54584ac69d0c8c15d686499"
    ),
    "_sql_udf_in_nested_subquery": (
        "a8d7beee8e9f9f7a07b3190b75eb0f125487a1ea2a97184ea27e2ea3496585ef"
    ),
    "_sql_udf_public_error_text": (
        "cc7eb644d47bcbeb4e350752f2883726e663bb28af44f9565efb1ca813aef4fc"
    ),
    "_sql_where_residual_base_projections": (
        "3ed9fe9841bf681c43959aa2b613358706133ae296227c650684a8dd0922d0e8"
    ),
    "_supported_array_typecodes": (
        "2aca58bde81ceb539ba3bb0d1ceaaf4932fb9dc1511a14beeed3812567952e04"
    ),
    "_sync_display_style_into_builder_config": (
        "a9bb3ca992f74ca72c0df9a86ecedfa4f3f823f1fd03492925b50039c5a83f94"
    ),
    "_to_str": ("0f0153e803cb1aeaee5f39310973385c3c3b5c9a9d9f8288c43d4fb7b72855ea"),
    "_try_rewrite_select_list_python_udfs": (
        "2eeb6c6f87e06a91dba046290852b963f8ebc684c0311f6f6fe70ed058594113"
    ),
    "_unbounded_batch_warned": ("16df4f8b7be6bea1ff40984e9e382317ed86bb54403eea374b1467d377f5da11"),
    "_update_rest_has_set_clause": (
        "e31e2f9f95bb70759d93a3171bfa084e5b90b1fc2ee32e0328e4173f7fdaeffd"
    ),
    "_validate_decimal_column_envelope": (
        "ce721975668df3006331b41a052a75a5dab1194cf418b5e732818e62f516e37b"
    ),
    "_validate_decimal_envelope": (
        "7cbe95cc4e3bf1eded5ae7383d727f81ba2b01bee4ff6009b5670fb37d526d3f"
    ),
    "_values_sql_with_explicit_casts": (
        "c7791529067d8df8ad40d926c911d3ac261fdf4aa614d3bfadd392bffa76d027"
    ),
    "_values_sql_with_typed_nulls": (
        "b9d719528afdb528f9b496ed3faadafadcd92aae7eeb591eee619941edf86ab5"
    ),
    "_warn_master_once": ("6478827c8e6a2af23903d2345be582926e74f931a4f72f323638fcc97de2618a"),
    "_warn_unbounded_batch_once": (
        "3c0a7d26edc06b421c7910e87706b7efdf978ec603664a267d167d82b149d92b"
    ),
    "logger": ("fa49a10e7315bca551601a1c6c048afadc925fcc6a050e5bfcd74012a380f91e"),
    "normalize_display_style": ("8b1e207bfcb7f37f433f026942124dd695674b14369d7a18bf3dbcc6b311b9ec"),
    "resolve_table_name": ("6450b57013df334a7c72a8a0a0258b51b8fd6c6d665d65bda275241a2c2ea4c5"),
}

EXPECTED_OWNERS = {
    "_ARRAY_TYPECODES_SUPPORTED": "create_dataframe_values",
    "_AUTO_MEMORY_CATALOG_KEY": "catalog_resolution",
    "_BATCH_SIZE_KEYS": "session_configuration",
    "_CONF_GET_UNSET": "session_configuration",
    "_CREATE_TABLE_PREFIX_RE": "sql_relations",
    "_CREATE_TEMP_TABLE_SQL_RE": "sql_relations",
    "_CREATE_VIEW_SQL_RE": "sql_relations",
    "_CSV_NATIVE_OPTION_KEYS": "reader_support",
    "_CSV_UNSUPPORTED_PARSE_OPTIONS": "reader_support",
    "_DATAFUSION_CONF_KEY_RE": "session_configuration",
    "_DATAFUSION_CONF_PREFIX": "session_configuration",
    "_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY": "session_configuration",
    "_DECIMAL_MAX_ABS": "create_dataframe_values",
    "_DECIMAL_PRECISION": "create_dataframe_values",
    "_DECIMAL_SCALE": "create_dataframe_values",
    "_DEFAULT_DISPLAY_STYLE": "session_configuration",
    "_DELETE_FROM_PREFIX_RE": "sql_relations",
    "_DISPLAY_STYLE_KEY": "session_configuration",
    "_DISPLAY_STYLE_VALUES": "session_configuration",
    "_DROP_TABLE_SQL_RE": "sql_relations",
    "_EXCEL_NATIVE_OPTION_KEYS": "reader_support",
    "_FROM_JOIN_NON_TABLE": "sql_relations",
    "_I64_MAX": "reader_support",
    "_I64_MIN": "reader_support",
    "_ICEBERG_TIME_TRAVEL_OPTIONS": "reader_support",
    "_INFER_NESTED_DICT_AS_STRUCT": "create_dataframe_inference",
    "_INSERT_DIRECTORY_HEAD_RE": "sql_relations",
    "_INSERT_PREFIX_RE": "sql_relations",
    "_JSON_NATIVE_OPTION_KEYS": "reader_support",
    "_JSON_UNSUPPORTED_PARSE_OPTIONS": "reader_support",
    "_LEGACY_FIRST_ELEMENT_COERCE": "create_dataframe_inference",
    "_MEMORY_LIMIT_KEYS": "session_configuration",
    "_MEMORY_LIMIT_KEY_LOWER": "session_configuration",
    "_MERGE_INTO_SQL_RE": "sql_relations",
    "_NUMPY_DATETIME64_DATE_UNITS": "create_dataframe_values",
    "_RELATION_FOLLOW_KEYWORDS": "sql_relations",
    "_SELECT_OR_WITH_HEAD_RE": "sql_relations",
    "_SPARK_SCALAR_MERGE_KIND_ORDER": "create_dataframe_tuples",
    "_SPARK_SCALAR_MERGE_LABELS": "create_dataframe_tuples",
    "_SQLCONF_DEFAULTS": "session_configuration",
    "_SQLCONF_STATIC_KEYS": "session_configuration",
    "_STOPPED_MESSAGE": "session_state",
    "_TARGET_PARTITIONS_KEYS": "session_configuration",
    "_TYPED_NULL_SQL": "create_dataframe_values",
    "_UNSUPPORTED_SEMANTIC_READER_OPTIONS": "reader_support",
    "_UPDATE_PREFIX_RE": "sql_relations",
    "_active_session": "session_state",
    "_alias_catalog_name": "catalog_resolution",
    "_apply_builder_datafusion_conf": "session_configuration",
    "_apply_permutation": "create_dataframe_schema",
    "_array_typecodes_supported": "create_dataframe_values",
    "_arrow_null_sql_to_type": "create_dataframe_arrow",
    "_arrow_table_from_pandas": "create_dataframe_arrow",
    "_arrow_table_from_polars": "create_dataframe_arrow",
    "_arrow_table_from_raw_tuples": "create_dataframe_columns",
    "_arrow_table_from_raw_tuples_fast": "create_dataframe_columns",
    "_arrow_table_from_raw_tuples_legacy": "create_dataframe_rows",
    "_arrow_table_from_tuples": "create_dataframe_tuples",
    "_arrow_type_for_typed_null_sql": "create_dataframe_tuples",
    "_arrow_type_is_nested": "create_dataframe_inference",
    "_arrow_type_merge_label": "create_dataframe_inference",
    "_auto_memory_catalog_wanted": "catalog_resolution",
    "_bind_named_row": "create_dataframe_rows",
    "_builder_has_memory_limit_key": "session_configuration",
    "_catalog_names_from_builder_config": "catalog_resolution",
    "_coerce_schema_names": "create_dataframe_values",
    "_collect_cte_names": "sql_relations",
    "_column_null_sql_from_raw_tuples": "create_dataframe_schema",
    "_config_value_error": "session_state",
    "_create_dataframe_from_rows": "create_dataframe_rows",
    "_create_dataframe_from_rows_inner": "create_dataframe_rows",
    "_data_type_to_sql_type": "create_dataframe_values",
    "_datetime64_unit_from_dtype": "create_dataframe_schema",
    "_default_catalog_from_builder_config": "catalog_resolution",
    "_default_namespace_from_builder_config": "catalog_resolution",
    "_drop_cdf_temp_view": "create_dataframe_rows",
    "_empty_frame_sql": "create_dataframe_rows",
    "_empty_typed_arrow_frame": "create_dataframe_rows",
    "_find_matching_paren": "sql_relations",
    "_format_datafusion_set_sql": "session_configuration",
    "_forward_datafusion_conf": "session_configuration",
    "_infer_arrow_type_from_python_sample": "create_dataframe_inference",
    "_infer_null_sql_from_raw_cells": "create_dataframe_schema",
    "_infer_struct_arrow_from_dict_samples": "create_dataframe_inference",
    "_is_datafusion_conf_key": "session_configuration",
    "_is_pandas_dataframe": "create_dataframe_values",
    "_is_polars_dataframe": "create_dataframe_values",
    "_join_table_identifier_segments": "catalog_resolution",
    "_json_input_nonempty": "reader_support",
    "_json_multiline_empty_schema_is_mismatch": "reader_support",
    "_late_catalog_names": "session_state",
    "_localize_naive_timestamp_column": "create_dataframe_arrow",
    "_looks_like_datafusion_conf_key": "session_configuration",
    "_master_warned": "session_state",
    "_match_from_or_join_keyword": "sql_relations",
    "_materialize_arrow_as_memtable_frame": "create_dataframe_rows",
    "_materialize_values_as_memtable_frame": "create_dataframe_rows",
    "_merge_inferred_arrow_types": "create_dataframe_inference",
    "_merge_struct_arrow_types": "create_dataframe_inference",
    "_normalize_create_dataframe_cell": "create_dataframe_values",
    "_normalize_frame_arrow_column": "create_dataframe_arrow",
    "_normalize_nested_sql_type_aliases": "create_dataframe_inference",
    "_null_sql_for_pandas_dtype": "create_dataframe_schema",
    "_null_sql_for_polars_dtype": "create_dataframe_schema",
    "_numpy_datetime64_unit": "create_dataframe_values",
    "_pa_array_or_refuse": "create_dataframe_tuples",
    "_pandas_dtype_needs_object_null_witness": "create_dataframe_schema",
    "_parse_create_dataframe_schema": "create_dataframe_values",
    "_parse_jdbc_int_option": "reader_support",
    "_parse_schema_ddl": "create_dataframe_schema",
    "_parse_simple_sql_udf_call": "sql_udf_parsing",
    "_parse_table_identifier_segments": "sql_relations",
    "_prepare_nested_cell": "create_dataframe_inference",
    "_promote_csv_string_types": "reader_support",
    "_python_scalar_merge_kind": "create_dataframe_tuples",
    "_reader_path_to_str": "reader_support",
    "_refuse_dual_memory_pool_knobs": "session_configuration",
    "_refuse_duplicate_pandas_columns": "create_dataframe_rows",
    "_refuse_duplicate_tuple_column_names": "create_dataframe_tuples",
    "_refuse_incompatible_scalar_merge_kinds": "create_dataframe_tuples",
    "_refuse_list_element_type_merge": "create_dataframe_tuples",
    "_refuse_long_double_merge": "create_dataframe_tuples",
    "_refuse_runtime_memory_limit_gb": "session_configuration",
    "_register_cdf_view_cleanup": "create_dataframe_rows",
    "_reset_active_session_for_tests": "session_state",
    "_reset_dropin_warnings_for_tests": "session_state",
    "_rows_from_mapping_list": "create_dataframe_rows",
    "_rows_from_pandas": "create_dataframe_rows",
    "_rows_from_polars": "create_dataframe_rows",
    "_scan_sql_table_identifier_end": "sql_relations",
    "_schema_fields": "reader_support",
    "_schema_names_and_permutation": "create_dataframe_schema",
    "_skip_sql_ws_and_comments": "sql_relations",
    "_spark_dict_key_union_order": "create_dataframe_rows",
    "_split_leading_sql_trivia": "sql_relations",
    "_split_leading_table_ident": "sql_relations",
    "_split_sql_select_list": "sql_udf_parsing",
    "_split_sql_table_name_list": "sql_relations",
    "_sql_collect_registry_udf_hits": "sql_udf_discovery",
    "_sql_find_registry_udf_calls": "sql_udf_discovery",
    "_sql_literal": "create_dataframe_values",
    "_sql_mask_strings_and_comments": "sql_relations",
    "_sql_materialize_expr_udfs": "sql_udf_materialization",
    "_sql_peel_select_trailing_clauses": "sql_udf_discovery",
    "_sql_plan_order_by_aliases": "sql_udf_materialization",
    "_sql_residual_has_subquery": "sql_udf_discovery",
    "_sql_strip_comments_preserve_strings": "sql_udf_parsing",
    "_sql_table_ref": "sql_relations",
    "_sql_top_level_keyword_index": "sql_udf_parsing",
    "_sql_type_to_arrow": "create_dataframe_inference",
    "_sql_udf_arg_is_simple": "sql_udf_discovery",
    "_sql_udf_call_match_key": "sql_udf_discovery",
    "_sql_udf_clean_exception": "sql_udf_materialization",
    "_sql_udf_in_nested_subquery": "sql_udf_parsing",
    "_sql_udf_public_error_text": "sql_udf_materialization",
    "_sql_where_residual_base_projections": "sql_udf_residual",
    "_supported_array_typecodes": "create_dataframe_values",
    "_sync_display_style_into_builder_config": "catalog_resolution",
    "_to_str": "session_state",
    "_try_rewrite_select_list_python_udfs": "sql_udf_rewrite",
    "_unbounded_batch_warned": "session_state",
    "_update_rest_has_set_clause": "sql_relations",
    "_validate_decimal_column_envelope": "create_dataframe_arrow",
    "_validate_decimal_envelope": "create_dataframe_inference",
    "_values_sql_with_explicit_casts": "create_dataframe_tuples",
    "_values_sql_with_typed_nulls": "create_dataframe_rows",
    "_warn_master_once": "session_state",
    "_warn_unbounded_batch_once": "session_state",
    "logger": "session_configuration",
    "normalize_display_style": "session_configuration",
    "resolve_table_name": "catalog_resolution",
}

EXPECTED_RUNTIME_NAMES = (
    "AnalysisException",
    "Any",
    "Catalog",
    "DEFAULT_CATALOG_NAME",
    "DEFAULT_DATABASE_NAME",
    "DEFAULT_SESSION_TIME_ZONE",
    "DEFAULT_TIMESTAMP_TYPE",
    "DataFrame",
    "IllegalArgumentException",
    "Path",
    "PySparkException",
    "PySparkRuntimeError",
    "PySparkTypeError",
    "PySparkValueError",
    "SESSION_TIME_ZONE_KEY",
    "TIMESTAMP_TYPE_KEY",
    "TYPE_CHECKING",
    "_ARRAY_TYPECODES_SUPPORTED",
    "_AUTO_MEMORY_CATALOG_KEY",
    "_BATCH_SIZE_KEYS",
    "_CONF_GET_UNSET",
    "_CREATE_TABLE_PREFIX_RE",
    "_CREATE_TEMP_TABLE_SQL_RE",
    "_CREATE_VIEW_SQL_RE",
    "_CSV_NATIVE_OPTION_KEYS",
    "_CSV_UNSUPPORTED_PARSE_OPTIONS",
    "_DATAFUSION_CONF_KEY_RE",
    "_DATAFUSION_CONF_PREFIX",
    "_DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY",
    "_DECIMAL_MAX_ABS",
    "_DECIMAL_PRECISION",
    "_DECIMAL_SCALE",
    "_DEFAULT_DISPLAY_STYLE",
    "_DELETE_FROM_PREFIX_RE",
    "_DISPLAY_STYLE_KEY",
    "_DISPLAY_STYLE_VALUES",
    "_DROP_TABLE_SQL_RE",
    "_EXCEL_NATIVE_OPTION_KEYS",
    "_FROM_JOIN_NON_TABLE",
    "_I64_MAX",
    "_I64_MIN",
    "_ICEBERG_TIME_TRAVEL_OPTIONS",
    "_INFER_NESTED_DICT_AS_STRUCT",
    "_INSERT_DIRECTORY_HEAD_RE",
    "_INSERT_PREFIX_RE",
    "_JSON_NATIVE_OPTION_KEYS",
    "_JSON_UNSUPPORTED_PARSE_OPTIONS",
    "_LEGACY_FIRST_ELEMENT_COERCE",
    "_MEMORY_LIMIT_KEYS",
    "_MEMORY_LIMIT_KEY_LOWER",
    "_MERGE_INTO_SQL_RE",
    "_NUMPY_DATETIME64_DATE_UNITS",
    "_RELATION_FOLLOW_KEYWORDS",
    "_SELECT_OR_WITH_HEAD_RE",
    "_SPARK_SCALAR_MERGE_KIND_ORDER",
    "_SPARK_SCALAR_MERGE_LABELS",
    "_SQLCONF_DEFAULTS",
    "_SQLCONF_STATIC_KEYS",
    "_STOPPED_MESSAGE",
    "_TARGET_PARTITIONS_KEYS",
    "_TYPED_NULL_SQL",
    "_UNSUPPORTED_SEMANTIC_READER_OPTIONS",
    "_UPDATE_PREFIX_RE",
    "_active_session",
    "_alias_catalog_name",
    "_apply_builder_datafusion_conf",
    "_apply_permutation",
    "_array_typecodes_supported",
    "_arrow_null_sql_to_type",
    "_arrow_table_from_pandas",
    "_arrow_table_from_polars",
    "_arrow_table_from_raw_tuples",
    "_arrow_table_from_raw_tuples_fast",
    "_arrow_table_from_raw_tuples_legacy",
    "_arrow_table_from_tuples",
    "_arrow_type_for_typed_null_sql",
    "_arrow_type_is_nested",
    "_arrow_type_merge_label",
    "_auto_memory_catalog_wanted",
    "_bind_named_row",
    "_builder_has_memory_limit_key",
    "_catalog_names_from_builder_config",
    "_coerce_schema_names",
    "_collect_cte_names",
    "_column_null_sql_from_raw_tuples",
    "_config_value_error",
    "_create_dataframe_from_rows",
    "_create_dataframe_from_rows_inner",
    "_data_type_to_sql_type",
    "_datetime64_unit_from_dtype",
    "_default_catalog_from_builder_config",
    "_default_namespace_from_builder_config",
    "_drop_cdf_temp_view",
    "_empty_frame_sql",
    "_empty_typed_arrow_frame",
    "_find_matching_paren",
    "_format_datafusion_set_sql",
    "_forward_datafusion_conf",
    "_infer_arrow_type_from_python_sample",
    "_infer_null_sql_from_raw_cells",
    "_infer_struct_arrow_from_dict_samples",
    "_is_datafusion_conf_key",
    "_is_pandas_dataframe",
    "_is_plain_ident",
    "_is_polars_dataframe",
    "_join_table_identifier_segments",
    "_json_input_nonempty",
    "_json_multiline_empty_schema_is_mismatch",
    "_late_catalog_names",
    "_localize_naive_timestamp_column",
    "_looks_like_datafusion_conf_key",
    "_master_warned",
    "_match_from_or_join_keyword",
    "_materialize_arrow_as_memtable_frame",
    "_materialize_values_as_memtable_frame",
    "_merge_inferred_arrow_types",
    "_merge_struct_arrow_types",
    "_native",
    "_normalize_create_dataframe_cell",
    "_normalize_frame_arrow_column",
    "_normalize_nested_sql_type_aliases",
    "_null_sql_for_pandas_dtype",
    "_null_sql_for_polars_dtype",
    "_numpy_datetime64_unit",
    "_pa_array_or_refuse",
    "_pandas_dtype_needs_object_null_witness",
    "_parse_create_dataframe_schema",
    "_parse_jdbc_int_option",
    "_parse_schema_ddl",
    "_parse_simple_sql_udf_call",
    "_parse_table_identifier_segments",
    "_prepare_nested_cell",
    "_promote_csv_string_types",
    "_prop_key_is_secret",
    "_python_scalar_merge_kind",
    "_quote_ident",
    "_quote_ident_if_needed",
    "_reader_path_to_str",
    "_refuse_dual_memory_pool_knobs",
    "_refuse_duplicate_pandas_columns",
    "_refuse_duplicate_tuple_column_names",
    "_refuse_incompatible_scalar_merge_kinds",
    "_refuse_list_element_type_merge",
    "_refuse_long_double_merge",
    "_refuse_runtime_memory_limit_gb",
    "_register_cdf_view_cleanup",
    "_reject_path_escape_segment",
    "_reset_active_session_for_tests",
    "_reset_dropin_warnings_for_tests",
    "_rows_from_mapping_list",
    "_rows_from_pandas",
    "_rows_from_polars",
    "_scan_sql_table_identifier_end",
    "_schema_fields",
    "_schema_names_and_permutation",
    "_skip_sql_ws_and_comments",
    "_spark_dict_key_union_order",
    "_split_leading_sql_trivia",
    "_split_leading_table_ident",
    "_split_sql_select_list",
    "_split_sql_table_name_list",
    "_sql_collect_registry_udf_hits",
    "_sql_find_registry_udf_calls",
    "_sql_literal",
    "_sql_mask_strings_and_comments",
    "_sql_materialize_expr_udfs",
    "_sql_peel_select_trailing_clauses",
    "_sql_plan_order_by_aliases",
    "_sql_residual_has_subquery",
    "_sql_strip_comments_preserve_strings",
    "_sql_table_ref",
    "_sql_top_level_keyword_index",
    "_sql_type_to_arrow",
    "_sql_udf_arg_is_simple",
    "_sql_udf_call_match_key",
    "_sql_udf_clean_exception",
    "_sql_udf_in_nested_subquery",
    "_sql_udf_public_error_text",
    "_sql_where_residual_base_projections",
    "_supported_array_typecodes",
    "_sync_display_style_into_builder_config",
    "_to_str",
    "_try_rewrite_select_list_python_udfs",
    "_unbounded_batch_warned",
    "_update_rest_has_set_clause",
    "_validate_decimal_column_envelope",
    "_validate_decimal_envelope",
    "_values_sql_with_explicit_casts",
    "_values_sql_with_typed_nulls",
    "_warn_master_once",
    "_warn_unbounded_batch_once",
    "annotations",
    "contextlib",
    "contextvars",
    "logger",
    "logging",
    "normalize_display_style",
    "re",
    "resolve_table_name",
    "scratch_view_name",
    "sql_string_literal",
    "uuid",
    "warnings",
)

WIRED_RUNTIME_NAMES = frozenset(
    {
        "DataFrameNaFunctions",
        "DataFrameReader",
        "DataFrameStatFunctions",
        "DataFrameWriter",
        "DataFrameWriterV2",
        "GroupedData",
        "ReParkSession",
        "ReparkSession",
        "RuntimeConfig",
        "SparkContext",
        "SparkSession",
        "UDFRegistration",
    }
)


def _top_level_symbol_hashes() -> dict[str, str]:
    """Return normalized AST hashes for extracted top-level symbols."""
    measured: dict[str, str] = {}
    for filename in MODULE_FILES[1:]:
        tree = ast.parse((SESSION / filename).read_text())
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                names = [node.name]
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                names = [target.id for target in targets if isinstance(target, ast.Name)]
            else:
                continue
            digest = hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
            for name in names:
                measured[name] = digest
    return measured


def _top_level_symbol_owners() -> dict[str, str]:
    """Return each extracted symbol's responsibility module."""
    measured: dict[str, str] = {}
    for filename in MODULE_FILES[1:]:
        tree = ast.parse((SESSION / filename).read_text())
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                names = [node.name]
            elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                names = [target.id for target in targets if isinstance(target, ast.Name)]
            else:
                continue
            for name in names:
                if name in EXPECTED_OWNERS:
                    measured[name] = Path(filename).stem
    return measured


def _loaded_global_names(code: types.CodeType) -> set[str]:
    """Return global names loaded by a code object and its nested expressions."""
    names = {
        instruction.argval
        for instruction in dis.get_instructions(code)
        if instruction.opname in {"LOAD_GLOBAL", "LOAD_FROM_DICT_OR_GLOBALS"}
        and isinstance(instruction.argval, str)
    }
    for constant in code.co_consts:
        if isinstance(constant, types.CodeType):
            names.update(_loaded_global_names(constant))
    return names


def test_all_parent_symbols_remain_on_compatibility_namespace() -> None:
    """The router keeps every source-bound parent name importable."""
    missing = [name for name in EXPECTED_RUNTIME_NAMES if not hasattr(_funcs, name)]
    assert missing == []


def test_star_import_keeps_the_parent_public_namespace_exact() -> None:
    """Router-only imports do not leak through the star export."""
    expected = {name for name in EXPECTED_RUNTIME_NAMES if not name.startswith("_")}
    expected.update(WIRED_RUNTIME_NAMES)
    measured = {name for name in dir(_funcs) if not name.startswith("_")}
    assert measured == expected


def test_tree_consumers_keep_their_private_import_contract() -> None:
    """The two private names imported explicitly by tree consumers remain stable."""
    assert hasattr(_funcs, "_active_session")
    assert callable(_funcs._register_cdf_view_cleanup)


def test_current_main_literal_helper_paths_remain_importable() -> None:
    """The session package and compatibility router share the same public helper."""
    import repark.spark.session as session_package

    assert _funcs.sql_string_literal is session_package.sql_string_literal


def test_moved_symbol_bodies_match_the_integrated_baseline() -> None:
    """Moved bodies match the frozen parent plus current-main behavior changes."""
    measured = _top_level_symbol_hashes()
    assert {name: measured[name] for name in EXPECTED_SYMBOL_HASHES} == EXPECTED_SYMBOL_HASHES


def test_symbols_keep_their_responsibility_owner_and_router_identity() -> None:
    """Each moved binding comes from its declared responsibility module."""
    assert _top_level_symbol_owners() == EXPECTED_OWNERS
    for name, owner in EXPECTED_OWNERS.items():
        value = getattr(_funcs, name)
        owner_module = sys.modules[f"repark.spark.session.{owner}"]
        assert value is getattr(owner_module, name)
        if callable(value):
            assert value.__module__ == f"repark.spark.session.{owner}"


def test_lazy_mutable_cache_stays_coherent_through_compatibility_modules() -> None:
    """Legacy reads and writes track the cache owner (pins: production-file-size/C-010)."""
    import repark.spark.session as session_package

    owner = sys.modules["repark.spark.session.create_dataframe_values"]
    original = owner._ARRAY_TYPECODES_SUPPORTED
    try:
        owner._ARRAY_TYPECODES_SUPPORTED = None
        assert _funcs._ARRAY_TYPECODES_SUPPORTED is None
        assert session_package._ARRAY_TYPECODES_SUPPORTED is None
        marker = frozenset({"critic"})
        _funcs._ARRAY_TYPECODES_SUPPORTED = marker
        assert owner._ARRAY_TYPECODES_SUPPORTED is marker
        assert session_package._ARRAY_TYPECODES_SUPPORTED is marker
    finally:
        owner._ARRAY_TYPECODES_SUPPORTED = original


def test_cross_owner_globals_resolve_to_their_canonical_binding() -> None:
    """Moved functions resolve every peer binding from its declared owner."""
    required_bindings: set[tuple[str, str, str]] = set()
    for name, owner in EXPECTED_OWNERS.items():
        value = getattr(_funcs, name)
        if not isinstance(value, types.FunctionType):
            continue
        for global_name in _loaded_global_names(value.__code__):
            canonical_owner = EXPECTED_OWNERS.get(global_name)
            if canonical_owner is None or canonical_owner == owner:
                continue
            required_bindings.add((owner, global_name, canonical_owner))
            canonical_module = sys.modules[f"repark.spark.session.{canonical_owner}"]
            assert global_name in value.__globals__
            assert value.__globals__[global_name] is getattr(canonical_module, global_name)
    assert len(required_bindings) == 76


def test_split_files_stay_within_default_source_ceiling() -> None:
    """The compatibility router and every extracted module stay at 1,000 lines or less."""
    counts = {name: len((SESSION / name).read_text().splitlines()) for name in MODULE_FILES}
    assert max(counts.values()) <= 1_000


def test_cap_1_funcs_source_size_exception_is_retired() -> None:
    """The Python source-size exceptions table no longer names the router."""
    guard = ast.parse((ROOT / "scripts/check_lib_py.py").read_text())
    exception_keys: set[str] = set()
    for node in guard.body:
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
            continue
        if node.target.id != "EXCEPTIONS" or not isinstance(node.value, ast.Dict):
            continue
        exception_keys = {key.value for key in node.value.keys if isinstance(key, ast.Constant)}
    assert "python/repark/src/repark/spark/session/_funcs.py" not in exception_keys


def test_session_package_imports_without_a_cycle() -> None:
    """A fresh interpreter imports the assembled compatibility surface."""
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-c", "import repark.spark.session._funcs"],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
