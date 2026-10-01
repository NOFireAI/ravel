use super::*;

#[test]
fn mapping_round_trips_through_toml() {
    let m = parse_mapping(
        r#"
ts_column = "timestamp"
ts_unit = "micros"
body_column = "msg"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "code"
column = "status"
type = "i64"
"#,
    )
    .expect("valid mapping");
    assert_eq!(m.ts_column, "timestamp");
    assert_eq!(m.ts_unit, TsUnit::Micros);
    assert_eq!(m.body_column.as_deref(), Some("msg"));
    assert_eq!(m.resource_attributes.len(), 1);
    assert_eq!(m.resource_attributes[0].value_type, ColType::Str);
    assert_eq!(m.attributes.len(), 1);
    assert_eq!(m.attributes[0].value_type, ColType::I64);
}

#[test]
fn unknown_mapping_field_is_rejected() {
    let err = parse_mapping("ts_column = \"t\"\nts_unit = \"nanos\"\nbogus = 1\n")
        .expect_err("deny_unknown_fields rejects a typo");
    assert!(matches!(err, LoadError::Setup(_)));
}
