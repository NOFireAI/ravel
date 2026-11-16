//! A corpus of the released ES|QL syntax: every command and form this parser
//! admits, each with its tree shape and spans checked, printed and parsed
//! back; malformed cases for each with their exact error, line and column;
//! and the commands that stay refused.

#![allow(clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;

use ravel_esql::ast::{
    BinaryOp, CallArgs, ChangePointCommand, Command, CompletionCommand, DissectCommand,
    EnrichCommand, Expr, ForkCommand, FromSource, FuseCommand, FuseMethod, GrokCommand, InList,
    Literal, LiteralValue, LookupJoinCommand, MapLiteral, ParamName, PromqlCommand, PromqlValue,
    QualifiedName, Query, RerankCommand, Setting, StatsCommand, Unary, UnaryOp,
};
use ravel_esql::{Error, Limits, LineCol, Span, parse, print};

/// What the refusal of a comparison-level operand adds after the followers.
const NO_CHAIN: &str = " (a comparison, `:`, `IN`, `LIKE`, `RLIKE` or `IS NULL` does not \
     chain and is not an operand of arithmetic or `::` unless parenthesised)";
const SOURCE_EXPECTED: &str = "`SET` or a source command (`FROM`, `ROW`, `TS`, `SHOW` or `PROMQL`)";
const NESTED_SOURCE_EXPECTED: &str = "`FROM`, which starts a subquery";
const PROMQL_EXPECTED: &str = "a `name = value` parameter or the parenthesised PromQL query";
const SAMPLE_EXPECTED: &str = "a decimal probability such as `0.25`";
const IN_ELEMENT_PREDICATE: &str = "`,` or `)` (an `IN` list element is a value or one \
     comparison; parenthesise an `IN`, `LIKE`, `RLIKE`, `IS NULL` or `:` there)";

/// The commands and forms the corpus covers; each needs at least three
/// admitted cases and two malformed ones.
const FORMS: &[&str] = &[
    "SET",
    "SHOW INFO",
    "TS",
    "FROM subquery",
    "IN subquery",
    "PROMQL",
    "DISSECT",
    "GROK",
    "MV_EXPAND",
    "LOOKUP JOIN",
    "ENRICH",
    "INLINE STATS",
    "FORK",
    "FUSE",
    "CHANGE_POINT",
    "SAMPLE",
    "COMPLETION",
    "RERANK",
    "LIMIT BY",
    "RENAME",
    "option map",
    "parameter",
];

type Check = fn(&Query, &str);

fn parsed(source: &str) -> Query {
    parse(source, &Limits::default()).unwrap_or_else(|error| {
        panic!(
            "{source:?} refused at {:?}: {error}",
            error.line_col(source)
        )
    })
}

/// Parses `source`, prints the tree and parses the printed text, which must
/// give the same tree ignoring spans.
fn round_trip(source: &str) -> Query {
    let query = parsed(source);
    let printed = print(&query, &Limits::default())
        .unwrap_or_else(|error| panic!("{source:?} does not print: {error}"));
    let reparsed = parse(&printed, &Limits::default())
        .unwrap_or_else(|error| panic!("{source:?} printed as {printed:?}, refused: {error}"));
    assert!(
        query.eq_ignoring_spans(&reparsed),
        "{source:?} printed as {printed:?}\nparsed:   {query:#?}\nreparsed: {reparsed:#?}"
    );
    query
}

fn text(source: &str, span: Span) -> &str {
    source
        .get(span.start..span.end)
        .unwrap_or_else(|| panic!("{span:?} is outside {source:?}"))
}

fn last(query: &Query) -> &Command {
    query.processing.last().unwrap_or(&query.source)
}

fn dotted(name: &QualifiedName) -> String {
    name.parts
        .iter()
        .map(|part| part.text.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn names(names: &[QualifiedName]) -> Vec<String> {
    names.iter().map(dotted).collect()
}

fn keys(map: &MapLiteral) -> Vec<&str> {
    map.entries.iter().map(|e| e.key.as_str()).collect()
}

fn field_name(expr: &Expr) -> String {
    match expr {
        Expr::Field(name) => dotted(name),
        other => panic!("not a field: {other:?}"),
    }
}

fn string_value(expr: &Expr) -> &str {
    match expr {
        Expr::Literal(Literal {
            value: LiteralValue::String(value),
            ..
        }) => value,
        other => panic!("not a string literal: {other:?}"),
    }
}

fn param_name(expr: &Expr) -> (&ParamName, bool) {
    match expr {
        Expr::Param(param) => (&param.name, param.identifier),
        other => panic!("not a parameter: {other:?}"),
    }
}

fn named(name: &str) -> ParamName {
    ParamName::Named(name.to_owned())
}

/// The single setting of `query`'s `index`th directive.
fn setting(query: &Query, index: usize) -> &Setting {
    query
        .settings
        .get(index)
        .unwrap_or_else(|| panic!("no setting {index} in {query:#?}"))
}

fn where_condition(query: &Query) -> &Expr {
    match last(query) {
        Command::Where(w) => &w.condition,
        other => panic!("not WHERE: {other:?}"),
    }
}

fn stats(command: &Command) -> &StatsCommand {
    match command {
        Command::InlineStats(s) => s,
        other => panic!("not INLINE STATS: {other:?}"),
    }
}

fn promql(query: &Query) -> &PromqlCommand {
    match &query.source {
        Command::Promql(c) => c,
        other => panic!("not PROMQL: {other:?}"),
    }
}

fn dissect(query: &Query) -> &DissectCommand {
    match last(query) {
        Command::Dissect(c) => c,
        other => panic!("not DISSECT: {other:?}"),
    }
}

fn grok(query: &Query) -> &GrokCommand {
    match last(query) {
        Command::Grok(c) => c,
        other => panic!("not GROK: {other:?}"),
    }
}

fn lookup_join(query: &Query) -> &LookupJoinCommand {
    match last(query) {
        Command::LookupJoin(c) => c,
        other => panic!("not LOOKUP JOIN: {other:?}"),
    }
}

fn enrich(query: &Query) -> &EnrichCommand {
    match last(query) {
        Command::Enrich(c) => c,
        other => panic!("not ENRICH: {other:?}"),
    }
}

fn fork(command: &Command) -> &ForkCommand {
    match command {
        Command::Fork(c) => c,
        other => panic!("not FORK: {other:?}"),
    }
}

fn fuse(query: &Query) -> &FuseCommand {
    match last(query) {
        Command::Fuse(c) => c,
        other => panic!("not FUSE: {other:?}"),
    }
}

fn change_point(query: &Query) -> &ChangePointCommand {
    match last(query) {
        Command::ChangePoint(c) => c,
        other => panic!("not CHANGE_POINT: {other:?}"),
    }
}

fn completion(query: &Query) -> &CompletionCommand {
    match last(query) {
        Command::Completion(c) => c,
        other => panic!("not COMPLETION: {other:?}"),
    }
}

fn rerank(query: &Query) -> &RerankCommand {
    match last(query) {
        Command::Rerank(c) => c,
        other => panic!("not RERANK: {other:?}"),
    }
}

fn call_map(expr: &Expr) -> &MapLiteral {
    match expr {
        Expr::Call(call) => match &call.args {
            CallArgs::List(args) => match args.last() {
                Some(Expr::Map(map)) => map,
                other => panic!("last argument is not a map: {other:?}"),
            },
            CallArgs::Star => panic!("`*` arguments"),
        },
        other => panic!("not a call: {other:?}"),
    }
}

fn admitted() -> Vec<(&'static str, &'static str, Check)> {
    vec![
        // ---- SET ----
        ("SET", "SET a = 1; ROW x = 1", |q, s| {
            assert_eq!(q.settings.len(), 1);
            let set = setting(q, 0);
            assert_eq!(set.name.text, "a");
            assert!(matches!(
                &set.value,
                Expr::Literal(Literal { value: LiteralValue::Integer(v), .. }) if v == "1"
            ));
            assert_eq!(text(s, set.span), "SET a = 1;");
            assert_eq!(text(s, q.span), "ROW x = 1");
        }),
        (
            "SET",
            "SET project_routing = \"_alias:*\";\nset approximate = true;\nFROM logs",
            |q, s| {
                assert_eq!(q.settings.len(), 2);
                assert_eq!(setting(q, 0).name.text, "project_routing");
                assert_eq!(string_value(&setting(q, 0).value), "_alias:*");
                assert_eq!(setting(q, 1).name.text, "approximate");
                assert!(matches!(
                    &setting(q, 1).value,
                    Expr::Literal(Literal {
                        value: LiteralValue::Boolean(true),
                        ..
                    })
                ));
                assert_eq!(text(s, setting(q, 1).span), "set approximate = true;");
                assert!(matches!(q.source, Command::From(_)));
            },
        ),
        (
            "SET",
            "SET opts = {\"k\": 1, \"nested\": {\"a\": [1, 2]}}; SET p = ?v; SET n = -1.5; ROW x = 1",
            |q, s| {
                let Expr::Map(map) = &setting(q, 0).value else {
                    panic!("not a map: {q:#?}");
                };
                assert_eq!(keys(map), ["k", "nested"]);
                assert!(matches!(&map.entries[1].value, Expr::Map(inner) if keys(inner) == ["a"]));
                assert_eq!(param_name(&setting(q, 1).value), (&named("v"), false));
                assert!(matches!(
                    &setting(q, 2).value,
                    Expr::Unary(Unary {
                        op: UnaryOp::Minus,
                        ..
                    })
                ));
                assert_eq!(text(s, setting(q, 2).span), "SET n = -1.5;");
            },
        ),
        // ---- SHOW INFO ----
        ("SHOW INFO", "SHOW INFO", |q, s| {
            assert!(matches!(q.source, Command::Show(_)));
            assert!(q.processing.is_empty());
            assert_eq!(text(s, q.source.span()), "SHOW INFO");
        }),
        ("SHOW INFO", "show\n  Info", |q, s| {
            assert!(matches!(q.source, Command::Show(_)));
            assert_eq!(text(s, q.source.span()), "show\n  Info");
        }),
        ("SHOW INFO", "SHOW INFO | KEEP version", |q, _| {
            assert!(matches!(q.source, Command::Show(_)));
            assert!(matches!(q.processing.as_slice(), [Command::Keep(_)]));
        }),
        // ---- TS ----
        ("TS", "TS metrics", |q, s| {
            let Command::Ts(ts) = &q.source else {
                panic!("not TS: {q:#?}");
            };
            assert_eq!(ts.sources.len(), 1);
            assert_eq!(ts.sources[0].text, "metrics");
            assert!(ts.metadata.is_none());
            assert_eq!(text(s, ts.span), "TS metrics");
        }),
        (
            "TS",
            "TS metrics-*, \"pods\" METADATA _index, _id",
            |q, s| {
                let Command::Ts(ts) = &q.source else {
                    panic!("not TS: {q:#?}");
                };
                let sources: Vec<_> = ts
                    .sources
                    .iter()
                    .map(|p| (p.text.as_str(), p.quoted))
                    .collect();
                assert_eq!(sources, [("metrics-*", false), ("pods", true)]);
                let metadata = ts.metadata.as_ref().expect("metadata");
                assert_eq!(names(&metadata.fields), ["_index", "_id"]);
                assert_eq!(text(s, ts.span), s);
            },
        ),
        (
            "TS",
            "TS pods | STATS max(rate(bytes)) BY TBUCKET(1 hour)",
            |q, _| {
                assert!(matches!(q.source, Command::Ts(_)));
                assert!(matches!(q.processing.as_slice(), [Command::Stats(_)]));
            },
        ),
        // ---- FROM with subqueries ----
        ("FROM subquery", "FROM a, (FROM b | WHERE x > 1)", |q, s| {
            let Command::FromQueries(from) = &q.source else {
                panic!("not FROM with subqueries: {q:#?}");
            };
            assert_eq!(from.sources.len(), 2);
            assert!(matches!(&from.sources[0], FromSource::Pattern(p) if p.text == "a"));
            let FromSource::Query(sub) = &from.sources[1] else {
                panic!("not a subquery: {from:#?}");
            };
            assert!(matches!(&sub.query.source, Command::From(f) if f.sources[0].text == "b"));
            assert!(matches!(
                sub.query.processing.as_slice(),
                [Command::Where(_)]
            ));
            assert_eq!(text(s, sub.span), "(FROM b | WHERE x > 1)");
            assert_eq!(text(s, sub.query.span), "FROM b | WHERE x > 1");
            assert_eq!(text(s, from.span), s);
        }),
        (
            "FROM subquery",
            "FROM a, (FROM b, (FROM c | KEEP d)), e* | LIMIT 5",
            |q, s| {
                let Command::FromQueries(from) = &q.source else {
                    panic!("not FROM with subqueries: {q:#?}");
                };
                assert_eq!(from.sources.len(), 3);
                let FromSource::Query(sub) = &from.sources[1] else {
                    panic!("not a subquery: {from:#?}");
                };
                let Command::FromQueries(inner) = &sub.query.source else {
                    panic!("inner is not FROM with subqueries: {sub:#?}");
                };
                assert_eq!(inner.sources.len(), 2);
                assert!(matches!(&from.sources[2], FromSource::Pattern(p) if p.text == "e*"));
                assert_eq!(
                    text(s, from.span),
                    "FROM a, (FROM b, (FROM c | KEEP d)), e*"
                );
                assert!(matches!(q.processing.as_slice(), [Command::Limit(_)]));
            },
        ),
        (
            "FROM subquery",
            "FROM logs-*, (FROM metrics METADATA _id | STATS n = COUNT(*) BY host) | SORT n DESC",
            |q, _| {
                let Command::FromQueries(from) = &q.source else {
                    panic!("not FROM with subqueries: {q:#?}");
                };
                let FromSource::Query(sub) = &from.sources[1] else {
                    panic!("not a subquery: {from:#?}");
                };
                let Command::From(inner) = &sub.query.source else {
                    panic!("inner is not FROM: {sub:#?}");
                };
                assert_eq!(
                    names(&inner.metadata.as_ref().expect("metadata").fields),
                    ["_id"]
                );
                assert!(sub.query.settings.is_empty());
            },
        ),
        // ---- IN with a subquery ----
        (
            "IN subquery",
            "FROM t | WHERE a IN (FROM u | KEEP a)",
            |q, s| {
                let Expr::InSubquery(test) = where_condition(q) else {
                    panic!("not IN with a subquery: {q:#?}");
                };
                assert!(!test.negated);
                assert_eq!(field_name(&test.expr), "a");
                assert!(
                    matches!(&test.query.query.source, Command::From(f) if f.sources[0].text == "u")
                );
                assert_eq!(text(s, test.span), "a IN (FROM u | KEEP a)");
                assert_eq!(text(s, test.query.span), "(FROM u | KEEP a)");
            },
        ),
        (
            "IN subquery",
            "FROM t | WHERE a NOT IN (FROM u | WHERE b == 1 | KEEP a)",
            |q, _| {
                let Expr::InSubquery(test) = where_condition(q) else {
                    panic!("not IN with a subquery: {q:#?}");
                };
                assert!(test.negated);
                assert_eq!(test.query.query.processing.len(), 2);
            },
        ),
        (
            "IN subquery",
            "FROM t | WHERE x > 0 AND host IN (FROM hosts, (FROM more) | KEEP host)",
            |q, _| {
                let Expr::Binary(and) = where_condition(q) else {
                    panic!("not AND: {q:#?}");
                };
                assert_eq!(and.op, BinaryOp::And);
                let Expr::InSubquery(test) = and.right.as_ref() else {
                    panic!("right is not IN with a subquery: {and:#?}");
                };
                assert!(matches!(test.query.query.source, Command::FromQueries(_)));
            },
        ),
        // ---- PROMQL ----
        (
            "PROMQL",
            "PROMQL (rate(http_requests_total[5m]))",
            |q, s| {
                let c = promql(q);
                assert!(c.params.is_empty());
                assert_eq!(c.query.text, "rate(http_requests_total[5m])");
                assert_eq!(text(s, c.query.span), "rate(http_requests_total[5m])");
                assert!(c.query.error.is_none(), "{c:#?}");
                assert_eq!(text(s, c.span), s);
            },
        ),
        (
            "PROMQL",
            "PROMQL index = metrics-* step = 1m (sum by (job) (up))",
            |q, s| {
                let c = promql(q);
                let params: Vec<_> = c
                    .params
                    .iter()
                    .map(|p| match &p.value {
                        PromqlValue::Text(t) => (p.name.as_str(), t.text.as_str()),
                        PromqlValue::Param(_) => panic!("parameter value: {p:#?}"),
                    })
                    .collect();
                assert_eq!(params, [("index", "metrics-*"), ("step", "1m")]);
                assert_eq!(text(s, c.params[1].span), "step = 1m");
                assert_eq!(c.query.text, "sum by (job) (up)");
                assert!(c.query.error.is_none(), "{c:#?}");
            },
        ),
        (
            "PROMQL",
            "PROMQL step = ?step (up) | WHERE value > 0",
            |q, _| {
                let c = promql(q);
                assert!(matches!(
                    &c.params[0].value,
                    PromqlValue::Param(p) if p.name == named("step") && !p.identifier
                ));
                assert!(matches!(q.processing.as_slice(), [Command::Where(_)]));
            },
        ),
        ("PROMQL", "PROMQL (1 +)", |q, s| {
            let c = promql(q);
            let error = c
                .query
                .error
                .as_ref()
                .expect("the PromQL parser refuses `1 +`");
            assert!(!error.message.is_empty());
            assert_eq!(text(s, error.span), "1 +");
        }),
        // ---- DISSECT ----
        (
            "DISSECT",
            "FROM t | DISSECT message \"%{date} %{msg}\"",
            |q, s| {
                let c = dissect(q);
                assert_eq!(field_name(&c.input), "message");
                assert_eq!(c.pattern, "%{date} %{msg}");
                assert!(c.append_separator.is_none());
                assert_eq!(text(s, c.span), "DISSECT message \"%{date} %{msg}\"");
            },
        ),
        (
            "DISSECT",
            "FROM t | DISSECT a.b \"%{x},%{y}\" APPEND_SEPARATOR = \", \"",
            |q, s| {
                let c = dissect(q);
                assert_eq!(field_name(&c.input), "a.b");
                assert_eq!(c.append_separator.as_deref(), Some(", "));
                assert_eq!(
                    text(s, c.span),
                    "DISSECT a.b \"%{x},%{y}\" APPEND_SEPARATOR = \", \""
                );
            },
        ),
        ("DISSECT", "FROM t | DISSECT ?input \"%{a}\"", |q, _| {
            assert_eq!(param_name(&dissect(q).input), (&named("input"), false));
        }),
        (
            "DISSECT",
            "FROM t | DISSECT TO_STRING(raw) \"%{a}\"",
            |q, _| {
                assert!(matches!(&dissect(q).input, Expr::Call(c) if c.name == "TO_STRING"));
            },
        ),
        // ---- GROK ----
        (
            "GROK",
            "FROM t | GROK message \"%{IP:ip} %{WORD:verb}\"",
            |q, s| {
                let c = grok(q);
                assert_eq!(field_name(&c.input), "message");
                assert_eq!(c.pattern, "%{IP:ip} %{WORD:verb}");
                assert_eq!(text(s, c.span), "GROK message \"%{IP:ip} %{WORD:verb}\"");
            },
        ),
        ("GROK", "FROM t | GROK a.b \"%{NUMBER:n:int}\"", |q, _| {
            let c = grok(q);
            assert_eq!(field_name(&c.input), "a.b");
            assert_eq!(c.pattern, "%{NUMBER:n:int}");
        }),
        (
            "GROK",
            "FROM t | GROK CONCAT(a, \" \", b) \"%{WORD:w}\"",
            |q, _| {
                assert!(matches!(&grok(q).input, Expr::Call(c) if c.name == "CONCAT"));
            },
        ),
        // ---- MV_EXPAND ----
        ("MV_EXPAND", "FROM t | MV_EXPAND tags", |q, s| {
            let Command::MvExpand(c) = last(q) else {
                panic!("not MV_EXPAND: {q:#?}");
            };
            assert_eq!(dotted(&c.field), "tags");
            assert_eq!(text(s, c.span), "MV_EXPAND tags");
        }),
        ("MV_EXPAND", "FROM t | MV_EXPAND host.ips", |q, _| {
            let Command::MvExpand(c) = last(q) else {
                panic!("not MV_EXPAND: {q:#?}");
            };
            assert_eq!(dotted(&c.field), "host.ips");
        }),
        ("MV_EXPAND", "FROM t | MV_EXPAND ??f", |q, _| {
            let Command::MvExpand(c) = last(q) else {
                panic!("not MV_EXPAND: {q:#?}");
            };
            let param = c.field.parts[0].param.as_ref().expect("a parameter");
            assert_eq!((&param.name, param.identifier), (&named("f"), true));
        }),
        ("MV_EXPAND", "FROM t | MV_EXPAND `a b`", |q, _| {
            let Command::MvExpand(c) = last(q) else {
                panic!("not MV_EXPAND: {q:#?}");
            };
            assert!(c.field.parts[0].quoted);
            assert_eq!(dotted(&c.field), "a b");
        }),
        // ---- LOOKUP JOIN ----
        (
            "LOOKUP JOIN",
            "FROM t | LOOKUP JOIN hosts ON host",
            |q, s| {
                let c = lookup_join(q);
                assert_eq!((c.index.text.as_str(), c.index.quoted), ("hosts", false));
                assert_eq!(names(&c.on), ["host"]);
                assert_eq!(text(s, c.span), "LOOKUP JOIN hosts ON host");
            },
        ),
        (
            "LOOKUP JOIN",
            "FROM t | LOOKUP JOIN \"threat-list\" ON source.ip, port",
            |q, _| {
                let c = lookup_join(q);
                assert_eq!(
                    (c.index.text.as_str(), c.index.quoted),
                    ("threat-list", true)
                );
                assert_eq!(names(&c.on), ["source.ip", "port"]);
            },
        ),
        (
            "LOOKUP JOIN",
            "FROM t\n| lookup join hosts-lookup ON ?key",
            |q, s| {
                let c = lookup_join(q);
                assert_eq!(c.index.text, "hosts-lookup");
                assert_eq!(names(&c.on), ["?key"]);
                assert_eq!(text(s, c.span), "lookup join hosts-lookup ON ?key");
            },
        ),
        // ---- ENRICH ----
        ("ENRICH", "FROM t | ENRICH regions_policy", |q, s| {
            let c = enrich(q);
            assert_eq!(c.policy.text, "regions_policy");
            assert!(c.on.is_none());
            assert!(c.with.is_empty());
            assert_eq!(text(s, c.span), "ENRICH regions_policy");
        }),
        (
            "ENRICH",
            "FROM t | ENRICH regions_policy ON region_code",
            |q, s| {
                let c = enrich(q);
                assert_eq!(c.on.as_ref().map(dotted).as_deref(), Some("region_code"));
                assert_eq!(text(s, c.span), "ENRICH regions_policy ON region_code");
            },
        ),
        (
            "ENRICH",
            "FROM t | ENRICH _any:hosts ON ip WITH name, geo = location",
            |q, s| {
                let c = enrich(q);
                assert_eq!(c.policy.text, "_any:hosts");
                let with: Vec<_> = c
                    .with
                    .iter()
                    .map(|f| (f.name.as_ref().map(dotted), dotted(&f.field)))
                    .collect();
                assert_eq!(
                    with,
                    [
                        (None, "name".to_owned()),
                        (Some("geo".to_owned()), "location".to_owned())
                    ]
                );
                assert_eq!(text(s, c.with[1].span), "geo = location");
            },
        ),
        // ---- INLINE STATS ----
        (
            "INLINE STATS",
            "FROM t | INLINE STATS m = MAX(x) BY host",
            |q, s| {
                let c = stats(last(q));
                assert_eq!(c.aggregates.len(), 1);
                assert_eq!(c.groupings.len(), 1);
                assert_eq!(text(s, c.span), "INLINE STATS m = MAX(x) BY host");
            },
        ),
        (
            "INLINE STATS",
            "FROM t | INLINESTATS c = COUNT(*)",
            |q, s| {
                let c = stats(last(q));
                assert_eq!(c.aggregates.len(), 1);
                assert!(c.groupings.is_empty());
                assert_eq!(text(s, c.span), "INLINESTATS c = COUNT(*)");
            },
        ),
        (
            "INLINE STATS",
            "FROM t | inline stats avg(x) WHERE y > 1, n = COUNT(*) BY z, w",
            |q, _| {
                let c = stats(last(q));
                assert!(c.aggregates[0].filter.is_some());
                assert!(c.aggregates[1].filter.is_none());
                assert_eq!(c.groupings.len(), 2);
            },
        ),
        // ---- FORK ----
        (
            "FORK",
            "FROM t | FORK (WHERE a > 1) (WHERE a <= 1 | LIMIT 5)",
            |q, s| {
                let c = fork(last(q));
                assert_eq!(c.branches.len(), 2);
                assert!(matches!(
                    c.branches[0].commands.as_slice(),
                    [Command::Where(_)]
                ));
                assert!(matches!(
                    c.branches[1].commands.as_slice(),
                    [Command::Where(_), Command::Limit(_)]
                ));
                assert_eq!(text(s, c.branches[0].span), "(WHERE a > 1)");
                assert_eq!(
                    text(s, c.span),
                    "FORK (WHERE a > 1) (WHERE a <= 1 | LIMIT 5)"
                );
            },
        ),
        (
            "FORK",
            "FROM t | FORK (WHERE MATCH(title, \"x\") | SORT _score DESC | LIMIT 10) \
             (WHERE MATCH(body, \"x\")) (EVAL k = 1) | FUSE",
            |q, _| {
                assert_eq!(q.processing.len(), 2);
                let c = fork(&q.processing[0]);
                assert_eq!(c.branches.len(), 3);
                assert_eq!(c.branches[0].commands.len(), 3);
                assert!(matches!(q.processing[1], Command::Fuse(_)));
            },
        ),
        (
            "FORK",
            "FROM t\n| FORK\n  (STATS c = COUNT(*))\n  (KEEP a, b | DROP c)",
            |q, s| {
                let c = fork(last(q));
                assert!(matches!(
                    c.branches[1].commands.as_slice(),
                    [Command::Keep(_), Command::Drop(_)]
                ));
                assert_eq!(text(s, c.branches[1].span), "(KEEP a, b | DROP c)");
            },
        ),
        // ---- FUSE ----
        (
            "FUSE",
            "FROM t | FORK (LIMIT 1) (LIMIT 2) | FUSE",
            |q, s| {
                let c = fuse(q);
                assert!(c.method.is_none());
                assert!(c.score_by.is_none() && c.key_by.is_empty() && c.group_by.is_none());
                assert!(c.options.is_none());
                assert_eq!(text(s, c.span), "FUSE");
            },
        ),
        (
            "FUSE",
            "FROM t | FUSE LINEAR SCORE BY _score KEY BY _id, _index GROUP BY _fork \
             WITH {\"normalizer\": \"minmax\"}",
            |q, s| {
                let c = fuse(q);
                assert_eq!(c.method, Some(FuseMethod::Linear));
                assert_eq!(c.score_by.as_ref().map(dotted).as_deref(), Some("_score"));
                assert_eq!(names(&c.key_by), ["_id", "_index"]);
                assert_eq!(c.group_by.as_ref().map(dotted).as_deref(), Some("_fork"));
                assert_eq!(keys(c.options.as_ref().expect("options")), ["normalizer"]);
                assert!(text(s, c.span).ends_with("{\"normalizer\": \"minmax\"}"));
            },
        ),
        (
            "FUSE",
            "FROM t | FUSE RRF WITH {\"rank_constant\": 60, \"weights\": {\"fork1\": 0.5}}",
            |q, _| {
                let c = fuse(q);
                assert_eq!(c.method, Some(FuseMethod::Rrf));
                assert_eq!(
                    keys(c.options.as_ref().expect("options")),
                    ["rank_constant", "weights"]
                );
            },
        ),
        ("FUSE", "FROM t | FUSE KEY BY a.b", |q, s| {
            let c = fuse(q);
            assert!(c.method.is_none());
            assert_eq!(names(&c.key_by), ["a.b"]);
            assert_eq!(text(s, c.span), "FUSE KEY BY a.b");
        }),
        // ---- CHANGE_POINT ----
        ("CHANGE_POINT", "FROM t | CHANGE_POINT value", |q, s| {
            let c = change_point(q);
            assert_eq!(dotted(&c.value), "value");
            assert!(c.key.is_none() && c.output.is_none());
            assert_eq!(text(s, c.span), "CHANGE_POINT value");
        }),
        (
            "CHANGE_POINT",
            "FROM t | CHANGE_POINT value ON @timestamp",
            |q, _| {
                let c = change_point(q);
                assert_eq!(c.key.as_ref().map(dotted).as_deref(), Some("@timestamp"));
                assert!(c.output.is_none());
            },
        ),
        (
            "CHANGE_POINT",
            "FROM t | CHANGE_POINT m ON k AS type, pvalue",
            |q, s| {
                let c = change_point(q);
                let output = c.output.as_ref().expect("output names");
                assert_eq!(dotted(&output.type_name), "type");
                assert_eq!(dotted(&output.pvalue_name), "pvalue");
                assert_eq!(text(s, output.span), "AS type, pvalue");
                assert_eq!(text(s, c.span), "CHANGE_POINT m ON k AS type, pvalue");
            },
        ),
        // ---- SAMPLE ----
        ("SAMPLE", "FROM t | SAMPLE 0.25", |q, s| {
            let Command::Sample(c) = last(q) else {
                panic!("not SAMPLE: {q:#?}");
            };
            assert_eq!(c.probability, "0.25");
            assert_eq!(text(s, c.span), "SAMPLE 0.25");
        }),
        ("SAMPLE", "FROM t | SAMPLE .5", |q, _| {
            let Command::Sample(c) = last(q) else {
                panic!("not SAMPLE: {q:#?}");
            };
            assert_eq!(c.probability, ".5");
        }),
        ("SAMPLE", "FROM t | SAMPLE 0.001 | LIMIT 10", |q, _| {
            assert!(matches!(
                q.processing.as_slice(),
                [Command::Sample(c), Command::Limit(_)] if c.probability == "0.001"
            ));
        }),
        // ---- COMPLETION ----
        (
            "COMPLETION",
            "FROM t | COMPLETION question WITH {\"inference_id\": \"my-llm\"}",
            |q, s| {
                let c = completion(q);
                assert!(c.target.is_none());
                assert_eq!(field_name(&c.prompt), "question");
                assert_eq!(keys(&c.options), ["inference_id"]);
                assert_eq!(string_value(&c.options.entries[0].value), "my-llm");
                assert_eq!(text(s, c.span), &s[9..]);
            },
        ),
        (
            "COMPLETION",
            "FROM t | COMPLETION answer = CONCAT(\"Summarise: \", body) \
             WITH {\"inference_id\": \"llm\"}",
            |q, _| {
                let c = completion(q);
                assert_eq!(c.target.as_ref().map(dotted).as_deref(), Some("answer"));
                assert!(matches!(&c.prompt, Expr::Call(call) if call.name == "CONCAT"));
            },
        ),
        (
            "COMPLETION",
            "FROM t | COMPLETION ??out = ?prompt WITH {\"inference_id\": ?id}",
            |q, _| {
                let c = completion(q);
                let target = c.target.as_ref().expect("a target");
                let param = target.parts[0].param.as_ref().expect("a parameter target");
                assert_eq!((&param.name, param.identifier), (&named("out"), true));
                assert_eq!(param_name(&c.prompt), (&named("prompt"), false));
                assert_eq!(
                    param_name(&c.options.entries[0].value),
                    (&named("id"), false)
                );
            },
        ),
        // ---- RERANK ----
        (
            "RERANK",
            "FROM t | RERANK \"puppies\" ON title WITH {\"inference_id\": \"reranker\"}",
            |q, s| {
                let c = rerank(q);
                assert!(c.target.is_none());
                assert_eq!(string_value(&c.query), "puppies");
                assert_eq!(names(&c.on), ["title"]);
                assert_eq!(keys(&c.options), ["inference_id"]);
                assert_eq!(text(s, c.span), &s[9..]);
            },
        ),
        (
            "RERANK",
            "FROM t | RERANK score = \"q\" ON title, body.text \
             WITH {\"inference_id\": \"r\", \"top_n\": 5}",
            |q, _| {
                let c = rerank(q);
                assert_eq!(c.target.as_ref().map(dotted).as_deref(), Some("score"));
                assert_eq!(names(&c.on), ["title", "body.text"]);
                assert_eq!(keys(&c.options), ["inference_id", "top_n"]);
            },
        ),
        (
            "RERANK",
            "FROM t | RERANK ?q ON a WITH {\"inference_id\": \"r\"}",
            |q, _| {
                let c = rerank(q);
                assert!(c.target.is_none());
                assert_eq!(param_name(&c.query), (&named("q"), false));
            },
        ),
        // ---- LIMIT BY ----
        ("LIMIT BY", "FROM t | LIMIT 5 BY host", |q, s| {
            let Command::LimitBy(c) = last(q) else {
                panic!("not LIMIT BY: {q:#?}");
            };
            assert_eq!(c.count, 5);
            assert_eq!(names(&c.groupings), ["host"]);
            assert_eq!(text(s, c.span), "LIMIT 5 BY host");
        }),
        ("LIMIT BY", "FROM t | LIMIT 1 BY a, b.c", |q, _| {
            let Command::LimitBy(c) = last(q) else {
                panic!("not LIMIT BY: {q:#?}");
            };
            assert_eq!(
                (c.count, names(&c.groupings)),
                (1, vec!["a".to_owned(), "b.c".to_owned()])
            );
        }),
        ("LIMIT BY", "FROM t | SORT x | LIMIT 0 BY x", |q, _| {
            assert!(matches!(
                q.processing.as_slice(),
                [Command::Sort(_), Command::LimitBy(c)] if c.count == 0
            ));
        }),
        // ---- RENAME, both forms ----
        ("RENAME", "FROM t | RENAME a AS b", |q, s| {
            let Command::Rename(c) = last(q) else {
                panic!("not RENAME: {q:#?}");
            };
            assert_eq!(
                (dotted(&c.renames[0].old), dotted(&c.renames[0].new)),
                ("a".into(), "b".into())
            );
            assert_eq!(text(s, c.renames[0].span), "a AS b");
        }),
        ("RENAME", "FROM t | RENAME b = a", |q, s| {
            let Command::Rename(c) = last(q) else {
                panic!("not RENAME: {q:#?}");
            };
            assert_eq!(
                (dotted(&c.renames[0].old), dotted(&c.renames[0].new)),
                ("a".into(), "b".into())
            );
            assert_eq!(text(s, c.renames[0].span), "b = a");
        }),
        (
            "RENAME",
            "FROM t | RENAME a AS b, d = c, `e f` AS g",
            |q, _| {
                let Command::Rename(c) = last(q) else {
                    panic!("not RENAME: {q:#?}");
                };
                let pairs: Vec<_> = c
                    .renames
                    .iter()
                    .map(|r| (dotted(&r.old), dotted(&r.new)))
                    .collect();
                assert_eq!(
                    pairs,
                    [
                        ("a".to_owned(), "b".to_owned()),
                        ("c".to_owned(), "d".to_owned()),
                        ("e f".to_owned(), "g".to_owned())
                    ]
                );
            },
        ),
        // ---- option maps ----
        (
            "option map",
            "FROM t | WHERE MATCH(title, \"q\", {\"fuzziness\": 1, \"operator\": \"AND\"})",
            |q, s| {
                let map = call_map(where_condition(q));
                assert_eq!(keys(map), ["fuzziness", "operator"]);
                assert_eq!(
                    text(s, map.span),
                    "{\"fuzziness\": 1, \"operator\": \"AND\"}"
                );
                assert_eq!(text(s, map.entries[1].key_span), "\"operator\"");
            },
        ),
        (
            "option map",
            "ROW x = f({\"a\": [1, 2], \"b\": {\"c\": true}, \"d\": -1.5})",
            |q, _| {
                let Command::Row(row) = &q.source else {
                    panic!("not ROW: {q:#?}");
                };
                let map = call_map(&row.fields[0].expr);
                assert_eq!(keys(map), ["a", "b", "d"]);
                assert!(matches!(&map.entries[0].value, Expr::Array(a) if a.elements.len() == 2));
                assert!(matches!(&map.entries[1].value, Expr::Map(_)));
            },
        ),
        (
            "option map",
            "FROM t | WHERE QSTR(\"a:b\", {\"default_field\": ?f})",
            |q, _| {
                let map = call_map(where_condition(q));
                assert_eq!(param_name(&map.entries[0].value), (&named("f"), false));
            },
        ),
        // ---- parameters ----
        ("parameter", "ROW a = ?, b = ?2, c = ?name", |q, _| {
            let Command::Row(row) = &q.source else {
                panic!("not ROW: {q:#?}");
            };
            let params: Vec<_> = row.fields.iter().map(|f| param_name(&f.expr)).collect();
            assert_eq!(
                params,
                [
                    (&ParamName::Positional, false),
                    (&ParamName::Numbered("2".into()), false),
                    (&named("name"), false)
                ]
            );
        }),
        (
            "parameter",
            "FROM t | KEEP ??field, a* | EVAL ??out = ?v + 1",
            |q, _| {
                let Command::Keep(keep) = &q.processing[0] else {
                    panic!("not KEEP: {q:#?}");
                };
                let param = keep.patterns[0].parts[0]
                    .param
                    .as_ref()
                    .expect("a parameter");
                assert_eq!((&param.name, param.identifier), (&named("field"), true));
                let Command::Eval(eval) = &q.processing[1] else {
                    panic!("not EVAL: {q:#?}");
                };
                let name = eval.fields[0].name.as_ref().expect("an assigned name");
                assert_eq!(name.parts[0].text, "??out");
                assert!(name.parts[0].param.as_ref().is_some_and(|p| p.identifier));
            },
        ),
        (
            "parameter",
            "FROM t | STATS c = COUNT(*) BY ??group | SORT ?x",
            |q, _| {
                let Command::Stats(stats) = &q.processing[0] else {
                    panic!("not STATS: {q:#?}");
                };
                assert_eq!(
                    param_name(&stats.groupings[0].expr),
                    (&named("group"), true)
                );
                let Command::Sort(sort) = &q.processing[1] else {
                    panic!("not SORT: {q:#?}");
                };
                assert_eq!(param_name(&sort.keys[0].expr), (&named("x"), false));
            },
        ),
    ]
}

fn assert_covers_every_form(counts: &BTreeMap<&str, usize>, at_least: usize) {
    for form in FORMS {
        let count = counts.get(form).copied().unwrap_or(0);
        assert!(count >= at_least, "{form}: {count} cases, want {at_least}");
    }
    for form in counts.keys() {
        assert!(FORMS.contains(form), "{form} is not a listed form");
    }
}

#[test]
fn every_command_in_the_corpus_parses_and_prints_back() {
    let mut counts = BTreeMap::new();
    for (form, source, check) in admitted() {
        let query = round_trip(source);
        check(&query, source);
        *counts.entry(form).or_insert(0) += 1;
    }
    assert_covers_every_form(&counts, 3);
}

/// Asserts `source` is refused with [`Error::UnexpectedToken`] at `found`
/// (`None` for the end of input) starting at byte `start`.
fn assert_unexpected(
    source: &str,
    found: Option<&str>,
    expected: &str,
    start: usize,
    line: usize,
    column: usize,
) {
    let error = match parse(source, &Limits::default()) {
        Ok(query) => panic!("{source:?} parsed: {query:#?}"),
        Err(error) => error,
    };
    let Error::UnexpectedToken {
        found: actual_found,
        expected: actual_expected,
        span,
    } = &error
    else {
        panic!("{source:?}: {error:?}");
    };
    let (want_found, end) = match found {
        Some(text) => (format!("`{text}`"), start + text.len()),
        None => ("end of input".to_owned(), start),
    };
    assert_eq!(actual_found, &want_found, "{source:?}");
    assert_eq!(*actual_expected, expected, "{source:?}");
    assert_eq!(*span, Span::new(start, end), "{source:?}");
    assert_eq!(
        error.line_col(source),
        LineCol { line, column },
        "{source:?}"
    );
}

/// (form, source, found, expected, start byte, line, column)
type MalformedCase<'a> = (
    &'a str,
    &'a str,
    Option<&'a str>,
    &'a str,
    usize,
    usize,
    usize,
);

#[test]
fn every_malformed_case_reports_its_line_and_column() {
    let after_where = format!("`AND`, `OR`, `|` or end of input{NO_CHAIN}");
    let cases: &[MalformedCase] = &[
        ("SET", "SET a = 1 ROW x = 1", Some("ROW"), "`;`", 10, 1, 11),
        ("SET", "SET = 1; ROW x = 1", Some("="), "a name", 4, 1, 5),
        ("SET", "SET a = 1;\n", None, SOURCE_EXPECTED, 11, 2, 1),
        (
            "SET",
            "SET a = x; ROW b = 1",
            Some("x"),
            "a constant",
            8,
            1,
            9,
        ),
        ("SHOW INFO", "SHOW", None, "`INFO`", 4, 1, 5),
        (
            "SHOW INFO",
            "SHOW\n  FUNCTIONS",
            Some("FUNCTIONS"),
            "`INFO`",
            7,
            2,
            3,
        ),
        ("TS", "TS", None, "a source pattern", 2, 1, 3),
        (
            "TS",
            "TS m, (FROM a)",
            Some("("),
            "a source pattern",
            6,
            1,
            7,
        ),
        (
            "FROM subquery",
            "FROM a, (FROM b",
            None,
            "`|` or `)`",
            15,
            1,
            16,
        ),
        (
            "FROM subquery",
            "FROM a, (ROW x = 1)",
            Some("ROW"),
            NESTED_SOURCE_EXPECTED,
            9,
            1,
            10,
        ),
        (
            "FROM subquery",
            "FROM a, (FROM b) METADATA _id",
            Some("METADATA"),
            "`,`, `|` or end of input (`METADATA` is not admitted beside a subquery)",
            17,
            1,
            18,
        ),
        (
            "FROM subquery",
            "FROM (FROM b), a",
            Some("("),
            "a source pattern",
            5,
            1,
            6,
        ),
        (
            "IN subquery",
            "FROM t\n| WHERE a IN (FROM u | KEEP a",
            None,
            "`|` or `)`",
            36,
            2,
            30,
        ),
        (
            "IN subquery",
            "FROM t | WHERE a IN (FROM u | KEEP a) == true",
            Some("=="),
            &after_where,
            38,
            1,
            39,
        ),
        ("PROMQL", "PROMQL", None, PROMQL_EXPECTED, 6, 1, 7),
        ("PROMQL", "PROMQL step (up)", Some("(up)"), "`=`", 12, 1, 13),
        (
            "PROMQL",
            "PROMQL step = 1m | (up)",
            Some("|"),
            PROMQL_EXPECTED,
            17,
            1,
            18,
        ),
        (
            "DISSECT",
            "FROM t | DISSECT msg",
            None,
            "a dissect pattern string",
            20,
            1,
            21,
        ),
        (
            "DISSECT",
            "FROM t | DISSECT msg \"%{a}\" APPEND_SEPARATOR \",\"",
            Some("\",\""),
            "`=`",
            45,
            1,
            46,
        ),
        (
            "DISSECT",
            "FROM t | DISSECT a + b \"%{x}\"",
            Some("+"),
            "a dissect pattern string",
            19,
            1,
            20,
        ),
        (
            "GROK",
            "FROM t | GROK msg",
            None,
            "a grok pattern string",
            17,
            1,
            18,
        ),
        (
            "GROK",
            "FROM t\n| GROK msg %{WORD:w}",
            Some("%"),
            "a grok pattern string",
            18,
            2,
            12,
        ),
        ("MV_EXPAND", "FROM t | MV_EXPAND", None, "a name", 18, 1, 19),
        (
            "MV_EXPAND",
            "FROM t | MV_EXPAND a, b",
            Some(","),
            "`|` or end of input",
            20,
            1,
            21,
        ),
        (
            "LOOKUP JOIN",
            "FROM t | LOOKUP JOIN hosts",
            None,
            "`ON`",
            26,
            1,
            27,
        ),
        (
            "LOOKUP JOIN",
            "FROM t | LOOKUP JOIN hosts* ON h",
            Some("hosts*"),
            "a lookup index name without `*`",
            21,
            1,
            22,
        ),
        (
            "LOOKUP JOIN",
            "FROM t | LOOKUP JOIN hosts ON a == b",
            Some("=="),
            "`|` or end of input",
            32,
            1,
            33,
        ),
        (
            "ENRICH",
            "FROM t | ENRICH",
            None,
            "an enrich policy name without `*`",
            15,
            1,
            16,
        ),
        (
            "ENRICH",
            "FROM t | ENRICH p WITH",
            None,
            "a name",
            22,
            1,
            23,
        ),
        (
            "ENRICH",
            "FROM t | ENRICH p* ON a",
            Some("p*"),
            "an enrich policy name without `*`",
            16,
            1,
            17,
        ),
        (
            "INLINE STATS",
            "FROM t | INLINE STATS",
            None,
            "an aggregate or `BY`",
            21,
            1,
            22,
        ),
        (
            "INLINE STATS",
            "FROM t | INLINESTATS BY",
            None,
            "an expression",
            23,
            1,
            24,
        ),
        (
            "FORK",
            "FROM t | FORK (WHERE a)",
            None,
            "`(` opening a `FORK` branch; `FORK` takes at least two",
            23,
            1,
            24,
        ),
        (
            "FORK",
            "FROM t | FORK (WHERE a) (FROM u)",
            Some("FROM"),
            "a processing command",
            25,
            1,
            26,
        ),
        (
            "FORK",
            "FROM t | FORK (WHERE a) (LIMIT 1",
            None,
            "`|` or `)`",
            32,
            1,
            33,
        ),
        (
            "FUSE",
            "FROM t | FUSE WITH",
            None,
            "an option map `{...}`",
            18,
            1,
            19,
        ),
        (
            "FUSE",
            "FROM t | FUSE KEY BY _id SCORE BY _score",
            Some("SCORE"),
            "`|` or end of input",
            25,
            1,
            26,
        ),
        (
            "FUSE",
            "FROM t | FUSE SCORE _score",
            Some("SCORE"),
            "`|` or end of input",
            14,
            1,
            15,
        ),
        (
            "CHANGE_POINT",
            "FROM t | CHANGE_POINT",
            None,
            "a name",
            21,
            1,
            22,
        ),
        (
            "CHANGE_POINT",
            "FROM t | CHANGE_POINT v AS type",
            None,
            "`,` and the p-value column",
            31,
            1,
            32,
        ),
        (
            "SAMPLE",
            "FROM t | SAMPLE 1",
            Some("1"),
            SAMPLE_EXPECTED,
            16,
            1,
            17,
        ),
        (
            "SAMPLE",
            "FROM t | SAMPLE",
            None,
            SAMPLE_EXPECTED,
            15,
            1,
            16,
        ),
        (
            "COMPLETION",
            "FROM t | COMPLETION prompt",
            None,
            "`WITH` and an option map",
            26,
            1,
            27,
        ),
        (
            "COMPLETION",
            "FROM t | COMPLETION prompt WITH \"x\"",
            Some("\"x\""),
            "an option map `{...}`",
            32,
            1,
            33,
        ),
        (
            "COMPLETION",
            "FROM t | COMPLETION p WITH {\"a\": 1, \"a\": 2}",
            Some("\"a\""),
            "a key not already in this map",
            36,
            1,
            37,
        ),
        (
            "RERANK",
            "FROM t | RERANK title ON a WITH {\"k\": 1}",
            Some("title"),
            "a query string or parameter",
            16,
            1,
            17,
        ),
        (
            "RERANK",
            "FROM t | RERANK \"q\" WITH {\"k\": 1}",
            Some("WITH"),
            "`ON`",
            20,
            1,
            21,
        ),
        (
            "RERANK",
            "FROM t | RERANK \"q\" ON a",
            None,
            "`,` or `WITH` and an option map",
            24,
            1,
            25,
        ),
        ("LIMIT BY", "FROM t | LIMIT 5 BY", None, "a name", 19, 1, 20),
        (
            "LIMIT BY",
            "FROM t | LIMIT 5 BY a + 1",
            Some("+"),
            "`|` or end of input",
            22,
            1,
            23,
        ),
        (
            "RENAME",
            "FROM t | RENAME a b",
            Some("b"),
            "`AS` or `=`",
            18,
            1,
            19,
        ),
        ("RENAME", "FROM t | RENAME a AS", None, "a name", 20, 1, 21),
        ("RENAME", "FROM t | RENAME b = ", None, "a name", 20, 1, 21),
        (
            "option map",
            "ROW x = f(a, {\"k\": 1}, b)",
            Some(","),
            "`)` (an option map is the last argument)",
            21,
            1,
            22,
        ),
        (
            "option map",
            "ROW x = f({\"k\": x})",
            Some("x"),
            "a constant",
            16,
            1,
            17,
        ),
        (
            "option map",
            "ROW x = f({k: 1})",
            Some("k"),
            "a string key",
            11,
            1,
            12,
        ),
        (
            "parameter",
            "ROW a = ?1 ?2",
            Some("?2"),
            "`|` or end of input",
            11,
            1,
            12,
        ),
        (
            "parameter",
            "FROM t | RERANK ??q ON a WITH {\"k\": 1}",
            Some("??q"),
            "a query string or parameter",
            16,
            1,
            17,
        ),
    ];
    let mut counts = BTreeMap::new();
    for &(form, source, found, expected, start, line, column) in cases {
        assert_unexpected(source, found, expected, start, line, column);
        *counts.entry(form).or_insert(0) += 1;
    }
    assert_covers_every_form(&counts, 2);
}

#[test]
fn dev_only_commands_stay_unsupported() {
    for command in ["EXPLAIN", "METRICS"] {
        let source = format!("{command} x");
        assert_eq!(
            parse(&source, &Limits::default()),
            Err(Error::UnsupportedSyntax {
                command: command.into(),
                span: Span::new(0, command.len()),
            }),
            "{source:?}"
        );
    }
    // (as written, as reported)
    let processing = [
        ("DEDUP", "DEDUP"),
        ("HIGHLIGHT", "HIGHLIGHT"),
        ("LOOKUP", "LOOKUP"),
        ("JOIN", "JOIN"),
        ("LEFT JOIN", "LEFT JOIN"),
        ("Right Join", "RIGHT JOIN"),
        ("FULL JOIN", "FULL JOIN"),
        ("IP_LOCATION", "IP_LOCATION"),
        ("METRICS_INFO", "METRICS_INFO"),
        ("REGISTERED_DOMAIN", "REGISTERED_DOMAIN"),
        ("TS_COLLAPSE", "TS_COLLAPSE"),
        ("TS_INFO", "TS_INFO"),
        ("URI_PARTS", "URI_PARTS"),
        ("USER_AGENT", "USER_AGENT"),
    ];
    // The same refusal at the top level, in a `FORK` branch, in a subquery
    // of `FROM` and in the subquery of `IN`.
    let contexts = [
        ("FROM t | ", " x"),
        ("FROM t | FORK (", " x) (LIMIT 1)"),
        ("FROM a, (FROM b | ", " x)"),
        ("FROM t | WHERE c IN (FROM u | ", " x)"),
    ];
    for (written, reported) in processing {
        for (before, after) in contexts {
            let source = format!("{before}{written}{after}");
            let error = parse(&source, &Limits::default())
                .err()
                .unwrap_or_else(|| panic!("{source:?} parsed"));
            assert_eq!(
                error,
                Error::UnsupportedSyntax {
                    command: reported.into(),
                    span: Span::new(before.len(), before.len() + written.len()),
                },
                "{source:?}"
            );
            assert_eq!(
                error.line_col(&source),
                LineCol {
                    line: 1,
                    column: before.len() + 1
                },
                "{source:?}"
            );
        }
    }
}

#[test]
fn in_list_elements_admit_a_value_or_one_comparison() {
    let source = "ROW x = a IN (1, b == 2, c + 1, -d, f(e), (g IN (1)), (NOT h), i::long)";
    let query = round_trip(source);
    let Command::Row(row) = &query.source else {
        panic!("not ROW: {query:#?}");
    };
    let Expr::In(InList { list, .. }) = &row.fields[0].expr else {
        panic!("not IN: {row:#?}");
    };
    let kinds: Vec<&str> = list
        .iter()
        .map(|e| match e {
            Expr::Literal(_) => "literal",
            Expr::Binary(b) if b.op == BinaryOp::Eq => "==",
            Expr::Binary(b) if b.op == BinaryOp::Add => "+",
            Expr::Unary(u) if u.op == UnaryOp::Minus => "-",
            Expr::Unary(u) if u.op == UnaryOp::Not => "NOT",
            Expr::Call(_) => "call",
            Expr::In(_) => "IN",
            Expr::Cast(_) => "cast",
            other => panic!("unexpected element {other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        ["literal", "==", "+", "-", "call", "IN", "NOT", "cast"]
    );

    // (source, the refused operator)
    let refused = [
        ("ROW x = a IN (b IN (1))", "IN"),
        ("ROW x = a IN (1, b NOT IN (1))", "NOT"),
        ("ROW x = a IN (b LIKE \"p\")", "LIKE"),
        ("ROW x = a IN (b NOT RLIKE \"p\")", "NOT"),
        ("ROW x = a IN (b IS NULL)", "IS"),
        ("ROW x = a IN (b : \"x\")", ":"),
    ];
    for (source, operator) in refused {
        let start = source.rfind(&format!(" {operator} ")).expect("operator") + 1;
        assert_unexpected(
            source,
            Some(operator),
            IN_ELEMENT_PREDICATE,
            start,
            1,
            start + 1,
        );
    }
    let in_element = format!("`,` or `)`{NO_CHAIN}");
    assert_unexpected(
        "ROW x = a IN (1, b == 1 == 2)",
        Some("=="),
        &in_element,
        24,
        1,
        25,
    );
    assert_unexpected(
        "ROW x = a IN (NOT b)",
        Some("NOT"),
        "an expression (parenthesise a `NOT` here)",
        14,
        1,
        15,
    );
}

#[test]
fn match_needs_a_field_on_its_left() {
    for source in [
        "FROM t | WHERE f(a) : \"x\"",
        "FROM t | WHERE 1 : \"x\"",
        "FROM t | WHERE \"a\" : \"x\"",
    ] {
        let start = source.find(" : ").expect("`:`") + 1;
        assert_unexpected(
            source,
            Some(":"),
            "an operator; `:` needs a field on its left",
            start,
            1,
            start + 1,
        );
    }
    for source in [
        "FROM t | WHERE a.b : \"x\"",
        "FROM t | WHERE a::keyword : \"x\"",
    ] {
        let query = round_trip(source);
        assert!(
            matches!(where_condition(&query), Expr::Binary(b) if b.op == BinaryOp::Match),
            "{source:?}"
        );
    }
}

#[test]
fn a_refused_second_comparison_names_what_may_follow_in_its_context() {
    // (source, what may follow the first comparison there)
    let cases = [
        (
            "FROM t | WHERE a == 1 == 2",
            "`AND`, `OR`, `|` or end of input",
        ),
        (
            "ROW x = a == 1 == 2",
            "`AND`, `OR`, `,`, `|` or end of input",
        ),
        (
            "FROM t | STATS a == 1 == 2",
            "`AND`, `OR`, `WHERE`, `BY`, `,`, `|` or end of input",
        ),
        (
            "FROM t | STATS COUNT(*) WHERE a == 1 == 2",
            "`AND`, `OR`, `BY`, `,`, `|` or end of input",
        ),
        (
            "FROM t | SORT a == 1 == 2",
            "`AND`, `OR`, `ASC`, `DESC`, `NULLS`, `,`, `|` or end of input",
        ),
        ("ROW x = (a == 1 == 2)", "`AND`, `OR` or `)`"),
        ("ROW x = f(a == 1 == 2)", "`AND`, `OR`, `,` or `)`"),
        (
            "FROM t | FORK (WHERE a == 1 == 2) (LIMIT 1)",
            "`AND`, `OR`, `|` or `)`",
        ),
        (
            "FROM a, (FROM b | EVAL x = a == 1 == 2)",
            "`AND`, `OR`, `,`, `|` or `)`",
        ),
        (
            "FROM t | WHERE c IN (FROM u | STATS a == 1 == 2)",
            "`AND`, `OR`, `WHERE`, `BY`, `,`, `|` or `)`",
        ),
        (
            "FROM t | FORK (STATS COUNT(*) WHERE a == 1 == 2) (LIMIT 1)",
            "`AND`, `OR`, `BY`, `,`, `|` or `)`",
        ),
        (
            "FROM t | FORK (SORT a == 1 == 2) (LIMIT 1)",
            "`AND`, `OR`, `ASC`, `DESC`, `NULLS`, `,`, `|` or `)`",
        ),
        (
            "FROM t | WHERE a IN (FROM u) == 1",
            "`AND`, `OR`, `|` or end of input",
        ),
    ];
    for (source, followers) in cases {
        let start = source.rfind("==").expect("`==`");
        assert_unexpected(
            source,
            Some("=="),
            &format!("{followers}{NO_CHAIN}"),
            start,
            1,
            start + 1,
        );
    }
}

#[test]
fn set_directives_end_at_a_semicolon_and_stay_outside_the_query_span() {
    let source = "SET a = 1;SET b = \"x\" ;\n FROM t";
    let query = round_trip(source);
    assert_eq!(query.settings.len(), 2);
    assert_eq!(text(source, setting(&query, 1).span), "SET b = \"x\" ;");
    assert_eq!(text(source, query.span), "FROM t");
    // A directive is not a pipeline stage and a pipeline cannot hold one.
    assert_unexpected(
        "FROM t | SET a = 1;",
        Some("SET"),
        "a processing command",
        9,
        1,
        10,
    );
    assert_unexpected(
        "FROM a, (SET x = 1; FROM b)",
        Some("SET"),
        NESTED_SOURCE_EXPECTED,
        9,
        1,
        10,
    );
}

#[test]
fn an_in_list_opening_with_the_word_from_prints_parenthesised() {
    // `IN (FROM` opens a subquery, so a field or function named `from`
    // first in a list must be parenthesised to stay a list element.
    assert_unexpected(
        "ROW x = a IN (from)",
        Some(")"),
        "a source pattern",
        18,
        1,
        19,
    );
    // Kept refused rather than told apart by what follows the source: a
    // subquery may list several sources, so `(FROM a, b | ...)` and a list
    // `(from, b)` differ only after the comma.
    assert_unexpected(
        "ROW x = a IN (from, b)",
        Some(","),
        "a source pattern",
        18,
        1,
        19,
    );
    for (source, printed) in [
        ("ROW x = a IN ((from), b)", "ROW x = a IN ((from), b)"),
        ("ROW x = a IN ((FROM.c + 1))", "ROW x = a IN ((FROM.c + 1))"),
        ("ROW x = a IN ((from(1)))", "ROW x = a IN ((from(1)))"),
        ("ROW x = a IN (b, from)", "ROW x = a IN (b, from)"),
        ("ROW x = a IN (`from`)", "ROW x = a IN (`from`)"),
        ("ROW x = a IN (fromage)", "ROW x = a IN (fromage)"),
    ] {
        let query = round_trip(source);
        assert_eq!(
            print(&query, &Limits::default()).expect("prints"),
            printed,
            "{source:?}"
        );
    }
}

/// A `|`, `)`, quote or `#` inside a PromQL string, regex or comment belongs
/// to the query: the region ends at its own closing parenthesis and the
/// pipeline continues after it.
#[test]
fn a_promql_region_ends_at_its_own_closing_parenthesis() {
    let cases = [
        // A regex with `|` and an escaped `)`.
        (
            r#"PROMQL (up{job=~"a|b\\)"}) | WHERE value > 0"#,
            r#"up{job=~"a|b\\)"}"#,
        ),
        // An escaped double quote before `)` and `|`.
        (
            r#"PROMQL (up{job="a\")|("}) | WHERE value > 0"#,
            r#"up{job="a\")|("}"#,
        ),
        // A single-quoted string holding a double quote and `#`.
        (
            r#"PROMQL (count(up{path='/"(x|y)#'})) | WHERE value > 0"#,
            r#"count(up{path='/"(x|y)#'})"#,
        ),
        // A raw string holding `)`, `|`, `#` and a double quote.
        (
            r#"PROMQL (up{path=`a|"b)#`}) | WHERE value > 0"#,
            r#"up{path=`a|"b)#`}"#,
        ),
        // A comment holding `)`, `|` and a quote, ended by the newline.
        (
            "PROMQL (sum(up) # ) | \"x' (\n) | WHERE value > 0",
            "sum(up) # ) | \"x' (\n",
        ),
    ];
    for (source, inner) in cases {
        let query = round_trip(source);
        let c = promql(&query);
        assert_eq!(c.query.text, inner, "{source:?}");
        assert_eq!(text(source, c.query.span), inner, "{source:?}");
        assert!(c.query.error.is_none(), "{source:?}: {:#?}", c.query.error);
        assert_eq!(
            text(source, c.span),
            format!("PROMQL ({inner})"),
            "{source:?}"
        );
        assert!(
            matches!(query.processing.as_slice(), [Command::Where(_)]),
            "{source:?}: {:#?}",
            query.processing
        );
    }
}

/// `parse_guarded` reports a message and no position, so a refusal inside
/// such a region spans the whole query text between the parentheses.
#[test]
fn a_refused_promql_region_spans_its_query_text() {
    let source = r#"PROMQL (up{job=~"a|b)"} + # ) |
) | WHERE value > 0"#;
    let query = round_trip(source);
    let c = promql(&query);
    let error = c.query.error.as_ref().expect("`up +` is refused");
    assert!(!error.message.is_empty());
    assert_eq!(error.span, c.query.span);
    assert_eq!(text(source, error.span), "up{job=~\"a|b)\"} + # ) |\n");
    assert!(matches!(query.processing.as_slice(), [Command::Where(_)]));
}
