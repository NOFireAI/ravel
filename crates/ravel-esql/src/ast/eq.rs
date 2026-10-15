//! Span-blind structural equality. A work list holds the expression, command
//! and query pairs still to compare, so no comparison recurses on the tree's
//! height, nested pipelines included.

use super::{
    CallArgs, ChangePointOutput, Command, EnrichField, Expr, Field, FromSource, MapLiteral,
    NamePart, Parameter, PromqlValue, QualifiedName, Query, SourcePattern, Subquery,
};

enum Pair<'a> {
    Expr(&'a Expr, &'a Expr),
    Command(&'a Command, &'a Command),
    Query(&'a Query, &'a Query),
}

type Work<'a> = Vec<Pair<'a>>;

pub(super) fn exprs_equal(a: &Expr, b: &Expr) -> bool {
    run(vec![Pair::Expr(a, b)])
}

pub(super) fn queries_equal(a: &Query, b: &Query) -> bool {
    run(vec![Pair::Query(a, b)])
}

fn run(mut work: Work<'_>) -> bool {
    while let Some(pair) = work.pop() {
        let same = match pair {
            Pair::Expr(a, b) => expr_eq(a, b, &mut work),
            Pair::Command(a, b) => command_eq(a, b, &mut work),
            Pair::Query(a, b) => query_eq(a, b, &mut work),
        };
        if !same {
            return false;
        }
    }
    true
}

fn query_eq<'a>(a: &'a Query, b: &'a Query, work: &mut Work<'a>) -> bool {
    if a.settings.len() != b.settings.len() || a.processing.len() != b.processing.len() {
        return false;
    }
    for (x, y) in a.settings.iter().zip(&b.settings) {
        if !part_eq(&x.name, &y.name) {
            return false;
        }
        work.push(Pair::Expr(&x.value, &y.value));
    }
    work.push(Pair::Command(&a.source, &b.source));
    commands_eq(&a.processing, &b.processing, work)
}

fn subquery_eq<'a>(a: &'a Subquery, b: &'a Subquery, work: &mut Work<'a>) -> bool {
    work.push(Pair::Query(&a.query, &b.query));
    true
}

fn commands_eq<'a>(a: &'a [Command], b: &'a [Command], work: &mut Work<'a>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    work.extend(a.iter().zip(b).map(|(x, y)| Pair::Command(x, y)));
    true
}

fn param_eq(a: &Parameter, b: &Parameter) -> bool {
    a.name == b.name && a.identifier == b.identifier
}

fn part_eq(a: &NamePart, b: &NamePart) -> bool {
    a.text == b.text
        && a.quoted == b.quoted
        && match (&a.param, &b.param) {
            (None, None) => true,
            (Some(x), Some(y)) => param_eq(x, y),
            _ => false,
        }
}

fn name_eq(a: &QualifiedName, b: &QualifiedName) -> bool {
    a.parts.len() == b.parts.len() && a.parts.iter().zip(&b.parts).all(|(x, y)| part_eq(x, y))
}

fn names_eq(a: &[QualifiedName], b: &[QualifiedName]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| name_eq(x, y))
}

fn opt_name_eq(a: &Option<QualifiedName>, b: &Option<QualifiedName>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => name_eq(x, y),
        _ => false,
    }
}

fn pattern_eq(a: &SourcePattern, b: &SourcePattern) -> bool {
    a.text == b.text && a.quoted == b.quoted
}

fn patterns_eq(a: &[SourcePattern], b: &[SourcePattern]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| pattern_eq(x, y))
}

fn field_eq<'a>(a: &'a Field, b: &'a Field, work: &mut Work<'a>) -> bool {
    work.push(Pair::Expr(&a.expr, &b.expr));
    opt_name_eq(&a.name, &b.name)
}

fn fields_eq<'a>(a: &'a [Field], b: &'a [Field], work: &mut Work<'a>) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| field_eq(x, y, work))
}

fn opt_expr_eq<'a>(a: &'a Option<Expr>, b: &'a Option<Expr>, work: &mut Work<'a>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            work.push(Pair::Expr(x, y));
            true
        }
        _ => false,
    }
}

fn list_eq<'a>(a: &'a [Expr], b: &'a [Expr], work: &mut Work<'a>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    work.extend(a.iter().zip(b).map(|(x, y)| Pair::Expr(x, y)));
    true
}

fn map_eq<'a>(a: &'a MapLiteral, b: &'a MapLiteral, work: &mut Work<'a>) -> bool {
    a.entries.len() == b.entries.len()
        && a.entries.iter().zip(&b.entries).all(|(x, y)| {
            work.push(Pair::Expr(&x.value, &y.value));
            x.key == y.key
        })
}

fn opt_map_eq<'a>(
    a: &'a Option<MapLiteral>,
    b: &'a Option<MapLiteral>,
    work: &mut Work<'a>,
) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => map_eq(x, y, work),
        _ => false,
    }
}

fn enrich_fields_eq(a: &[EnrichField], b: &[EnrichField]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|(x, y)| opt_name_eq(&x.name, &y.name) && name_eq(&x.field, &y.field))
}

fn output_eq(a: &Option<ChangePointOutput>, b: &Option<ChangePointOutput>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            name_eq(&x.type_name, &y.type_name) && name_eq(&x.pvalue_name, &y.pvalue_name)
        }
        _ => false,
    }
}

fn command_eq<'a>(a: &'a Command, b: &'a Command, work: &mut Work<'a>) -> bool {
    match (a, b) {
        (Command::Row(x), Command::Row(y)) => fields_eq(&x.fields, &y.fields, work),
        (Command::From(x), Command::From(y)) | (Command::Ts(x), Command::Ts(y)) => {
            let metadata = match (&x.metadata, &y.metadata) {
                (None, None) => true,
                (Some(m), Some(n)) => names_eq(&m.fields, &n.fields),
                _ => false,
            };
            patterns_eq(&x.sources, &y.sources) && metadata
        }
        (Command::Where(x), Command::Where(y)) => {
            work.push(Pair::Expr(&x.condition, &y.condition));
            true
        }
        (Command::Eval(x), Command::Eval(y)) => fields_eq(&x.fields, &y.fields, work),
        (Command::Keep(x), Command::Keep(y)) => names_eq(&x.patterns, &y.patterns),
        (Command::Drop(x), Command::Drop(y)) => names_eq(&x.patterns, &y.patterns),
        (Command::Rename(x), Command::Rename(y)) => {
            x.renames.len() == y.renames.len()
                && x.renames
                    .iter()
                    .zip(&y.renames)
                    .all(|(r, s)| name_eq(&r.old, &s.old) && name_eq(&r.new, &s.new))
        }
        (Command::Stats(x), Command::Stats(y))
        | (Command::InlineStats(x), Command::InlineStats(y)) => {
            x.aggregates.len() == y.aggregates.len()
                && x.aggregates.iter().zip(&y.aggregates).all(|(p, q)| {
                    field_eq(&p.field, &q.field, work) && opt_expr_eq(&p.filter, &q.filter, work)
                })
                && fields_eq(&x.groupings, &y.groupings, work)
        }
        (Command::Sort(x), Command::Sort(y)) => {
            x.keys.len() == y.keys.len()
                && x.keys.iter().zip(&y.keys).all(|(p, q)| {
                    work.push(Pair::Expr(&p.expr, &q.expr));
                    p.order == q.order && p.nulls == q.nulls
                })
        }
        (Command::Limit(x), Command::Limit(y)) => x.count == y.count,
        (Command::Show(_), Command::Show(_)) => true,
        (Command::FromQueries(x), Command::FromQueries(y)) => {
            x.sources.len() == y.sources.len()
                && x.sources.iter().zip(&y.sources).all(|(s, t)| match (s, t) {
                    (FromSource::Pattern(p), FromSource::Pattern(q)) => pattern_eq(p, q),
                    (FromSource::Query(p), FromSource::Query(q)) => subquery_eq(p, q, work),
                    _ => false,
                })
        }
        (Command::Promql(x), Command::Promql(y)) => {
            x.query.text == y.query.text
                && x.query.error.as_ref().map(|e| &e.message)
                    == y.query.error.as_ref().map(|e| &e.message)
                && x.params.len() == y.params.len()
                && x.params.iter().zip(&y.params).all(|(p, q)| {
                    p.name == q.name
                        && match (&p.value, &q.value) {
                            (PromqlValue::Text(s), PromqlValue::Text(t)) => pattern_eq(s, t),
                            (PromqlValue::Param(s), PromqlValue::Param(t)) => param_eq(s, t),
                            _ => false,
                        }
                })
        }
        (Command::Dissect(x), Command::Dissect(y)) => {
            work.push(Pair::Expr(&x.input, &y.input));
            x.pattern == y.pattern && x.append_separator == y.append_separator
        }
        (Command::Grok(x), Command::Grok(y)) => {
            work.push(Pair::Expr(&x.input, &y.input));
            x.pattern == y.pattern
        }
        (Command::MvExpand(x), Command::MvExpand(y)) => name_eq(&x.field, &y.field),
        (Command::LookupJoin(x), Command::LookupJoin(y)) => {
            pattern_eq(&x.index, &y.index) && names_eq(&x.on, &y.on)
        }
        (Command::Enrich(x), Command::Enrich(y)) => {
            pattern_eq(&x.policy, &y.policy)
                && opt_name_eq(&x.on, &y.on)
                && enrich_fields_eq(&x.with, &y.with)
        }
        (Command::Fork(x), Command::Fork(y)) => {
            x.branches.len() == y.branches.len()
                && x.branches
                    .iter()
                    .zip(&y.branches)
                    .all(|(p, q)| commands_eq(&p.commands, &q.commands, work))
        }
        (Command::Fuse(x), Command::Fuse(y)) => {
            x.method == y.method
                && opt_name_eq(&x.score_by, &y.score_by)
                && names_eq(&x.key_by, &y.key_by)
                && opt_name_eq(&x.group_by, &y.group_by)
                && opt_map_eq(&x.options, &y.options, work)
        }
        (Command::ChangePoint(x), Command::ChangePoint(y)) => {
            name_eq(&x.value, &y.value)
                && opt_name_eq(&x.key, &y.key)
                && output_eq(&x.output, &y.output)
        }
        (Command::Sample(x), Command::Sample(y)) => x.probability == y.probability,
        (Command::Completion(x), Command::Completion(y)) => {
            work.push(Pair::Expr(&x.prompt, &y.prompt));
            opt_name_eq(&x.target, &y.target) && map_eq(&x.options, &y.options, work)
        }
        (Command::Rerank(x), Command::Rerank(y)) => {
            work.push(Pair::Expr(&x.query, &y.query));
            opt_name_eq(&x.target, &y.target)
                && names_eq(&x.on, &y.on)
                && map_eq(&x.options, &y.options, work)
        }
        (Command::LimitBy(x), Command::LimitBy(y)) => {
            x.count == y.count && names_eq(&x.groupings, &y.groupings)
        }
        _ => false,
    }
}

fn expr_eq<'a>(a: &'a Expr, b: &'a Expr, work: &mut Work<'a>) -> bool {
    match (a, b) {
        (Expr::Literal(x), Expr::Literal(y)) => x.value == y.value,
        (Expr::Interval(x), Expr::Interval(y)) => x.value == y.value && x.unit == y.unit,
        (Expr::Field(x), Expr::Field(y)) => name_eq(x, y),
        (Expr::Param(x), Expr::Param(y)) => param_eq(x, y),
        (Expr::Call(x), Expr::Call(y)) => {
            x.name == y.name
                && match (&x.args, &y.args) {
                    (CallArgs::Star, CallArgs::Star) => true,
                    (CallArgs::List(p), CallArgs::List(q)) => list_eq(p, q, work),
                    _ => false,
                }
        }
        (Expr::Unary(x), Expr::Unary(y)) => {
            work.push(Pair::Expr(&x.operand, &y.operand));
            x.op == y.op
        }
        (Expr::Binary(x), Expr::Binary(y)) => {
            work.push(Pair::Expr(&x.left, &y.left));
            work.push(Pair::Expr(&x.right, &y.right));
            x.op == y.op
        }
        (Expr::In(x), Expr::In(y)) => {
            work.push(Pair::Expr(&x.expr, &y.expr));
            x.negated == y.negated && list_eq(&x.list, &y.list, work)
        }
        (Expr::InSubquery(x), Expr::InSubquery(y)) => {
            work.push(Pair::Expr(&x.expr, &y.expr));
            x.negated == y.negated && subquery_eq(&x.query, &y.query, work)
        }
        (Expr::Like(x), Expr::Like(y)) => {
            work.push(Pair::Expr(&x.expr, &y.expr));
            x.kind == y.kind && x.pattern == y.pattern && x.negated == y.negated
        }
        (Expr::IsNull(x), Expr::IsNull(y)) => {
            work.push(Pair::Expr(&x.expr, &y.expr));
            x.negated == y.negated
        }
        (Expr::Cast(x), Expr::Cast(y)) => {
            work.push(Pair::Expr(&x.expr, &y.expr));
            x.type_name == y.type_name
        }
        (Expr::Array(x), Expr::Array(y)) => list_eq(&x.elements, &y.elements, work),
        (Expr::Map(x), Expr::Map(y)) => map_eq(x, y, work),
        _ => false,
    }
}
