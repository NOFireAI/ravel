#!/usr/bin/env python3
"""Hermetic unit tests for scripts/check-doc-claims.py (ADR-1658).

Every test builds a throwaway repository under a temporary directory, writes a
registry and the normative docs it needs, and runs the gate against that root.
One failing and one passing case per rule (QUOTE, BIND, NEGATIVE, STATUS), the
two cases ADR-1658 decision 5 pins (deleting a bound test fails BIND, an
unregistered negative sentence fails NEGATIVE), and the exit-2 cases.

Run: cd scripts && python3 -m unittest test_check_doc_claims -v
"""

import importlib.util
import io
import os
import re
import shutil
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPEC = importlib.util.spec_from_file_location(
    "check_doc_claims", os.path.join(_HERE, "check-doc-claims.py"))
cdc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cdc)

REAL_REPO = os.path.dirname(_HERE)

# A real bound file and the #[test] function the seeded registry binds in it.
REAL_BOUND_FILE = "crates/ravel-mcp/src/tools/mod.rs"
REAL_BOUND_TEST = "dispatch_routes_every_catalog_name_and_refuses_unknown_names"

WIDGET_RS = '''\
//! Widgets.

/// Builds a widget. Mentions `fn phantom_widget` only in a comment.
pub fn build_widget() -> u32 {
    7
}

pub struct Gadget;

impl Gadget {
    pub const fn size(&self) -> u32 {
        1
    }
}

// fn deleted_helper() -> u32 { 0 }
/* fn also_deleted() {} */

#[cfg(test)]
mod tests {
    #[test]
    fn widget_is_seven() {
        assert_eq!(super::build_widget(), 7);
    }
}
'''

README = """\
# Project

The widget builder ships today.

The frobnicator does not exist in this build.
"""

BASE_ENTRY = """\
- id: readme-frobnicator
  doc: README.md
  quote: "The frobnicator does not exist in this build."
  kind: negative
  binds: []
  status: not-implemented
  note: No frobnicator symbol anywhere.
"""


def entry(**fields):
    """Render one registry entry; list values render as block lists."""
    order = ["id", "doc", "quote", "kind", "binds", "status", "issue", "note"]
    lines = []
    first = True
    for key in order:
        if key not in fields:
            continue
        value = fields[key]
        prefix = "- " if first else "  "
        first = False
        if isinstance(value, list):
            if not value:
                lines.append(f"{prefix}{key}: []")
            else:
                lines.append(f"{prefix}{key}:")
                lines.extend(f"    - {item}" for item in value)
        else:
            escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'{prefix}{key}: "{escaped}"')
    return "\n".join(lines) + "\n"


class RepoCase(unittest.TestCase):
    """A temp repo with every spec doc present and one registered marker line."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="docclaims-")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def write(self, files, registry=None):
        files = dict(files)
        for rel in cdc.SPEC_DOCS:
            files.setdefault(rel, "# Spec\n\nNothing absent here.\n")
        files.setdefault("README.md", README)
        files.setdefault("crates/widget/src/lib.rs", WIDGET_RS)
        if registry is not None:
            files[cdc.REGISTRY] = "# Claims registry.\n" + BASE_ENTRY + registry
        for rel, content in files.items():
            absp = os.path.join(self.root, rel)
            os.makedirs(os.path.dirname(absp), exist_ok=True)
            with open(absp, "w", encoding="utf-8") as fh:
                fh.write(content)

    def run_gate(self, files=None, registry=""):
        self.write(files or {}, registry)
        out = io.StringIO()
        err = io.StringIO()
        code = cdc.run(root=self.root, out=out, err=err)
        return code, out.getvalue() + err.getvalue()

    def assertRule(self, output, rule, needle=""):
        hits = [line for line in output.splitlines()
                if line.startswith(f"{rule} ") and needle in line]
        self.assertTrue(hits, f"expected a {rule} finding containing {needle!r}:\n{output}")


class BaselineTest(RepoCase):
    def test_scaffold_is_clean(self):
        code, output = self.run_gate()
        self.assertEqual(code, 0, output)
        self.assertIn("doc claims: clean", output)


class QuoteRuleTest(RepoCase):
    def test_quote_present_once_passes(self):
        code, output = self.run_gate(registry=entry(
            id="widget-ships", doc="README.md",
            quote="The widget builder ships today.", kind="positive",
            binds=["crates/widget/src/lib.rs::build_widget"], status="verified",
            note="n"))
        self.assertEqual(code, 0, output)

    def test_quote_missing_fails(self):
        code, output = self.run_gate(registry=entry(
            id="widget-gone", doc="README.md",
            quote="The widget builder shipped last year.", kind="positive",
            binds=[], status="verified", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "QUOTE", "widget-gone")

    def test_quote_twice_fails(self):
        doc = README + "\nThe widget builder ships today.\n"
        code, output = self.run_gate(files={"README.md": doc}, registry=entry(
            id="widget-twice", doc="README.md",
            quote="The widget builder ships today.", kind="positive",
            binds=[], status="verified", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "QUOTE", "widget-twice")

    def test_quote_matches_across_a_line_wrap_and_blockquote(self):
        doc = README + "\n> The gizmo is\n> not available on arm.\n"
        code, output = self.run_gate(files={"README.md": doc}, registry=entry(
            id="gizmo", doc="README.md",
            quote="The gizmo is not available on arm.", kind="negative",
            binds=[], status="not-implemented", note="n"))
        self.assertEqual(code, 0, output)

    def test_quote_in_a_non_normative_doc_fails(self):
        code, output = self.run_gate(
            files={"docs/adrs/0001-x.md": "# ADR\n\nThe thing does not exist.\n"},
            registry=entry(id="adr", doc="docs/adrs/0001-x.md",
                           quote="The thing does not exist.", kind="negative",
                           binds=[], status="not-implemented", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "REGISTRY", "adr")


class BindRuleTest(RepoCase):
    def bind(self, ref, status="verified"):
        return self.run_gate(registry=entry(
            id="bind-case", doc="README.md",
            quote="The widget builder ships today.", kind="positive",
            binds=[ref], status=status, note="n"))

    def test_fn_definition_resolves(self):
        code, output = self.bind("crates/widget/src/lib.rs::build_widget")
        self.assertEqual(code, 0, output)

    def test_nested_path_and_test_function_resolve(self):
        code, output = self.bind("crates/widget/src/lib.rs::Gadget::size")
        self.assertEqual(code, 0, output)
        code, output = self.bind("crates/widget/src/lib.rs::tests::widget_is_seven")
        self.assertEqual(code, 0, output)

    def test_missing_file_fails(self):
        code, output = self.bind("crates/widget/src/gone.rs::build_widget")
        self.assertEqual(code, 1, output)
        self.assertRule(output, "BIND", "gone.rs")

    def test_symbol_only_in_a_comment_fails(self):
        for sym in ("phantom_widget", "deleted_helper", "also_deleted"):
            code, output = self.bind(f"crates/widget/src/lib.rs::{sym}")
            self.assertEqual(code, 1, output)
            self.assertRule(output, "BIND", sym)

    def test_symbol_inside_a_longer_identifier_fails(self):
        code, output = self.bind("crates/widget/src/lib.rs::build")
        self.assertEqual(code, 1, output)
        self.assertRule(output, "BIND", "build")

    STRINGY_RS = '''\
pub fn messages() -> (&'static str, &'static str, char, char) {
    ("fn frobnicate was removed", r#"fn raw_ghost "quoted" "#, '"', '\\'')
}

pub fn pointers(_a: &'static Mode, _b: *const Foo) {}

pub const GLOB: &str = "sys/qualify/*";

pub fn arrived_later() {}

/* a closing comment after the glob string */
'''

    def bind_in_stringy(self, ref, status="verified"):
        return self.run_gate(
            files={"crates/widget/src/stringy.rs": self.STRINGY_RS},
            registry=entry(
                id="bind-case", doc="README.md",
                quote="The widget builder ships today.", kind="positive",
                binds=[ref], status=status, note="n"))

    def test_keyword_inside_a_string_literal_is_not_a_definition(self):
        for sym in ("frobnicate", "raw_ghost"):
            code, output = self.bind_in_stringy(f"crates/widget/src/stringy.rs::{sym}")
            self.assertEqual(code, 1, output)
            self.assertRule(output, "BIND", sym)

    def test_static_and_const_in_type_position_are_not_definitions(self):
        for sym in ("Mode", "Foo"):
            code, output = self.bind_in_stringy(f"crates/widget/src/stringy.rs::{sym}")
            self.assertEqual(code, 1, output)
            self.assertRule(output, "BIND", sym)

    def test_comment_opener_inside_a_string_does_not_hide_later_definitions(self):
        code, output = self.bind_in_stringy("crates/widget/src/stringy.rs::arrived_later")
        self.assertEqual(code, 0, output)
        code, output = self.bind_in_stringy(
            "crates/widget/src/stringy.rs::arrived_later", status="not-implemented")
        self.assertEqual(code, 1, output)
        self.assertRule(output, "STATUS", "bind-case")

    def test_non_rust_reference_fails(self):
        code, output = self.bind("crates/widget/Cargo.toml::mcp")
        self.assertEqual(code, 1, output)
        self.assertRule(output, "BIND", "Cargo.toml")

    def test_deleting_a_bound_test_function_fails(self):
        """ADR-1658 D5: a real bound file, its test deleted, fails BIND."""
        with open(os.path.join(REAL_REPO, REAL_BOUND_FILE), encoding="utf-8") as fh:
            real = fh.read()
        self.assertIn(f"fn {REAL_BOUND_TEST}(", real)
        ref = f"{REAL_BOUND_FILE}::{REAL_BOUND_TEST}"
        reg = entry(id="mcp-served", doc="README.md",
                    quote="The widget builder ships today.", kind="positive",
                    binds=[ref], status="verified", note="n")

        code, output = self.run_gate(files={REAL_BOUND_FILE: real}, registry=reg)
        self.assertEqual(code, 0, output)

        deleted = re.sub(
            r"#\[test\]\s*\n\s*fn " + REAL_BOUND_TEST + r"\(\)",
            "#[test]\n    fn renamed_away()", real)
        self.assertNotEqual(deleted, real)
        code, output = self.run_gate(files={REAL_BOUND_FILE: deleted}, registry=reg)
        self.assertEqual(code, 1, output)
        self.assertRule(output, "BIND", REAL_BOUND_TEST)


class NegativeRuleTest(RepoCase):
    def test_registered_marker_line_passes(self):
        code, output = self.run_gate()
        self.assertEqual(code, 0, output)

    def test_unregistered_negative_sentence_fails(self):
        """ADR-1658 D5: an unregistered negative sentence in a normative doc."""
        guide = "# Guide\n\nThe `--turbo` flag is not supported yet.\n"
        code, output = self.run_gate(files={"docs/guides/turbo.md": guide})
        self.assertEqual(code, 1, output)
        self.assertRule(output, "NEGATIVE", "docs/guides/turbo.md:3")

    def test_marker_scan_is_case_insensitive_and_covers_every_marker(self):
        for marker in cdc.MARKERS:
            doc = f"# Spec\n\n{marker.capitalize()} here, somehow.\n"
            code, output = self.run_gate(files={cdc.SPEC_DOCS[0]: doc})
            self.assertEqual(code, 1, f"{marker}: {output}")
            self.assertRule(output, "NEGATIVE", cdc.SPEC_DOCS[0])

    def test_marker_in_code_fence_or_history_is_not_scanned(self):
        guide = "# Guide\n\n```text\nerror: not implemented\n```\n"
        code, output = self.run_gate(files={
            "docs/guides/fence.md": guide,
            "docs/adrs/0001-x.md": "# ADR\n\nThis does not exist.\n",
            "docs/internal/notes.md": "# Notes\n\nNot available.\n",
            "docs/reference/ravel-cli-flags.md": "# Flags\n\nNot available.\n",
        })
        self.assertEqual(code, 0, output)

    def test_reference_is_scanned(self):
        code, output = self.run_gate(files={
            "docs/reference/http-api.md": "# API\n\nGzip is not available.\n",
        })
        self.assertEqual(code, 1, output)
        self.assertRule(output, "NEGATIVE", "docs/reference/http-api.md:3")

    def test_entry_on_another_line_does_not_cover(self):
        doc = README + "\nThe frobnicator does not exist on arm either.\n"
        code, output = self.run_gate(files={"README.md": doc})
        self.assertEqual(code, 1, output)
        self.assertRule(output, "NEGATIVE", "README.md:7")


class StatusRuleTest(RepoCase):
    def test_contradicted_with_issue_passes(self):
        code, output = self.run_gate(registry=entry(
            id="c", doc="README.md", quote="The widget builder ships today.",
            kind="positive", binds=[], status="contradicted", issue="1658",
            note="n"))
        self.assertEqual(code, 0, output)

    def test_contradicted_without_issue_fails(self):
        code, output = self.run_gate(registry=entry(
            id="c", doc="README.md", quote="The widget builder ships today.",
            kind="positive", binds=[], status="contradicted", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "STATUS", "c")

    def test_contradicted_with_a_null_issue_fails(self):
        for issue in ("~", "null", '""'):
            code, output = self.run_gate(registry=entry(
                id="c", doc="README.md", quote="The widget builder ships today.",
                kind="positive", binds=["crates/widget/src/lib.rs::build_widget"],
                status="contradicted", issue=issue, note="n"))
            self.assertEqual(code, 1, (issue, output))
            self.assertRule(output, "STATUS", "c")

    def test_not_implemented_with_unresolved_binds_passes(self):
        code, output = self.run_gate(registry=entry(
            id="ni", doc="README.md", quote="The widget builder ships today.",
            kind="negative", binds=["crates/widget/src/lib.rs::frobnicate"],
            status="not-implemented", note="n"))
        self.assertEqual(code, 0, output)

    def test_not_implemented_with_a_missing_bind_file_fails(self):
        code, output = self.run_gate(registry=entry(
            id="ni", doc="README.md", quote="The widget builder ships today.",
            kind="negative", binds=["crates/widget/src/gone.rs::frobnicate"],
            status="not-implemented", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "BIND", "gone.rs")

    def test_not_implemented_with_a_missing_leading_segment_fails(self):
        code, output = self.run_gate(registry=entry(
            id="ni", doc="README.md", quote="The widget builder ships today.",
            kind="negative", binds=["crates/widget/src/lib.rs::Missing::frobnicate"],
            status="not-implemented", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "BIND", "Missing")

    def test_not_implemented_whose_binds_resolve_fails(self):
        code, output = self.run_gate(registry=entry(
            id="ni", doc="README.md", quote="The widget builder ships today.",
            kind="negative", binds=["crates/widget/src/lib.rs::build_widget"],
            status="not-implemented", note="n"))
        self.assertEqual(code, 1, output)
        self.assertRule(output, "STATUS", "ni")


class RegistrySchemaTest(RepoCase):
    def test_unknown_kind_status_and_duplicate_id_fail(self):
        reg = entry(id="x", doc="README.md", quote="The widget builder ships today.",
                    kind="maybe", binds=[], status="plausible", note="n")
        reg += entry(id="readme-frobnicator", doc="README.md",
                     quote="The widget builder", kind="positive", binds=[],
                     status="verified", note="n")
        code, output = self.run_gate(registry=reg)
        self.assertEqual(code, 1, output)
        self.assertRule(output, "REGISTRY", "kind")
        self.assertRule(output, "REGISTRY", "status")
        self.assertRule(output, "REGISTRY", "duplicate")

    def test_missing_field_fails(self):
        reg = "- id: y\n  doc: README.md\n  quote: The widget builder ships today.\n"
        code, output = self.run_gate(registry=reg)
        self.assertEqual(code, 1, output)
        self.assertRule(output, "REGISTRY", "y")


class CannotRunTest(RepoCase):
    def test_missing_registry_exits_2(self):
        self.write({})
        out, err = io.StringIO(), io.StringIO()
        self.assertEqual(cdc.run(root=self.root, out=out, err=err), 2)
        self.assertIn("registry", err.getvalue())

    def test_zero_marker_matches_exits_2(self):
        code, output = self.run_gate(files={"README.md": "# Project\n\nAll here.\n"})
        self.assertEqual(code, 2, output)
        self.assertIn("zero marker matches", output)

    def test_missing_spec_doc_exits_2(self):
        self.write({}, "")
        os.remove(os.path.join(self.root, cdc.SPEC_DOCS[0]))
        out, err = io.StringIO(), io.StringIO()
        self.assertEqual(cdc.run(root=self.root, out=out, err=err), 2)
        self.assertIn(cdc.SPEC_DOCS[0], err.getvalue())

    def test_unparseable_registry_exits_2(self):
        code, output = self.run_gate(registry="- id: [unterminated\n")
        self.assertEqual(code, 2, output)


class ParserTest(unittest.TestCase):
    def test_scalars_lists_and_folding(self):
        text = (
            "# comment\n"
            "\n"
            "- id: a\n"
            '  quote: "say \\"hi\\" \\\\ there"\n'
            "  binds:\n"
            "    - x.rs::A\n"
            "    - 'y.rs::B'\n"
            "  note: first line\n"
            "    continues here\n"
            "- id: b\n"
            "  binds: []\n"
            "  quote: 'it''s'\n"
        )
        entries = cdc.parse_registry(text)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]["quote"], 'say "hi" \\ there')
        self.assertEqual(entries[0]["binds"], ["x.rs::A", "y.rs::B"])
        self.assertEqual(entries[0]["note"], "first line continues here")
        self.assertEqual(entries[1]["binds"], [])
        self.assertEqual(entries[1]["quote"], "it's")

    def test_rejects_features_outside_the_subset(self):
        bad = [
            "- id: a\n  note: has # a comment-looking tail\n",
            "- id: a\n  note: key: value\n",
            "- id: a\n  note: |\n    block\n",
            "id: a\n",
            '- id: "unterminated\n',
            "- id: a\n  id: b\n",
            '- id: "bad \\n escape"\n',
        ]
        for text in bad:
            with self.assertRaises(cdc.CheckError, msg=text):
                cdc.parse_registry(text)


if __name__ == "__main__":
    unittest.main()
