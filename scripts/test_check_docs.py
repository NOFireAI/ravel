#!/usr/bin/env python3
"""Hermetic unit tests for scripts/check_docs.py.

No network and no repository walk: every test builds a throwaway repository under
a temporary directory (the way test_check_readme_commands.py builds its fixtures),
points check_docs at it, and asserts the rules. One test per rule identifier
proves the rule fires on a seeded violation and stays silent on clean input, plus
tests that pin the baseline's line-independent key and the --strict-baseline
behaviour.

Run: cd scripts && python3 -m unittest test_check_docs -v
"""

import io
import os
import shutil
import tempfile
import unittest

import check_docs

SESSION_RS = '''\
pub const SAMPLES_TABLE: &str = "samples";
pub const LOGS_TABLE: &str = "logs";
pub const SPANS_TABLE: &str = "spans";
'''


def _rel_from_docs(path):
    return path[len("docs/"):]


class RepoCase(unittest.TestCase):
    """Base class: a temp repo with the mandatory scaffolding filled in."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="docscheck-")
        self._saved_root = check_docs.REPO_ROOT
        self._saved_baseline = check_docs.BASELINE_PATH
        check_docs.REPO_ROOT = self.root
        check_docs.BASELINE_PATH = os.path.join(self.root, "scripts", "docs_lint_baseline.txt")

    def tearDown(self):
        check_docs.REPO_ROOT = self._saved_root
        check_docs.BASELINE_PATH = self._saved_baseline
        shutil.rmtree(self.root, ignore_errors=True)

    def write(self, files):
        """Write `files` (relpath -> content), then fill missing scaffolding.

        Scaffolding: the SQL table source constants, a docs/README.md that links
        every provided docs markdown (so ORPHAN stays quiet unless a test drops
        that link), and a docs/adrs/README.md indexing every provided ADR file.
        """
        files = dict(files)
        files.setdefault("crates/ravel-sql/src/session.rs", SESSION_RS)

        docs_md = [
            p for p in files
            if p.startswith("docs/") and p.endswith(".md") and p != "docs/README.md"
        ]
        if "docs/README.md" not in files:
            links = ["# Docs index", ""]
            for p in sorted(docs_md):
                links.append(f"- [{p}]({_rel_from_docs(p)})")
            files["docs/README.md"] = "\n".join(links) + "\n"

        if "docs/adrs/README.md" not in files:
            adrs = sorted(
                p for p in files
                if p.startswith("docs/adrs/") and p.endswith(".md")
                and os.path.basename(p)[:4].isdigit()
            )
            rows = ["# Decision records", "", "| # | Title | Status |", "|---|---|---|"]
            for p in adrs:
                base = os.path.basename(p)
                rows.append(f"| [{base[:4]}]({base}) | t | Accepted |")
            files["docs/adrs/README.md"] = "\n".join(rows) + "\n"

        for rel, content in files.items():
            absp = os.path.join(self.root, rel)
            os.makedirs(os.path.dirname(absp), exist_ok=True)
            with open(absp, "w", encoding="utf-8") as fh:
                fh.write(content)

    def findings(self, files):
        self.write(files)
        repo = check_docs.Repo()
        check_docs.gather(repo)
        return check_docs.collect_findings(repo)

    def rules(self, findings, rule):
        return [f for f in findings if f.rule == rule]

    def gate(self, strict=False):
        out = io.StringIO()
        code = check_docs.run(strict=strict, out=out)
        return code, out.getvalue()

    def update_baseline(self):
        out = io.StringIO()
        return check_docs.run(update_baseline=True, out=out)


# --------------------------------------------------------------------------
# One test per rule identifier: fires on a violation, silent on clean input.
# --------------------------------------------------------------------------


class TestLink(RepoCase):
    def test_broken_link_fires(self):
        f = self.findings({"docs/guides/a.md": "See [x](nope.md).\n"})
        self.assertEqual([x.key for x in self.rules(f, "LINK")], ["nope.md"])

    def test_resolving_link_clean(self):
        f = self.findings({
            "docs/guides/a.md": "See [x](b.md).\n",
            "docs/guides/b.md": "# B\n",
        })
        self.assertEqual(self.rules(f, "LINK"), [])

    @staticmethod
    def _naive_case_insensitive_exists(absp):
        # What os.path.exists IS on a case-insensitive macOS filesystem: a
        # component matches an on-disk entry ignoring case. This is the naive
        # implementation the rule must beat; simulating it makes the demonstration
        # platform-independent (on Linux os.path.exists is already case-exact, so
        # it cannot show the bug).
        parent = os.path.dirname(absp)
        name = os.path.basename(absp)
        try:
            entries = os.listdir(parent)
        except OSError:
            return False
        return name.lower() in [e.lower() for e in entries]

    def test_case_sensitivity_beats_os_path_exists(self):
        # A real on-disk entry `Foo.md`, a link written as `foo.md`: identical on a
        # case-insensitive macOS filesystem, a 404 on GitHub.
        self.write({
            "docs/guides/Foo.md": "# Foo\n",
            "docs/guides/a.md": "See [x](Foo.md).\n",
        })
        good = os.path.join(self.root, "docs/guides/Foo.md")
        bad = os.path.join(self.root, "docs/guides/foo.md")
        self.assertTrue(check_docs.exists_cs(good))
        self.assertFalse(check_docs.exists_cs(bad))
        # The naive (case-insensitive) resolver ACCEPTS foo.md -- the bug. exists_cs
        # rejects it. This is the assertion that fails if exists_cs is replaced by a
        # case-insensitive check (on macOS, `return os.path.exists(absp)` is exactly
        # that; here we simulate it so the demonstration also holds on Linux).
        self.assertTrue(self._naive_case_insensitive_exists(bad))
        self.assertFalse(check_docs.exists_cs(bad))
        # And end to end: the wrong-case link is a LINK finding.
        f = self.findings({
            "docs/guides/Foo.md": "# Foo\n",
            "docs/guides/a.md": "See [x](foo.md).\n",
        })
        self.assertIn("foo.md", [x.key for x in self.rules(f, "LINK")])


class TestAnchor(RepoCase):
    def test_dead_anchor_fires(self):
        f = self.findings({"docs/guides/a.md": "# Hello\n\n[x](#nope)\n"})
        self.assertIn("#nope", [x.key for x in self.rules(f, "ANCHOR")])

    def test_live_anchor_clean(self):
        f = self.findings({"docs/guides/a.md": "# Hello World\n\n[x](#hello-world)\n"})
        self.assertEqual(self.rules(f, "ANCHOR"), [])

    def test_duplicate_heading_second_resolves_to_dash_one(self):
        anchors = check_docs.anchors_for(["## Repeat", "text", "## Repeat", "more"])
        # The second identical heading gets the -1 slug, in document order.
        self.assertIn("repeat", anchors)
        self.assertIn("repeat-1", anchors)
        self.assertNotIn("repeat-2", anchors)
        # And a link to #repeat-1 is clean while #repeat-2 is a dead anchor.
        body = "## Repeat\n\ntext\n\n## Repeat\n\n[a](#repeat-1)\n[b](#repeat-2)\n"
        f = self.findings({"docs/guides/a.md": body})
        keys = [x.key for x in self.rules(f, "ANCHOR")]
        self.assertIn("#repeat-2", keys)
        self.assertNotIn("#repeat-1", keys)


class TestProvenance(RepoCase):
    def test_commit_hash_and_stamp_fire(self):
        f = self.findings({
            "docs/guides/a.md": "At commit `cc5ef36d`.\nLast verified against the code: today.\n",
        })
        keys = [x.key for x in self.rules(f, "PROVENANCE")]
        self.assertIn("cc5ef36d", keys)
        self.assertIn("Last verified against the code", keys)

    def test_clean_prose_silent(self):
        f = self.findings({"docs/guides/a.md": "Ravel stores telemetry in objects.\n"})
        self.assertEqual(self.rules(f, "PROVENANCE"), [])


class TestTracker(RepoCase):
    def test_issue_and_adr_fire(self):
        f = self.findings({"docs/guides/a.md": "See #123 and ADR-0042 for details.\n"})
        keys = [x.key for x in self.rules(f, "TRACKER")]
        self.assertIn("#123", keys)
        self.assertIn("ADR-0042", keys)

    def test_background_heading_exempt(self):
        f = self.findings({
            "docs/guides/a.md": "# Guide\n\ntext\n\n## Background\n\nSee ADR-0042.\n",
        })
        self.assertEqual(self.rules(f, "TRACKER"), [])


class TestSrcpath(RepoCase):
    def test_source_path_fires(self):
        f = self.findings({"docs/guides/a.md": "Read crates/ravel-query/src/config.rs.\n"})
        self.assertIn("crates/ravel-query/src/config.rs",
                      [x.key for x in self.rules(f, "SRCPATH")])

    def test_no_source_path_clean(self):
        f = self.findings({"docs/guides/a.md": "Configure the query engine with flags.\n"})
        self.assertEqual(self.rules(f, "SRCPATH"), [])


class TestSuperlative(RepoCase):
    def test_word_and_em_dash_fire(self):
        f = self.findings({"docs/guides/a.md": "A blazing store — truly.\n"})
        keys = [x.key for x in self.rules(f, "SUPERLATIVE")]
        self.assertIn("blazing", keys)
        self.assertIn("em-dash", keys)

    def test_plain_prose_clean(self):
        f = self.findings({"docs/guides/a.md": "The store writes segments to object storage.\n"})
        self.assertEqual(self.rules(f, "SUPERLATIVE"), [])


class TestTerm(RepoCase):
    def test_declared_column_fires(self):
        f = self.findings({"docs/guides/a.md": "Promote a declared column for speed.\n"})
        self.assertIn("declared column", [x.key for x in self.rules(f, "TERM")])

    def test_robust_without_gloss_fires(self):
        f = self.findings({"docs/guides/a.md": "The system is robust under load.\n"})
        self.assertIn("robust", [x.key for x in self.rules(f, "TERM")])

    def test_robust_with_statistical_context_clean(self):
        # The word is correct here: the statistical sense, glossed or named.
        f = self.findings({
            "docs/guides/a.md":
                "A robust (median and scaled-MAD) estimator resists outliers.\n"
                "The MAD gives a robust spread.\n",
        })
        self.assertEqual(self.rules(f, "TERM"), [])

    def test_canonical_term_clean(self):
        f = self.findings({"docs/guides/a.md": "Promote a typed attribute column.\n"})
        self.assertEqual(self.rules(f, "TERM"), [])


class TestSvg(RepoCase):
    VALID = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 10 10" '
             'role="img" aria-label="A diagram"><rect width="10" height="10"/></svg>\n')

    def test_invalid_svg_fires(self):
        bad = ('<svg xmlns="http://www.w3.org/2000/svg">'
               '<image href="http://x/y.png"/></svg>\n')
        f = self.findings({
            "docs/d.svg": bad,
            "docs/guides/a.md": "![a diagram](../d.svg)\n",
        })
        reasons = " ".join(x.key for x in self.rules(f, "SVG"))
        self.assertIn("no viewBox", reasons)
        self.assertIn("raster <image>", reasons)

    def test_valid_referenced_svg_clean(self):
        f = self.findings({
            "docs/d.svg": self.VALID,
            "docs/guides/a.md": "![a diagram](../d.svg)\n",
        })
        self.assertEqual(self.rules(f, "SVG"), [])

    def test_missing_alt_text_fires(self):
        f = self.findings({
            "docs/d.svg": self.VALID,
            "docs/guides/a.md": "![](../d.svg)\n",
        })
        self.assertTrue(any("empty alt text" in x.key for x in self.rules(f, "SVG")))


class TestOrphan(RepoCase):
    def test_unreachable_page_fires(self):
        # docs/README.md links b.md and the ADR index; orphan.md is reachable
        # from nowhere.
        f = self.findings({
            "docs/README.md": "# Index\n\n- [b](b.md)\n- [adrs](adrs/README.md)\n",
            "docs/b.md": "# B\n",
            "docs/orphan.md": "# Orphan\n",
        })
        self.assertIn("docs/orphan.md", [x.key for x in self.rules(f, "ORPHAN")])

    def test_reachable_page_clean(self):
        f = self.findings({
            "docs/README.md": "# Index\n\n- [b](b.md)\n- [adrs](adrs/README.md)\n",
            "docs/b.md": "# B\n",
        })
        self.assertEqual(self.rules(f, "ORPHAN"), [])


class TestAdrindex(RepoCase):
    def test_unindexed_record_fires(self):
        f = self.findings({
            "docs/adrs/0001-a.md": "# A\n",
            "docs/adrs/0002-b.md": "# B\n",
            "docs/adrs/README.md":
                "# ADRs\n\n| # | Title | Status |\n|---|---|---|\n"
                "| [0001](0001-a.md) | a | Accepted |\n",
        })
        self.assertIn("0002", [x.key for x in self.rules(f, "ADRINDEX")])

    def test_fully_indexed_clean(self):
        f = self.findings({
            "docs/adrs/0001-a.md": "# A\n",
            "docs/adrs/README.md":
                "# ADRs\n\n| # | Title | Status |\n|---|---|---|\n"
                "| [0001](0001-a.md) | a | Accepted |\n",
        })
        self.assertEqual(self.rules(f, "ADRINDEX"), [])


class TestSqltable(RepoCase):
    def test_unknown_table_fires(self):
        f = self.findings({
            "docs/guides/a.md": "```sql\nSELECT x FROM bogus_table\n```\n",
        })
        self.assertIn("bogus_table", [x.key for x in self.rules(f, "SQLTABLE")])

    def test_registered_table_clean(self):
        f = self.findings({
            "docs/guides/a.md": "```sql\nSELECT x FROM samples\n```\n",
        })
        self.assertEqual(self.rules(f, "SQLTABLE"), [])


class TestFeature(RepoCase):
    MATRIX = (
        "# Ravel\n\n"
        "<!-- BEGIN SUPPORT MATRIX -->\n"
        "| Capability | Feature gate | In published image |\n"
        "|---|---|---|\n"
        "| Metrics | none | yes |\n"
        "| Flight SQL | {gate} | no |\n"
        "<!-- END SUPPORT MATRIX -->\n"
    )

    def _cargo(self, features):
        body = "[package]\nname = \"foo\"\n\n[features]\n"
        for name in features:
            body += f"{name} = []\n"
        return body

    def test_unknown_gate_fires(self):
        f = self.findings({
            "README.md": self.MATRIX.format(gate="flight-sql"),
            "crates/foo/Cargo.toml": self._cargo(["sql"]),
        })
        self.assertIn("flight-sql", [x.key for x in self.rules(f, "FEATURE")])

    def test_existing_gate_clean(self):
        f = self.findings({
            "README.md": self.MATRIX.format(gate="sql"),
            "crates/foo/Cargo.toml": self._cargo(["sql"]),
        })
        self.assertEqual(self.rules(f, "FEATURE"), [])

    def test_missing_published_image_column_fires(self):
        matrix = (
            "# Ravel\n\n"
            "<!-- BEGIN SUPPORT MATRIX -->\n"
            "| Capability | Feature gate |\n"
            "|---|---|\n"
            "| Metrics | none |\n"
            "<!-- END SUPPORT MATRIX -->\n"
        )
        f = self.findings({"README.md": matrix})
        self.assertTrue(
            any("In published image" in x.key for x in self.rules(f, "FEATURE"))
        )


# --------------------------------------------------------------------------
# Baseline behaviour.
# --------------------------------------------------------------------------


class TestBaseline(RepoCase):
    VIOLATION = "docs/guides/a.md"

    def _seed(self, body):
        self.write({self.VIOLATION: body})

    def test_baselined_finding_does_not_fail(self):
        self._seed("Promote a declared column.\n")
        self.assertEqual(self.update_baseline(), 0)
        code, _ = self.gate()
        self.assertEqual(code, 0)

    def test_same_finding_at_different_line_still_passes(self):
        # THE line-key test. Baseline the violation, then move it down several
        # lines. A line-keyed baseline would now see a "new" finding and fail; a
        # text-keyed baseline stays green. The moved line is line 1 -> line 4.
        self._seed("Promote a declared column.\n")
        self.assertEqual(self.update_baseline(), 0)
        with open(os.path.join(self.root, self.VIOLATION), "w", encoding="utf-8") as fh:
            fh.write("intro\n\nmore\n\nPromote a declared column.\n")
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_new_finding_absent_from_baseline_fails(self):
        self._seed("Promote a declared column.\n")
        self.assertEqual(self.update_baseline(), 0)
        # Add a second, un-baselined violation in the same file.
        with open(os.path.join(self.root, self.VIOLATION), "w", encoding="utf-8") as fh:
            fh.write("Promote a declared column.\nRavel has no ingester here.\n")
        code, out = self.gate()
        self.assertEqual(code, 1)
        self.assertIn("ingester", out)

    def test_strict_baseline_fails_on_unused_entry_default_does_not(self):
        self._seed("Promote a declared column.\n")
        self.assertEqual(self.update_baseline(), 0)
        # Fix the finding: the baseline entry now matches nothing (unused).
        with open(os.path.join(self.root, self.VIOLATION), "w", encoding="utf-8") as fh:
            fh.write("Promote a typed attribute column.\n")
        code_default, _ = self.gate(strict=False)
        self.assertEqual(code_default, 0)
        code_strict, out = self.gate(strict=True)
        self.assertEqual(code_strict, 1)
        self.assertIn("unused", out)


if __name__ == "__main__":
    unittest.main()


class TestGeneratedScope(RepoCase):
    """docs/reference/* is generated from the clap definitions, so its prose is
    the code's prose. TRACKER and SRCPATH would demand editing doc comments to
    satisfy a documentation gate, and the page would drift from the definition
    at the next regeneration. Every other rule still applies."""

    REF = "docs/reference/ravel-server-flags.md"

    def test_scope_is_generated_not_user(self):
        self.assertEqual(check_docs.classify(self.REF), "generated")

    def test_citations_and_source_paths_are_allowed(self):
        f = self.findings({
            self.REF: (
                "# Flags\n\n"
                "| `--adaptive-flush-delay` |  |  | Feeds the compactor "
                "(ADR-0067 decision 3); see crates/ravel-ingest/src/config.rs "
                "and issue #123 |\n"
            ),
        })
        self.assertEqual(self.rules(f, "TRACKER"), [])
        self.assertEqual(self.rules(f, "SRCPATH"), [])

    def test_the_same_text_in_a_guide_still_fires(self):
        # The exemption is scope-bound, not a hole in the rules.
        f = self.findings({
            "docs/guides/a.md": (
                "# G\n\nFeeds the compactor (ADR-0067 decision 3); see "
                "crates/ravel-ingest/src/config.rs and issue #123.\n"
            ),
        })
        self.assertTrue(self.rules(f, "TRACKER"))
        self.assertTrue(self.rules(f, "SRCPATH"))

    def test_other_rules_still_apply_to_generated(self):
        f = self.findings({
            self.REF: "# Flags\n\nA seamless corridor.\n\nSee [gone](nope.md).\n",
        })
        self.assertTrue(self.rules(f, "SUPERLATIVE"))
        self.assertTrue(self.rules(f, "LINK"))


class TestBaselineKeyTolerance(RepoCase):
    """An occurrence count of an already-baselined key is not enforced, so a
    concurrent pull request adding a 148th citation to a file with 147
    baselined ones cannot fail a branch that never touched it. A key the file
    has not carried before is still NEW."""

    def test_another_occurrence_of_a_baselined_key_passes(self):
        self.write({"docs/guides/a.md": "# A\n\nSee ADR-0001.\n"})
        self.assertEqual(self.update_baseline(), 0)
        # The same citation again, as a concurrent branch would add it.
        self.write({
            "docs/guides/a.md": "# A\n\nSee ADR-0001.\n\nAnd ADR-0001 again.\n",
        })
        code, out = self.gate()
        self.assertEqual(code, 0, out)

    def test_a_different_key_same_rule_still_fails(self):
        # This is the distinction: same rule, key the file never carried.
        self.write({"docs/guides/a.md": "# A\n\nSee ADR-0001.\n"})
        self.assertEqual(self.update_baseline(), 0)
        self.write({"docs/guides/a.md": "# A\n\nSee ADR-0001 and ADR-0002.\n"})
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("ADR-0002", out)
        self.assertNotIn("NEW  docs/guides/a.md\tTRACKER\tADR-0001", out)

    def test_a_baselined_key_does_not_leak_to_another_file(self):
        self.write({
            "docs/guides/a.md": "# A\n\nSee ADR-0001.\n",
            "docs/guides/b.md": "# B\n\nClean.\n",
        })
        self.assertEqual(self.update_baseline(), 0)
        self.write({
            "docs/guides/a.md": "# A\n\nSee ADR-0001.\n",
            "docs/guides/b.md": "# B\n\nSee ADR-0001.\n",
        })
        code, out = self.gate()
        self.assertEqual(code, 1, out)
        self.assertIn("docs/guides/b.md", out)


class TestProvenanceAiPhrase(RepoCase):
    """"Generated with" is ordinary English; only a named tool or AI after it
    is agent language. A fixture "generated with current timestamps" must not
    fail the gate, and "Generated with Claude Code" must."""

    def test_plain_generated_with_is_silent(self):
        f = self.findings({
            "docs/guides/a.md": "# A\n\nA fixture generated with current timestamps.\n",
        })
        self.assertEqual(self.rules(f, "PROVENANCE"), [])

    def test_named_tool_after_generated_with_fires(self):
        f = self.findings({
            "docs/guides/a.md": "# A\n\nGenerated with Claude Code.\n",
        })
        self.assertTrue(self.rules(f, "PROVENANCE"))

    def test_co_authored_by_fires(self):
        f = self.findings({"docs/guides/a.md": "# A\n\nCo-Authored-By: someone\n"})
        self.assertTrue(self.rules(f, "PROVENANCE"))


class TestSqlTableFunctionFromAndCte(RepoCase):
    """A FROM inside EXTRACT, SUBSTRING, TRIM or OVERLAY names a field, and a
    FROM naming a CTE defined in the same block names a legitimate table. Only
    a FROM naming nothing the session registers is a finding."""

    def test_extract_from_field_is_silent(self):
        f = self.findings({
            "docs/guides/a.md": "# A\n\n```sql\nSELECT EXTRACT(minute FROM ts) FROM samples\n```\n",
        })
        self.assertEqual(self.rules(f, "SQLTABLE"), [])

    def test_cte_name_is_silent(self):
        f = self.findings({
            "docs/guides/a.md": (
                "# A\n\n```sql\nWITH recent AS (SELECT * FROM samples)\n"
                "SELECT count(*) FROM recent\n```\n"
            ),
        })
        self.assertEqual(self.rules(f, "SQLTABLE"), [])

    def test_unregistered_table_still_fires(self):
        f = self.findings({
            "docs/guides/a.md": "# A\n\n```sql\nSELECT * FROM metrics LIMIT 5\n```\n",
        })
        self.assertIn("metrics", [x.key for x in self.rules(f, "SQLTABLE")])

    def test_shell_block_still_requires_select_on_the_line(self):
        f = self.findings({
            "docs/guides/a.md": "# A\n\n```sh\nFROM debian:bookworm\n```\n",
        })
        self.assertEqual(self.rules(f, "SQLTABLE"), [])


class TestSlugifyHeadingLink(RepoCase):
    """GitHub slugs the rendered heading text, so a link's URL is not part of
    the anchor."""

    def test_link_text_survives_url_does_not(self):
        self.assertEqual(check_docs.slugify("See [foo](bar.md) now"), "see-foo-now")

    def test_anchor_to_heading_with_link_resolves(self):
        f = self.findings({
            "docs/guides/bar.md": "# Bar\n",
            "docs/guides/a.md": "## See [foo](bar.md)\n\n[x](#see-foo)\n",
        })
        self.assertEqual(self.rules(f, "ANCHOR"), [])


class TestTrackerAdrIndexLink(RepoCase):
    """Linking the decision-record index is navigation the documentation index
    must do; linking one record is a citation."""

    def test_index_links_are_silent(self):
        f = self.findings({
            "docs/adrs/0001-x.md": "# One\n",
            "docs/README.md": "# Index\n\n[dir](adrs/) and [idx](adrs/README.md)\n",
        })
        self.assertEqual(self.rules(f, "TRACKER"), [])

    def test_record_link_still_fires(self):
        f = self.findings({
            "docs/adrs/0001-x.md": "# One\n",
            "docs/README.md": "# Index\n\n[one](adrs/0001-x.md)\n",
        })
        self.assertTrue(self.rules(f, "TRACKER"))


class TestChangelogScope(RepoCase):
    """CHANGELOG.md is exempt from the tracker rule and held to every other
    one: a changelog names the decision behind a change, and still must not
    carry a source path or a superlative."""

    def test_scope(self):
        self.assertEqual(check_docs.classify("CHANGELOG.md"), "changelog")

    def test_citations_allowed_rest_enforced(self):
        f = self.findings({
            "CHANGELOG.md": (
                "# Changelog\n\n- Typed statistics (ADR-0850, #123) in "
                "crates/x/src/y.rs, a seamless change.\n"
            ),
        })
        self.assertEqual(self.rules(f, "TRACKER"), [])
        self.assertTrue(self.rules(f, "SRCPATH"))
        self.assertTrue(self.rules(f, "SUPERLATIVE"))
