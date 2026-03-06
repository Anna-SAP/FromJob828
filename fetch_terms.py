#!/usr/bin/env python3
"""
fetch_terms.py — Pull the live multi-language terminology sheet from Google
Sheets, diff against local glossary JSON files, and perform incremental updates.

Data source (CSV export):
  https://docs.google.com/spreadsheets/d/1l7UklehnPi0XK1J4ieuHLrJFzhdpijSe
  Sheet gid=1868962544

Local store:
  output/glossaries/<lang-code>.json   — one file per language
  output/glossaries/_summary.json      — overview

Usage:
  python fetch_terms.py              # fetch, diff, update
  python fetch_terms.py --dry-run    # show diff only, don't write
"""

import argparse
import csv
import io
import json
import re
import sys
import urllib.request
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "output" / "glossaries"

SHEET_ID = "1l7UklehnPi0XK1J4ieuHLrJFzhdpijSe"
GID = "1868962544"
EXPORT_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/export?format=csv&gid={GID}"
)

# ---------------------------------------------------------------------------
# Column-header → canonical lang code mapping
# ---------------------------------------------------------------------------
HEADER_TO_LANG = {
    "zh_cn": "zh-cn",
    "zh_tw": "zh-tw",
    "nl_nl": "nl-nl",
    "fi_fi": "fi-fi",
    "ja_jp": "ja-jp",
    "ko_kr": "ko-kr",
    "pt_pt": "pt-pt",
    "pt_br": "pt-br",
    "en_gb": "en-gb",
    "fr_fr": "fr-fr",
    "fr_ca": "fr-ca",
    "de_de": "de-de",
    "it_it": "it-it",
    "es_es": "es-es",
    "es_419": "es-419",
    "pl_pl": "pl-pl",
    "ro_ro": "ro-ro",
    "sv_se": "sv-se",
}


def norm(s: str | None) -> str:
    """Strip and normalise a cell value; return '' for blanks."""
    if s is None:
        return ""
    return str(s).strip()


# ---------------------------------------------------------------------------
# 1. Fetch remote data
# ---------------------------------------------------------------------------

def fetch_csv() -> list[list[str]]:
    """Download the Google Sheet as CSV and return rows."""
    req = urllib.request.Request(EXPORT_URL, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=60)
    data = resp.read().decode("utf-8-sig")
    return list(csv.reader(io.StringIO(data)))


def parse_remote(rows: list[list[str]]) -> dict[str, list[dict]]:
    """Parse CSV rows into {lang_code: [term_dict, ...]}."""
    header = [h.strip().lower() for h in rows[0]]

    # Detect column indices for shared columns
    en_col = None
    def_col = None
    ctx_col = None
    pos_col = None
    notes_col = None
    remarks_col = None  # first Remarks after en_US (general remarks)
    dnt_col = None

    # lang_col: {lang_code: column_index}
    lang_cols: dict[str, int] = {}
    # Some lang columns are followed by a "Remarks" column
    lang_remarks: dict[str, int] = {}

    last_lang = None
    for i, h in enumerate(header):
        if h == "en_us":
            en_col = i
            continue
        if h == "definition":
            def_col = i
            continue
        if h == "context":
            ctx_col = i
            continue
        if h == "part of speech":
            pos_col = i
            continue
        if h == "notes":
            notes_col = i
            continue
        if h == "reference":
            continue
        if h == "dnt":
            dnt_col = i
            continue
        if h == "remarks":
            # Associate this Remarks with the preceding language column
            if last_lang:
                lang_remarks[last_lang] = i
            elif remarks_col is None:
                remarks_col = i
            continue

        # Check if this is a language column
        mapped = HEADER_TO_LANG.get(h)
        if mapped:
            lang_cols[mapped] = i
            last_lang = mapped

    if en_col is None:
        raise ValueError("Cannot find 'en_US' column in sheet header")

    result: dict[str, list[dict]] = {lang: [] for lang in lang_cols}

    for row in rows[1:]:
        def cell(idx):
            if idx is None or idx >= len(row):
                return ""
            return norm(row[idx])

        source = cell(en_col)
        if not source:
            continue

        definition = cell(def_col)
        context = cell(ctx_col)
        part_of_speech = cell(pos_col)
        notes_val = cell(notes_col)
        general_remarks = cell(remarks_col)
        dnt_val = cell(dnt_col)

        # Combine notes + general remarks + DNT info
        notes_parts = []
        if notes_val:
            notes_parts.append(notes_val)
        if general_remarks:
            notes_parts.append(general_remarks)
        if dnt_val:
            notes_parts.append(f"DNT: {dnt_val}")
        combined_notes = "; ".join(notes_parts) if notes_parts else None

        for lang, col_idx in lang_cols.items():
            target = cell(col_idx)
            if not target:
                continue

            # Per-language remarks
            lang_remark = cell(lang_remarks.get(lang))

            entry_notes = combined_notes
            if lang_remark:
                entry_notes = f"{entry_notes}; {lang_remark}" if entry_notes else lang_remark

            result[lang].append({
                "source_term": source,
                "target_term": target,
                "part_of_speech": part_of_speech or None,
                "definition": definition or None,
                "context": context or None,
                "notes": entry_notes,
            })

    return result


# ---------------------------------------------------------------------------
# 2. Load local data
# ---------------------------------------------------------------------------

def load_local(lang: str) -> dict:
    """Load existing local JSON for a language. Returns the full dict."""
    path = OUT_DIR / f"{lang}.json"
    if not path.exists():
        return {"language": lang, "total_terms": 0, "terms": []}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 3. Diff & merge
# ---------------------------------------------------------------------------

def diff_and_merge(
    local_data: dict,
    remote_terms: list[dict],
    lang: str,
) -> tuple[dict, dict]:
    """
    Merge remote terms into local data. Returns (updated_data, diff_stats).

    Strategy:
    - Build lookup by source_term (case-insensitive) from local.
    - For each remote term:
      - If source_term exists locally: check if target_term or metadata changed → update.
      - If source_term is new: add as new entry.
    - Track removed: source_terms in local that came from a previous online fetch
      (identified by source="online") but are no longer in remote.
    """
    local_terms = local_data.get("terms", [])

    # Index local terms by lowercase source_term
    local_by_src: dict[str, tuple[int, dict]] = {}
    for idx, t in enumerate(local_terms):
        key = t["source_term"].lower()
        local_by_src[key] = (idx, t)

    # Index remote terms
    remote_by_src: dict[str, dict] = {}
    for rt in remote_terms:
        key = rt["source_term"].lower()
        # If duplicate source_term in remote, keep last occurrence
        remote_by_src[key] = rt

    added = []
    modified = []
    removed = []
    unchanged = 0

    # Track which local entries are "online-sourced" for removal detection
    online_keys = set()
    for t in local_terms:
        if t.get("source") == "online":
            online_keys.add(t["source_term"].lower())

    # Process remote terms
    updated_local = list(local_terms)
    updates_by_idx: dict[int, dict] = {}

    for key, rt in remote_by_src.items():
        if key in local_by_src:
            idx, existing = local_by_src[key]
            # Check if anything changed
            changes = {}
            if norm(existing.get("target_term", "")) != norm(rt["target_term"]):
                changes["target_term"] = (existing.get("target_term"), rt["target_term"])
            if norm(existing.get("definition") or "") != norm(rt.get("definition") or ""):
                changes["definition"] = (existing.get("definition"), rt.get("definition"))
            if norm(existing.get("context") or "") != norm(rt.get("context") or ""):
                changes["context"] = (existing.get("context"), rt.get("context"))
            if norm(existing.get("part_of_speech") or "") != norm(rt.get("part_of_speech") or ""):
                changes["part_of_speech"] = (existing.get("part_of_speech"), rt.get("part_of_speech"))

            if changes:
                modified.append({"source_term": rt["source_term"], "changes": changes})
                # Apply updates
                merged = dict(existing)
                merged["target_term"] = rt["target_term"]
                if rt.get("definition"):
                    merged["definition"] = rt["definition"]
                if rt.get("context"):
                    merged["context"] = rt["context"]
                if rt.get("part_of_speech"):
                    merged["part_of_speech"] = rt["part_of_speech"]
                if rt.get("notes"):
                    merged["notes"] = rt["notes"]
                merged["source"] = "online"
                merged["last_mod_date"] = date.today().isoformat()
                updates_by_idx[idx] = merged
            else:
                unchanged += 1
        else:
            # New term from online
            added.append(rt["source_term"])

    # Detect removed: previously online-sourced terms no longer in remote
    for key in online_keys:
        if key not in remote_by_src:
            removed.append(key)

    # Apply updates in-place
    for idx, merged in updates_by_idx.items():
        updated_local[idx] = merged

    # Remove deleted online-sourced terms
    if removed:
        removed_set = set(removed)
        updated_local = [
            t for t in updated_local
            if not (t.get("source") == "online" and t["source_term"].lower() in removed_set)
        ]

    # Append new terms
    next_id = len(updated_local) + 1
    lang_upper = lang.upper()
    for key, rt in remote_by_src.items():
        if key not in local_by_src:
            updated_local.append({
                "term_id": f"{lang_upper}_{next_id:05d}",
                "language": lang,
                "source_term": rt["source_term"],
                "target_term": rt["target_term"],
                "part_of_speech": rt.get("part_of_speech"),
                "definition": rt.get("definition"),
                "context": rt.get("context"),
                "status": None,
                "notes": rt.get("notes"),
                "last_mod_date": date.today().isoformat(),
                "source": "online",
            })
            next_id += 1

    # Re-index term_ids
    for idx, t in enumerate(updated_local, start=1):
        t["term_id"] = f"{lang_upper}_{idx:05d}"
        t["language"] = lang

    # Build output
    output = {
        "language": lang,
        "total_terms": len(updated_local),
        "terms": updated_local,
    }

    stats = {
        "added": len(added),
        "modified": len(modified),
        "removed": len(removed),
        "unchanged": unchanged,
        "total_after": len(updated_local),
        "added_terms": added[:20],  # sample for display
        "modified_details": modified[:10],
        "removed_terms": removed[:20],
    }

    return output, stats


# ---------------------------------------------------------------------------
# 4. Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetch and update glossary terms from Google Sheets")
    parser.add_argument("--dry-run", action="store_true", help="Show diff only, don't write files")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Fetch
    print("Fetching terminology from Google Sheets...")
    rows = fetch_csv()
    print(f"  Downloaded {len(rows) - 1} rows (+ header)")

    # Parse
    remote_data = parse_remote(rows)
    print(f"  Languages found: {', '.join(sorted(remote_data.keys()))}")
    for lang in sorted(remote_data):
        print(f"    {lang}: {len(remote_data[lang])} terms")

    # Diff & merge per language
    print("\n--- Diff Report ---")
    all_stats = {}

    for lang in sorted(remote_data.keys()):
        remote_terms = remote_data[lang]
        if not remote_terms:
            continue

        local_data = load_local(lang)
        updated, stats = diff_and_merge(local_data, remote_terms, lang)
        all_stats[lang] = stats

        symbol = ""
        if stats["added"] or stats["modified"] or stats["removed"]:
            symbol = " *"
        print(f"\n  [{lang}]{symbol}")
        print(f"    Local before: {local_data['total_terms']}  |  Remote: {len(remote_terms)}  |  After merge: {stats['total_after']}")
        print(f"    Added: {stats['added']}  |  Modified: {stats['modified']}  |  Removed: {stats['removed']}  |  Unchanged: {stats['unchanged']}")

        if stats["added_terms"]:
            sample = stats["added_terms"][:5]
            more = f" ... +{stats['added'] - 5} more" if stats["added"] > 5 else ""
            print(f"    + New: {', '.join(sample)}{more}")

        if stats["modified_details"]:
            for m in stats["modified_details"][:3]:
                parts = []
                for field, (old, new) in m["changes"].items():
                    parts.append(f"{field}: {repr(old)[:40]} -> {repr(new)[:40]}")
                print(f"    ~ {m['source_term']}: {'; '.join(parts)}")

        if stats["removed_terms"]:
            sample = stats["removed_terms"][:5]
            print(f"    - Removed: {', '.join(sample)}")

        if not args.dry_run:
            out_path = OUT_DIR / f"{lang}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(updated, f, ensure_ascii=False, indent=2)

    # Update summary
    if not args.dry_run:
        summary_data = {}
        for lang_file in sorted(OUT_DIR.glob("*.json")):
            if lang_file.name.startswith("_"):
                continue
            with open(lang_file, encoding="utf-8") as f:
                d = json.load(f)
            summary_data[d["language"]] = {"total_terms": d["total_terms"]}

        summary = {
            "total_languages": len(summary_data),
            "last_fetched": date.today().isoformat(),
            "languages": summary_data,
        }
        with open(OUT_DIR / "_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    # Final summary
    total_added = sum(s["added"] for s in all_stats.values())
    total_modified = sum(s["modified"] for s in all_stats.values())
    total_removed = sum(s["removed"] for s in all_stats.values())

    print(f"\n--- Summary ---")
    print(f"  Total added:    {total_added}")
    print(f"  Total modified: {total_modified}")
    print(f"  Total removed:  {total_removed}")
    if args.dry_run:
        print("  (dry-run mode — no files written)")
    else:
        print(f"  Files updated in: {OUT_DIR}")


if __name__ == "__main__":
    main()
