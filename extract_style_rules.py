#!/usr/bin/env python3
"""Extract localization rules from RingCentral Style Guide PDFs.

Uses pdfplumber for text extraction and Anthropic Claude API for
semantic parsing into structured JSON rules.
"""

import json
import os
import re
import time
from pathlib import Path

import pdfplumber
from dotenv import load_dotenv
from json_repair import repair_json

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(override=True)
# Also try loading from parent directories if key not found
if not os.environ.get("ANTHROPIC_API_KEY"):
    for p in BASE_DIR.parents:
        env_path = p / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
            if os.environ.get("ANTHROPIC_API_KEY"):
                break

import anthropic

# ── Config ────────────────────────────────────────────────────────────────────
STYLE_GUIDE_DIR = BASE_DIR / "_Style Guide"
OUTPUT_DIR = BASE_DIR / "output" / "styleguides"
MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 16384

SYSTEM_PROMPT = (
    "You are a localization quality expert. Extract every distinct linguistic "
    "rule from this RingCentral Style Guide into a JSON array. Each atomic "
    "rule = one object. No omissions. Return ONLY valid JSON, no markdown "
    "fences, no preamble."
)

USER_PROMPT_TEMPLATE = """\
Language: {lang_code}
Source file: {filename}

Extract every rule into a JSON array. Each rule object must have these fields:
{{
  "rule_id": "{lang_code}_RULE_<5-digit-index starting from 00001>",
  "language": "{lang_code}",
  "category": "<one of: style_tone|punctuation|spelling|grammar|terminology|formatting|numbers|abbreviations|trademarks|ui_elements|localization_general>",
  "section": "<chapter/section title from the guide>",
  "description": "<precise description of the rule>",
  "example_correct": "<correct example if provided, else empty string>",
  "example_incorrect": "<incorrect example if provided, else empty string>",
  "notes": "<any additional notes, else empty string>"
}}

Rules:
- Every distinct guideline, rule, or recommendation = one JSON object.
- If a section contains sub-rules, split each into a separate object.
- Include both correct and incorrect examples where the guide provides them.
- Use "" for fields with no data.

Full style guide text:

{text}"""

CONTINUATION_PROMPT = """\
You were extracting rules but the response was cut off. Continue extracting \
the remaining rules from where you left off. Return ONLY a JSON array of the \
remaining rule objects (no preamble, no markdown fences). \
Continue rule_id numbering from {lang_code}_RULE_{next_idx:05d}."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_lang_code(filename: str) -> str | None:
    """Extract language code (e.g. ZH-CN, DE-DE) from filename."""
    match = re.search(r'[_\s]([A-Z]{2}-[A-Z]{2})[_\s]', filename)
    return match.group(1) if match else None


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from a PDF, filtering out headers and footers."""
    pages_text = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            raw = page.extract_text()
            if not raw:
                continue
            filtered_lines = []
            for line in raw.split('\n'):
                stripped = line.strip()
                if not stripped:
                    continue
                # Skip standalone page numbers
                if re.match(r'^\d{1,3}$', stripped):
                    continue
                # Skip repeated header title
                if re.match(
                    r'^RingCentral\s+Localization\s+Style\s+Guide$',
                    stripped,
                    re.IGNORECASE,
                ):
                    continue
                filtered_lines.append(line)
            if filtered_lines:
                pages_text.append('\n'.join(filtered_lines))
    return '\n\n'.join(pages_text)


def _strip_markdown_fences(text: str) -> str:
    """Remove markdown code fences wrapping JSON."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r'^```\w*\s*\n?', '', text)
        text = re.sub(r'\n?```\s*$', '', text)
    return text.strip()


def _salvage_complete_objects(text: str) -> str:
    """If JSON array is truncated, salvage all complete objects and close it."""
    # Find the last complete object (ending with })
    last_brace = text.rfind('}')
    if last_brace == -1:
        return text
    # Walk forward from last_brace to include any trailing comma/whitespace
    truncated = text[:last_brace + 1].rstrip()
    # Remove trailing comma if present
    if truncated.endswith(','):
        truncated = truncated[:-1]
    # If text starts with [ but doesn't end with ], close it
    stripped = truncated.strip()
    if stripped.startswith('[') and not stripped.endswith(']'):
        truncated = truncated + '\n]'
    return truncated


def _try_loads(text: str) -> list | None:
    """Try to json.loads text and return list of rules or None."""
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and 'rules' in result:
            return result['rules']
        return [result]
    except (json.JSONDecodeError, ValueError):
        return None


def parse_json_array(raw: str) -> list | None:
    """Try to parse a JSON array from raw API response text."""
    cleaned = _strip_markdown_fences(raw)

    # 1. Direct parse (fast path)
    result = _try_loads(cleaned)
    if result is not None:
        return result

    # 2. Try salvaging truncated JSON (close incomplete array)
    salvaged = _salvage_complete_objects(cleaned)
    result = _try_loads(salvaged)
    if result is not None:
        return result

    # 3. Use json-repair for malformed JSON (unescaped quotes, etc.)
    for text in (salvaged, cleaned):
        try:
            repaired = repair_json(text, return_objects=True)
            if isinstance(repaired, list) and repaired:
                return repaired
            if isinstance(repaired, dict) and 'rules' in repaired:
                return repaired['rules']
        except Exception:
            pass

    return None


def call_api(client: anthropic.Anthropic, lang_code: str, filename: str,
             text: str) -> tuple[list | None, str | None]:
    """Call Claude API, handling continuation if response is truncated.

    Returns (rules_list, raw_text_on_failure).
    """
    all_rules: list = []
    all_raw: list[str] = []
    messages = [{
        "role": "user",
        "content": USER_PROMPT_TEMPLATE.format(
            lang_code=lang_code, filename=filename, text=text
        ),
    }]

    for attempt in range(5):  # max 5 continuation rounds
        with client.messages.stream(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=messages,
        ) as stream:
            response = stream.get_final_message()

        raw_text = response.content[0].text
        all_raw.append(raw_text)
        truncated = response.stop_reason == "max_tokens"

        rules = parse_json_array(raw_text)

        if rules is not None:
            all_rules.extend(rules)

        if not truncated:
            break  # complete response

        # Truncated → ask for continuation regardless of parse success
        n_so_far = len(all_rules)
        print(f"    Truncated (parsed {len(rules) if rules else 0} rules "
              f"this chunk, {n_so_far} total), requesting more...")
        messages.append({"role": "assistant", "content": raw_text})
        messages.append({
            "role": "user",
            "content": CONTINUATION_PROMPT.format(
                lang_code=lang_code, next_idx=n_so_far + 1
            ),
        })
        time.sleep(1)

    if all_rules:
        return all_rules, None
    return None, '\n'.join(all_raw)


def normalize_rules(rules: list, lang_code: str) -> list:
    """Ensure every rule has the expected fields and sequential IDs."""
    expected_fields = [
        'category', 'section', 'description',
        'example_correct', 'example_incorrect', 'notes',
    ]
    for i, rule in enumerate(rules, 1):
        rule['rule_id'] = f"{lang_code}_RULE_{i:05d}"
        rule['language'] = lang_code
        for field in expected_fields:
            if field not in rule:
                rule[field] = ""
    return rules


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    client = anthropic.Anthropic()

    # Quick connectivity / credit check
    try:
        client.messages.create(
            model=MODEL, max_tokens=10,
            messages=[{"role": "user", "content": "Hi"}],
        )
    except anthropic.BadRequestError as e:
        if "credit balance" in str(e).lower():
            print("ERROR: Anthropic API credit balance is too low.")
            print("       Go to https://console.anthropic.com -> Plans & Billing")
            return
        raise
    except anthropic.AuthenticationError:
        print("ERROR: Invalid ANTHROPIC_API_KEY. Check your .env file.")
        return
    print("API connection OK\n")

    pdf_files = sorted(STYLE_GUIDE_DIR.glob("*.pdf"))
    print(f"Found {len(pdf_files)} PDF files in {STYLE_GUIDE_DIR}\n")

    if not pdf_files:
        print("ERROR: No PDF files found.")
        return

    summary: dict = {}

    for pdf_path in pdf_files:
        filename = pdf_path.name
        lang_code = extract_lang_code(filename)

        if not lang_code:
            print(f"SKIP  {filename}: cannot determine language code")
            continue

        output_path = OUTPUT_DIR / f"{lang_code}.json"

        # Skip already-processed languages
        if output_path.exists():
            print(f"SKIP  {lang_code}: {output_path.name} already exists")
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                summary[lang_code] = {
                    "total_rules": data["total_rules"],
                    "source": filename,
                }
            except (json.JSONDecodeError, KeyError):
                pass
            continue

        print(f"[{lang_code}] Processing {filename} ...")

        # 1. Extract text
        text = extract_pdf_text(pdf_path)
        print(f"  Text extracted: {len(text):,} chars")

        if len(text) < 100:
            print("  WARNING: Too little text, skipping")
            continue

        # 2. Call Claude API
        try:
            rules, raw_on_fail = call_api(client, lang_code, filename, text)
        except anthropic.APIError as e:
            print(f"  API ERROR: {e}")
            time.sleep(2)
            continue

        if rules is None:
            print("  ERROR: Could not parse JSON from response")
            raw_path = OUTPUT_DIR / f"{lang_code}_raw.txt"
            with open(raw_path, 'w', encoding='utf-8') as f:
                f.write(raw_on_fail or "")
            print(f"  Raw response saved to {raw_path.name}")
            time.sleep(1)
            continue

        # 3. Normalize and write
        rules = normalize_rules(rules, lang_code)

        output_data = {
            "language": lang_code,
            "source": filename,
            "total_rules": len(rules),
            "rules": rules,
        }

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"  -> {len(rules)} rules written to {output_path.name}")
        summary[lang_code] = {
            "total_rules": len(rules),
            "source": filename,
        }

        # Rate-limit delay
        time.sleep(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    summary_sorted = dict(sorted(summary.items()))
    summary_data = {
        "total_languages": len(summary_sorted),
        "total_rules": sum(v["total_rules"] for v in summary_sorted.values()),
        "languages": summary_sorted,
    }

    summary_path = OUTPUT_DIR / "_summary.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, ensure_ascii=False, indent=2)

    print(f"\n{'=' * 60}")
    print(f"DONE  {summary_data['total_languages']} languages | "
          f"{summary_data['total_rules']} total rules")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
