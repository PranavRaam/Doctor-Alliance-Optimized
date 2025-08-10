import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python batch_check_document_names.py <path-to-log>")
        sys.exit(2)

    log_path = sys.argv[1]

    # Extract IDs
    from extract_docids_from_log import extract_failed_docids
    doc_ids = extract_failed_docids(log_path)
    if not doc_ids:
        print("No DocIDs found for 'DocumentName field is required' in log.")
        return

    print(f"Found {len(doc_ids)} DocIDs with DocumentName errors:\n  " + ", ".join(doc_ids))

    # Resolve each document name
    from check_document_name import resolve_document_name

    successes = 0
    failures = 0
    for did in doc_ids:
        out = resolve_document_name(did)
        if out.get("success") and out.get("document_name"):
            print(f"✅ {did}: {out.get('document_name')}")
            successes += 1
        else:
            err = out.get("error") or "Could not resolve"
            print(f"❌ {did}: {err}")
            failures += 1

    print(f"\nSummary: {successes} resolved, {failures} unresolved")


if __name__ == "__main__":
    main()


