import sys
import re


def extract_failed_docids(log_path: str) -> list[str]:
    docids: set[str] = set()
    last_docid: str | None = None
    want_next_docid: bool = False

    # Patterns
    re_docid = re.compile(r"DocID\s*=\s*(\d+)")
    re_docid_alt = re.compile(r"(Document ID|docId)\s*[:=]\s*(\d+)")
    error_marker = "DocumentName field is required"

    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            # Track latest DocID reference in context
            m = re_docid.search(line)
            if not m:
                m = re_docid_alt.search(line)
            if m:
                last_docid = m.group(1) if m.lastindex == 1 else m.group(2)
                continue

            # On error line, capture last seen DocID or arm to capture the next DocID in following lines
            if error_marker in line:
                if last_docid:
                    docids.add(last_docid)
                # Also set a flag to capture the immediate next DocID reference
                want_next_docid = True
                continue

            # If we were waiting for the next DocID after an error, capture it now
            if want_next_docid and (m := (re_docid.search(line) or re_docid_alt.search(line))):
                captured = m.group(1) if m.lastindex == 1 else m.group(2)
                if captured:
                    docids.add(captured)
                want_next_docid = False

    return sorted(docids)


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_docids_from_log.py <path-to-log>")
        sys.exit(2)

    path = sys.argv[1]
    ids = extract_failed_docids(path)
    for did in ids:
        print(did)


if __name__ == "__main__":
    main()


