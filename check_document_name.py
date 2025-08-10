import sys
import json


def resolve_document_name(doc_id: str) -> dict:
    """Resolve document display name for a given Document ID using existing helper.

    Returns a dict: { success: bool, document_name: str, raw_status: str, raw_value_keys: list }
    """
    try:
        # Reuse the robust helper that already extracts display name / code
        from main import get_document_info

        info = get_document_info(str(doc_id).strip())
        result = {
            "success": bool(info.get("success")),
            "document_name": info.get("document_name") or "",
            "raw_status": info.get("status", ""),
            "raw_value_keys": list((info.get("document_type") or {}).keys()) if isinstance(info.get("document_type"), dict) else [],
        }
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


def main():
    if len(sys.argv) < 2:
        print("Usage: python check_document_name.py <DocumentID>")
        sys.exit(2)

    doc_id = sys.argv[1]
    out = resolve_document_name(doc_id)

    if out.get("success"):
        print(f"✅ Document ID: {doc_id}")
        print(f"   Document Name: {out.get('document_name')}")
        if out.get("raw_status"):
            print(f"   Raw Status: {out.get('raw_status')}")
        if out.get("raw_value_keys"):
            print(f"   Value Keys: {', '.join(out.get('raw_value_keys'))}")
        sys.exit(0)
    else:
        print(f"❌ Failed to resolve document name for {doc_id}")
        if out.get("error"):
            print(f"   Error: {out.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()


