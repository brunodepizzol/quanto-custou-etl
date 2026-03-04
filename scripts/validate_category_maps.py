import json
import re
import sys
from pathlib import Path


def validate_map(path: Path) -> list[str]:
    errors: list[str] = []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"{path}: JSON inválido ({exc})"]

    if not isinstance(obj, dict):
        return [f"{path}: raiz deve ser objeto JSON"]

    version = obj.get("version")
    rules = obj.get("rules")
    default = obj.get("default")

    if not isinstance(version, str) or not version.strip():
        errors.append(f"{path}: 'version' deve ser string não vazia")
    if not isinstance(default, str) or not default.strip():
        errors.append(f"{path}: 'default' deve ser string não vazia")
    if not isinstance(rules, list):
        errors.append(f"{path}: 'rules' deve ser array")
        return errors

    for i, rule in enumerate(rules):
        prefix = f"{path}: rules[{i}]"
        if not isinstance(rule, dict):
            errors.append(f"{prefix} deve ser objeto")
            continue
        match = rule.get("match")
        category = rule.get("category")
        if not isinstance(match, str) or not match.strip():
            errors.append(f"{prefix}.match deve ser string não vazia")
        if not isinstance(category, str) or not category.strip():
            errors.append(f"{prefix}.category deve ser string não vazia")
        if isinstance(match, str) and match.strip():
            try:
                re.compile(match, flags=re.IGNORECASE)
            except re.error as exc:
                errors.append(f"{prefix}.match regex inválida ({exc})")

    return errors


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    files = sorted(root.glob("mapping/**/category_map.json"))
    if not files:
        print("ERRO: nenhum mapping/**/category_map.json encontrado")
        return 1

    all_errors: list[str] = []
    for f in files:
        all_errors.extend(validate_map(f))

    if all_errors:
        print("Falha na validação de category_map.json:")
        for e in all_errors:
            print(f"- {e}")
        return 1

    print(f"OK: {len(files)} arquivo(s) de mapping validados.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
