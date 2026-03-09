from datetime import datetime, timedelta, timezone

from aggregate_insights_feed import aggregate_items


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def add_hours(dt: datetime, h: int) -> str:
    return (dt + timedelta(hours=h)).isoformat().replace("+00:00", "Z")


def main():
    base = datetime.now(timezone.utc)
    fixture = [
        {
            "sourceId": "federal/camara",
            "id": "dup-1",
            "enabled": True,
            "qualityScore": 0.8,
            "scoreFinal": 10.0,
            "freshUntil": add_hours(base, 12),
        },
        {
            "sourceId": "federal/camara",
            "id": "dup-1",
            "enabled": True,
            "qualityScore": 0.8,
            "scoreFinal": 20.0,
            "freshUntil": add_hours(base, 12),
        },
        {
            "sourceId": "federal/senado",
            "id": "expired-1",
            "enabled": True,
            "qualityScore": 0.9,
            "scoreFinal": 30.0,
            "freshUntil": add_hours(base, -1),
        },
        {
            "sourceId": "federal/senado",
            "id": "ok-1",
            "enabled": True,
            "qualityScore": 0.9,
            "scoreFinal": 15.0,
            "freshUntil": add_hours(base, 12),
        },
        {
            "sourceId": "municipal/prefeitura",
            "id": "low-quality",
            "enabled": True,
            "qualityScore": 0.5,
            "scoreFinal": 99.0,
            "freshUntil": add_hours(base, 12),
        },
    ]

    out, _dedup_removed = aggregate_items(fixture, max_items=2)
    ids = [f"{x.get('sourceId')}::{x.get('id')}" for x in out]
    assert len(out) == 2, f"esperado 2 itens, recebido {len(out)}"
    assert "federal/camara::dup-1" in ids, "dedupe nao preservou item com maior score"
    assert "federal/senado::expired-1" not in ids, "item expirado nao foi removido"
    assert "municipal/prefeitura::low-quality" not in ids, "item de baixa qualidade nao foi removido"
    assert out[0]["scoreFinal"] >= out[1]["scoreFinal"], "ordenacao por scoreFinal invalida"
    print(f"OK: test_aggregate_insights_logic ({len(out)} itens, now={now_iso()})")


if __name__ == "__main__":
    main()
