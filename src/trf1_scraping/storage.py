import json
import io
from datetime import date
from pathlib import Path

from openpyxl import Workbook
from src.pgfn_storage import ObjectStorage, StorageLocation


COLUMNS = (
    "id_processo",
    "nr_processo",
    "nr_classe_judicial",
    "secao",
    "dt_autuacao",
    "link_acesso",
    "filtro",
)


def save_process_list(
    records: list[dict], report_date: date, report_dir: StorageLocation
) -> dict:
    """Persiste JSON e XLSX determinísticos, sem transportar a lista no XCom."""
    storage = ObjectStorage(report_dir)
    ordered = sorted(
        records,
        key=lambda item: (
            str(item.get("nr_processo", "")),
            str(item.get("id_processo", "")),
        ),
    )
    stem = f"lista-processos-trf1-{report_date.isoformat()}"
    json_key = f"{stem}.json"
    xlsx_key = f"{stem}.xlsx"
    json_content = (
        json.dumps(
            {
                "tribunal": "TRF1",
                "data": report_date.isoformat(),
                "total": len(ordered),
                "processos": ordered,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")
    json_path = storage.write_bytes(json_key, json_content)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Processos"
    sheet.append(COLUMNS)
    for record in ordered:
        sheet.append([record.get(column, "") for column in COLUMNS])
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    try:
        output = io.BytesIO()
        workbook.save(output)
        xlsx_path = storage.write_bytes(xlsx_key, output.getvalue())
    finally:
        workbook.close()

    return {
        "date": report_date.isoformat(),
        "total": len(ordered),
        "json_path": str(json_path),
        "xlsx_path": str(xlsx_path),
    }
