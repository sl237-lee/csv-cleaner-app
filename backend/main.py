from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from cleaner import clean_csv
import uuid
import os
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

latest_reports = {}

VALID_MISSING_VALUE_MODES = {"strict", "safe", "fill"}


@app.get("/")
def root():
    return {"message": "CSV Cleaner API is running"}


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    missing_value_mode: str = Form("strict"),
    duplicate_columns: str = Form("")
):
    if not (
        file.filename.lower().endswith(".csv")
        or file.filename.lower().endswith(".xlsx")
    ):
        raise HTTPException(
            status_code=400,
            detail="Only CSV and XLSX files are supported."
        )

    if missing_value_mode not in VALID_MISSING_VALUE_MODES:
        raise HTTPException(
            status_code=400,
            detail="missing_value_mode must be one of: strict, safe, fill"
        )

    parsed_duplicate_columns = None

    if duplicate_columns.strip():
        try:
            parsed_duplicate_columns = json.loads(duplicate_columns)

            if not isinstance(parsed_duplicate_columns, list):
                raise ValueError
        except Exception:
            parsed_duplicate_columns = [
                col.strip()
                for col in duplicate_columns.split(",")
                if col.strip()
            ]

    file_bytes = await file.read()

    try:
        (
            df,
            report,
            before_preview,
            after_preview,
            changed_cells_preview
        ) = clean_csv(
            file_bytes=file_bytes,
            filename=file.filename,
            missing_value_mode=missing_value_mode,
            duplicate_columns=parsed_duplicate_columns
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process file: {str(e)}"
        )

    file_id = str(uuid.uuid4())

    csv_path = os.path.join(OUTPUT_DIR, f"{file_id}_cleaned.csv")
    xlsx_path = os.path.join(OUTPUT_DIR, f"{file_id}_cleaned.xlsx")

    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    latest_reports[file_id] = {
        "report": report,
        "before_preview": before_preview,
        "after_preview": after_preview,
        "changed_cells_preview": changed_cells_preview
    }

    return {
        "file_id": file_id,
        "report": report,
        "before_preview": before_preview,
        "after_preview": after_preview,
        "changed_cells_preview": changed_cells_preview,
        "download_csv_url": f"/download/{file_id}?format=csv",
        "download_xlsx_url": f"/download/{file_id}?format=xlsx"
    }


@app.get("/download/{file_id}")
def download_file(file_id: str, format: str = "csv"):
    if format not in {"csv", "xlsx"}:
        raise HTTPException(
            status_code=400,
            detail="format must be csv or xlsx"
        )

    if format == "csv":
        output_path = os.path.join(OUTPUT_DIR, f"{file_id}_cleaned.csv")
        filename = "cleaned_data.csv"
        media_type = "text/csv"
    else:
        output_path = os.path.join(OUTPUT_DIR, f"{file_id}_cleaned.xlsx")
        filename = "cleaned_data.xlsx"
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        output_path,
        filename=filename,
        media_type=media_type
    )


@app.get("/report/{file_id}")
def get_report(file_id: str):
    result = latest_reports.get(file_id)

    if not result:
        raise HTTPException(status_code=404, detail="Report not found")

    return JSONResponse(content=result)