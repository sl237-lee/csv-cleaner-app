import re
from io import BytesIO

import pandas as pd


EMAIL_COLUMNS_HINTS = {"email", "email_address", "e_mail", "mail"}
CITY_COLUMNS_HINTS = {"city", "town", "location_city"}
NAME_COLUMNS_HINTS = {"name", "full_name", "customer_name"}
DATE_COLUMNS_HINTS = {"date", "time", "timestamp", "signup", "created", "updated"}
AMOUNT_COLUMNS_HINTS = {"amount", "price", "cost", "revenue", "sales", "payment", "total"}
ID_COLUMNS_HINTS = {"id", "record_id", "user_id", "customer_id"}


CITY_ALIASES = {
    "nyc": "New York",
    "new york city": "New York",
    "new york, ny": "New York",
    "n.y.c.": "New York",
    "la": "Los Angeles",
    "l.a.": "Los Angeles",
    "los angeles, ca": "Los Angeles",
    "sf": "San Francisco",
    "s.f.": "San Francisco",
    "san fran": "San Francisco",
    "san francisco, ca": "San Francisco",
    "chi": "Chicago",
    "chicago, il": "Chicago",
    "bos": "Boston",
    "boston, ma": "Boston",
}


EMAIL_DOMAIN_CORRECTIONS = {
    "gamil.com": "gmail.com",
    "gmial.com": "gmail.com",
    "gmai.com": "gmail.com",
    "gnail.com": "gmail.com",
    "hotnail.com": "hotmail.com",
    "hotmial.com": "hotmail.com",
    "outlok.com": "outlook.com",
    "outloo.com": "outlook.com",
    "yaho.com": "yahoo.com",
    "yhoo.com": "yahoo.com",
}


def load_csv_safely(file_bytes: bytes) -> pd.DataFrame:
    preview = pd.read_csv(BytesIO(file_bytes), header=None)

    first_row = preview.iloc[0].fillna("").astype(str).tolist()
    second_row = preview.iloc[1].fillna("").astype(str).tolist()

    first_row_nonempty = sum(1 for x in first_row if x.strip() != "")
    second_row_nonempty = sum(1 for x in second_row if x.strip() != "")

    first_row_numeric_ratio = sum(
        1 for x in first_row if x.strip() != "" and x.replace(".", "", 1).isdigit()
    ) / max(first_row_nonempty, 1)

    second_row_alpha_ratio = sum(
        1 for x in second_row if any(c.isalpha() for c in x)
    ) / max(second_row_nonempty, 1)

    if first_row_numeric_ratio > 0.5 and second_row_alpha_ratio > 0.5:
        df = pd.read_csv(BytesIO(file_bytes), skiprows=1)
    else:
        df = pd.read_csv(BytesIO(file_bytes))

    return df


def load_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    lower = filename.lower()

    if lower.endswith(".csv"):
        return load_csv_safely(file_bytes)

    if lower.endswith(".xlsx"):
        return pd.read_excel(BytesIO(file_bytes))

    raise ValueError("Unsupported file format. Only CSV and XLSX are supported.")


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.replace(r"\s+", "_", regex=True)
    )
    return df


def is_missing_like(value) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip().lower() in {"", "nan", "none", "null", "n/a", "na"}:
        return True
    return False


def normalize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
        df[col] = df[col].replace(
            ["", "nan", "None", "null", "NULL", "N/A", "n/a", "NA"],
            pd.NA
        )
    return df


def infer_column_roles(df: pd.DataFrame) -> dict:
    roles = {
        "email": [],
        "city": [],
        "name": [],
        "date": [],
        "amount": [],
        "id": [],
    }

    for col in df.columns:
        lower = str(col).lower()

        if lower in EMAIL_COLUMNS_HINTS or "email" in lower:
            roles["email"].append(col)
        if lower in CITY_COLUMNS_HINTS or "city" in lower:
            roles["city"].append(col)
        if lower in NAME_COLUMNS_HINTS or lower.endswith("_name"):
            roles["name"].append(col)
        if any(token in lower for token in DATE_COLUMNS_HINTS):
            roles["date"].append(col)
        if any(token in lower for token in AMOUNT_COLUMNS_HINTS):
            roles["amount"].append(col)
        if lower in ID_COLUMNS_HINTS or lower.endswith("_id"):
            roles["id"].append(col)

    return roles


def fix_email(value):
    if is_missing_like(value):
        return pd.NA

    text = str(value).strip().lower().replace(" ", "")
    text = text.replace("(at)", "@").replace("[at]", "@")
    text = text.replace("(dot)", ".").replace("[dot]", ".")

    if text.count("@") != 1:
        return text

    local, domain = text.split("@", 1)
    domain = EMAIL_DOMAIN_CORRECTIONS.get(domain, domain)
    return f"{local}@{domain}"


def standardize_city(value):
    if is_missing_like(value):
        return pd.NA

    raw = str(value).strip()
    key = raw.lower()
    if key in CITY_ALIASES:
        return CITY_ALIASES[key]

    words = re.split(r"\s+", raw)
    return " ".join(word.capitalize() for word in words)


def standardize_name(value):
    if is_missing_like(value):
        return pd.NA

    raw = str(value).strip()
    raw = re.sub(r"\s+", " ", raw)
    return raw.title()


def normalize_date_columns(df: pd.DataFrame, roles: dict) -> tuple[pd.DataFrame, list[str]]:
    normalized_date_columns = []

    common_date_formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y.%m.%d",
        "%b %d %Y",
        "%B %d %Y",
    ]

    def parse_single_date(value):
        if pd.isna(value):
            return pd.NaT

        text = str(value).strip()
        if text == "":
            return pd.NaT

        # First try pandas flexible parser on the single value
        parsed = pd.to_datetime(text, errors="coerce")
        if not pd.isna(parsed):
            return parsed

        # Then try explicit known formats one by one
        for fmt in common_date_formats:
            parsed = pd.to_datetime(text, format=fmt, errors="coerce")
            if not pd.isna(parsed):
                return parsed

        return pd.NaT

    for col in df.columns:
        if col not in roles["date"] and not any(token in str(col).lower() for token in DATE_COLUMNS_HINTS):
            continue

        parsed_series = df[col].map(parse_single_date)

        if parsed_series.notna().sum() > 0:
            df[col] = parsed_series.dt.strftime("%Y-%m-%d")
            df[col] = df[col].where(parsed_series.notna(), pd.NA)
            normalized_date_columns.append(col)

    return df, normalized_date_columns


def detect_numeric_columns(df: pd.DataFrame, roles: dict) -> list[str]:
    numeric_columns = []

    for col in df.columns:
        if col in roles["id"]:
            continue

        converted = pd.to_numeric(df[col], errors="coerce")
        non_null_count = df[col].notna().sum()

        if non_null_count == 0:
            continue

        success_ratio = converted.notna().sum() / non_null_count
        likely_amount = col in roles["amount"]

        if success_ratio >= 0.8 or likely_amount:
            numeric_columns.append(col)

    return numeric_columns


def normalize_numeric_columns(df: pd.DataFrame, roles: dict) -> tuple[pd.DataFrame, list[str]]:
    normalized_numeric_columns = detect_numeric_columns(df, roles)

    for col in normalized_numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df, normalized_numeric_columns


def infer_domain_profile(df: pd.DataFrame, roles: dict) -> str:
    cols = set(df.columns)

    if {"email", "signup_date", "purchase_amount", "city"} & cols:
        return "customer_growth"

    if any(col in roles["amount"] for col in df.columns):
        return "commerce"

    if any(col in roles["email"] for col in df.columns):
        return "crm"

    return "generic"


def apply_domain_rules(
    df: pd.DataFrame,
    roles: dict,
    domain_profile: str
) -> tuple[pd.DataFrame, list[str]]:
    applied_rules = []

    for col in roles["email"]:
        before = df[col].copy()
        df[col] = df[col].apply(fix_email)
        if not before.equals(df[col]):
            applied_rules.append(f"email_normalization:{col}")

    for col in roles["city"]:
        before = df[col].copy()
        df[col] = df[col].apply(standardize_city)
        if not before.equals(df[col]):
            applied_rules.append(f"city_standardization:{col}")

    for col in roles["name"]:
        before = df[col].copy()
        df[col] = df[col].apply(standardize_name)
        if not before.equals(df[col]):
            applied_rules.append(f"name_standardization:{col}")

    if domain_profile in {"commerce", "customer_growth"}:
        for col in roles["amount"]:
            if col in df.columns:
                before = df[col].copy()
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df.loc[df[col] < 0, col] = pd.NA
                if not before.equals(df[col]):
                    applied_rules.append(f"amount_normalization:{col}")

    return df, applied_rules


def handle_missing_values(
    df: pd.DataFrame,
    mode: str = "strict"
) -> tuple[pd.DataFrame, int, dict]:
    missing_before = df.isna().sum().to_dict()
    rows_removed_with_missing_values = 0

    if mode == "strict":
        before_dropna = len(df)
        df = df.dropna()
        rows_removed_with_missing_values = before_dropna - len(df)

    elif mode == "safe":
        pass

    elif mode == "fill":
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
            else:
                df[col] = df[col].fillna("unknown")

    else:
        raise ValueError("missing_value_mode must be one of: strict, safe, fill")

    return df, rows_removed_with_missing_values, missing_before


def resolve_duplicate_columns(df: pd.DataFrame, duplicate_columns=None) -> list[str]:
    if duplicate_columns:
        valid = [col for col in duplicate_columns if col in df.columns]
        if valid:
            return valid

    return [col for col in df.columns if col != "id"]


def remove_duplicates(
    df: pd.DataFrame,
    duplicate_columns=None
) -> tuple[pd.DataFrame, int, list[str]]:
    dedup_columns = resolve_duplicate_columns(df, duplicate_columns)

    before_dedup = len(df)
    df = df.drop_duplicates(subset=dedup_columns)
    duplicates_removed = before_dedup - len(df)

    return df, duplicates_removed, dedup_columns


def serialize_value(value):
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return str(value) if not isinstance(value, (int, float, bool)) else value


def make_preview(df: pd.DataFrame, limit: int = 5) -> list[dict]:
    preview_df = df.head(limit).copy()

    for col in preview_df.columns:
        preview_df[col] = preview_df[col].map(serialize_value)

    return preview_df.to_dict(orient="records")


def build_changed_cells_preview(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    limit_rows: int = 5
) -> list[dict]:
    shared_rows = min(len(before_df), len(after_df), limit_rows)
    shared_cols = [col for col in before_df.columns if col in after_df.columns]

    changes = []

    for row_idx in range(shared_rows):
        for col in shared_cols:
            before_val = serialize_value(before_df.iloc[row_idx][col])
            after_val = serialize_value(after_df.iloc[row_idx][col])

            if str(before_val) != str(after_val):
                changes.append({
                    "row_index": row_idx,
                    "column": col,
                    "before": before_val,
                    "after": after_val,
                })

    return changes


def summarize_cell_changes(before_df: pd.DataFrame, after_df: pd.DataFrame) -> dict:
    shared_rows = min(len(before_df), len(after_df))
    shared_cols = [col for col in before_df.columns if col in after_df.columns]

    total_changed_cells = 0
    changed_by_column = {}

    for col in shared_cols:
        changed_count = 0
        for row_idx in range(shared_rows):
            before_val = serialize_value(before_df.iloc[row_idx][col])
            after_val = serialize_value(after_df.iloc[row_idx][col])

            if str(before_val) != str(after_val):
                changed_count += 1

        if changed_count > 0:
            changed_by_column[col] = changed_count
            total_changed_cells += changed_count

    return {
        "total_changed_cells_in_preview_window": total_changed_cells,
        "changed_cells_by_column_in_preview_window": changed_by_column,
    }


def build_report(
    original_rows: int,
    df: pd.DataFrame,
    rows_removed_with_missing_values: int,
    duplicates_removed: int,
    dedup_columns: list[str],
    normalized_date_columns: list[str],
    normalized_numeric_columns: list[str],
    missing_before: dict,
    missing_after: dict,
    missing_value_mode: str,
    domain_profile: str,
    applied_rules: list[str],
    change_summary: dict
) -> dict:
    return {
        "original_rows": original_rows,
        "cleaned_rows": len(df),
        "missing_value_mode": missing_value_mode,
        "rows_removed_with_missing_values": rows_removed_with_missing_values,
        "duplicates_removed": duplicates_removed,
        "duplicate_check_columns": dedup_columns,
        "normalized_date_columns": normalized_date_columns,
        "normalized_numeric_columns": normalized_numeric_columns,
        "inferred_domain_profile": domain_profile,
        "applied_business_rules": applied_rules,
        "missing_values_by_column_before_cleaning": missing_before,
        "missing_values_by_column_after_cleaning": missing_after,
        "change_summary": change_summary,
        "columns": list(df.columns),
    }


def clean_csv(
    file_bytes: bytes,
    filename: str,
    missing_value_mode: str = "strict",
    duplicate_columns=None
):
    df = load_file(file_bytes, filename)
    original_rows = len(df)

    df = standardize_column_names(df)
    parsed_df = df.copy()
    before_preview = make_preview(parsed_df)

    df = normalize_text_columns(df)

    roles = infer_column_roles(df)
    domain_profile = infer_domain_profile(df, roles)

    df, applied_rules = apply_domain_rules(df, roles, domain_profile)
    df, normalized_date_columns = normalize_date_columns(df, roles)
    df, normalized_numeric_columns = normalize_numeric_columns(df, roles)

    cleaned_before_drop_and_dedup = df.copy()

    df, rows_removed_with_missing_values, missing_before = handle_missing_values(
        df,
        mode=missing_value_mode
    )

    df, duplicates_removed, dedup_columns = remove_duplicates(
        df,
        duplicate_columns=duplicate_columns
    )

    missing_after = df.isna().sum().to_dict()
    after_preview = make_preview(df)

    changed_cells_preview = build_changed_cells_preview(
        parsed_df,
        cleaned_before_drop_and_dedup
    )

    change_summary = summarize_cell_changes(
        parsed_df,
        cleaned_before_drop_and_dedup
    )

    report = build_report(
        original_rows=original_rows,
        df=df,
        rows_removed_with_missing_values=rows_removed_with_missing_values,
        duplicates_removed=duplicates_removed,
        dedup_columns=dedup_columns,
        normalized_date_columns=normalized_date_columns,
        normalized_numeric_columns=normalized_numeric_columns,
        missing_before=missing_before,
        missing_after=missing_after,
        missing_value_mode=missing_value_mode,
        domain_profile=domain_profile,
        applied_rules=applied_rules,
        change_summary=change_summary,
    )

    return df, report, before_preview, after_preview, changed_cells_preview