from __future__ import annotations

import json
import re
import shutil
import uuid
from pathlib import Path

HERE = Path(__file__).parent
PROJECT = "Stratum"
MODEL_DIR = HERE / f"{PROJECT}.SemanticModel"
REPORT_DIR = HERE / f"{PROJECT}.Report"
SERVER = "192.168.1.64:5434"
DATABASE = "warehouse"
MEASURES_TABLE = "_Measures"
SCHEMAS = "https://developer.microsoft.com/json-schemas/fabric"

TABLES: dict[str, list[tuple[str, str]]] = {
    "dim_customers": [("customer_id", "int"), ("email_hash", "str"), ("first_name", "str"), ("country_code", "str"), ("customer_segment", "str"), ("customer_since", "ts"), ("first_order_at", "ts"), ("last_order_at", "ts"), ("lifetime_orders", "int"), ("lifetime_revenue", "num"), ("lifecycle_stage", "str")],
    "dim_date": [("date_day", "date"), ("year_number", "int"), ("quarter_number", "int"), ("month_number", "int"), ("month_name", "str"), ("day_of_month", "int"), ("iso_day_of_week", "int"), ("day_name", "str"), ("week_start_date", "date"), ("month_start_date", "date"), ("is_weekend", "bool")],
    "dim_products": [("product_id", "int"), ("sku", "str"), ("product_name", "str"), ("category", "str"), ("current_unit_price", "num"), ("is_active", "bool"), ("created_at", "ts")],
    "fct_order_items": [("order_item_id", "int"), ("order_id", "int"), ("product_id", "int"), ("customer_id", "int"), ("order_date", "date"), ("order_status", "str"), ("channel", "str"), ("is_revenue", "bool"), ("quantity", "int"), ("unit_price", "num"), ("discount_amount", "num"), ("line_gross_amount", "num"), ("line_net_amount", "num"), ("item_updated_at", "ts")],
    "fct_orders": [("order_id", "int"), ("customer_id", "int"), ("order_date", "date"), ("ordered_at", "ts"), ("order_status", "str"), ("channel", "str"), ("currency", "str"), ("line_count", "int"), ("total_quantity", "int"), ("gross_amount", "num"), ("discount_amount", "num"), ("net_amount", "num"), ("is_revenue", "bool"), ("updated_at", "ts")],
    "fct_pipeline_rejections": [("pipeline_name", "str"), ("batch_id", "str"), ("run_date", "date"), ("rejection_reason", "str"), ("rejected_rows", "int")],
    "fct_pipeline_runs": [("pipeline_name", "str"), ("batch_id", "str"), ("run_date", "date"), ("started_at", "ts"), ("finished_at", "ts"), ("run_status", "str"), ("is_success", "bool"), ("ingest_status", "str"), ("load_status", "str"), ("extraction_mode", "str"), ("duration_seconds", "num"), ("rows_read", "int"), ("rows_valid", "int"), ("rows_rejected", "int"), ("dlq_rate", "num"), ("rows_loaded", "int"), ("estimated_cost_usd", "num"), ("error_message", "str")],
    "fct_sessions": [("session_id", "str"), ("anonymous_id", "str"), ("customer_id", "int"), ("session_started_at", "ts"), ("session_ended_at", "ts"), ("session_date", "date"), ("duration_seconds", "num"), ("event_count", "int"), ("page_views", "int"), ("searches", "int"), ("product_views", "int"), ("add_to_carts", "int"), ("checkouts", "int"), ("purchases", "int"), ("order_id", "int"), ("device", "str"), ("country_code", "str"), ("is_converted", "bool")],
    "fct_web_traffic_5min": [("window_start", "ts"), ("window_end", "ts"), ("traffic_date", "date"), ("event_type", "str"), ("device", "str"), ("event_count", "int"), ("unique_visitors", "int")],
}  # fmt: skip

HIDDEN = {
    "email_hash",
    "batch_id",
    "item_updated_at",
    "updated_at",
    "anonymous_id",
    "session_id",
}
SORT_BY = {
    ("dim_date", "month_name"): "month_number",
    ("dim_date", "day_name"): "iso_day_of_week",
}

RELATIONSHIPS = [
    ("fct_orders", "order_date", "dim_date", "date_day"),
    ("fct_order_items", "order_date", "dim_date", "date_day"),
    ("fct_sessions", "session_date", "dim_date", "date_day"),
    ("fct_web_traffic_5min", "traffic_date", "dim_date", "date_day"),
    ("fct_pipeline_runs", "run_date", "dim_date", "date_day"),
    ("fct_pipeline_rejections", "run_date", "dim_date", "date_day"),
    ("fct_orders", "customer_id", "dim_customers", "customer_id"),
    ("fct_order_items", "customer_id", "dim_customers", "customer_id"),
    ("fct_sessions", "customer_id", "dim_customers", "customer_id"),
    ("fct_order_items", "product_id", "dim_products", "product_id"),
]

EUR, PCT, INT, USD, USD2, DEC = (
    "#,0 €",
    "0.0%",
    "#,0",
    "\\$#,0.0000",
    "\\$#,0.00",
    "#,0.0",
)
MEASURE_FORMATS = {
    "Revenue": EUR, "Average Order Value": EUR, "Revenue Previous Month": EUR,
    "Product Revenue": EUR, "Average Lifetime Value": EUR,
    "Revenue MoM %": PCT, "Discount Rate": PCT, "Cancellation Rate": PCT,
    "Returning Customer Share": PCT, "Conversion Rate": PCT, "Cart Abandonment Rate": PCT,
    "Success Rate": PCT, "DLQ Rate": PCT,
    "Estimated Cost (USD)": USD, "Cost per Million Rows (USD)": USD,
    "Estimated Monthly Cost (USD)": USD2,
    "Avg Session Duration (min)": DEC, "Avg Run Duration (s)": DEC, "P95 Run Duration (s)": DEC,
}  # fmt: skip
MEASURE_FOLDERS = {
    "Sales": ["Revenue", "Orders", "Average Order Value", "Revenue Previous Month", "Revenue MoM %", "Active Customers", "Discount Rate", "Cancellation Rate", "Product Revenue", "Units Sold"],
    "Customers & web": ["Customers", "Returning Customer Share", "Average Lifetime Value", "Sessions", "Sessions With Product View", "Sessions With Cart", "Sessions With Checkout", "Converted Sessions", "Conversion Rate", "Cart Abandonment Rate", "Avg Session Duration (min)", "Visitors (5-min windows)"],
}  # fmt: skip

TMDL_TYPES = {
    "int": "int64",
    "num": "double",
    "str": "string",
    "bool": "boolean",
    "date": "dateTime",
    "ts": "dateTime",
}


def tag(*parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "stratum/" + "/".join(parts)))


def tmdl_name(name: str) -> str:
    return (
        name
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name)
        else "'" + name.replace("'", "''") + "'"
    )


def indent(text: str, tabs: int) -> str:
    return "\n".join(
        ("\t" * tabs + line) if line.strip() else "" for line in text.splitlines()
    )


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        content if content.endswith("\n") else content + "\n", encoding="utf-8"
    )


def write_json(path: Path, data: dict) -> None:
    write(path, json.dumps(data, indent=2, ensure_ascii=False))


def read_measures() -> dict[str, str]:
    text = (HERE / "measures.dax").read_text()
    body = text.split("EVALUATE")[0]
    parts = re.split(r"MEASURE\s+'_Measures'\[([^\]]+)\]\s*=", body)[1:]
    return {
        name: expr.strip() for name, expr in zip(parts[::2], parts[1::2], strict=True)
    }


def m_query(table: str, columns: list[tuple[str, str]]) -> str:
    numbers = [f'{{"{c}", type number}}' for c, t in columns if t == "num"]
    stamps = [f'{{"{c}", ToUtc, type datetime}}' for c, t in columns if t == "ts"]
    steps = [
        "Source = PostgreSQL.Database(WarehouseServer, WarehouseDatabase)",
        f'Data = Source{{[Schema = "marts", Item = "{table}"]}}[Data]',
    ]
    if stamps:
        steps.insert(
            0,
            "ToUtc = (value) => if value = null then null"
            " else if Value.Is(value, type datetimezone)"
            " then DateTimeZone.RemoveZone(DateTimeZone.ToUtc(value))"
            " else value",
        )
    last = "Data"
    if numbers:
        steps.append(
            f"Numbers = Table.TransformColumnTypes({last}, {{{', '.join(numbers)}}})"
        )
        last = "Numbers"
    if stamps:
        steps.append(f"Utc = Table.TransformColumns({last}, {{{', '.join(stamps)}}})")
        last = "Utc"
    lines = (
        ["let"]
        + [f"    {s}," for s in steps[:-1]]
        + [f"    {steps[-1]}", "in", f"    {last}"]
    )
    return "\n".join(lines)


def table_tmdl(table: str, columns: list[tuple[str, str]]) -> str:
    out = [f"table {table}", f"\tlineageTag: {tag(table)}"]
    if table == "dim_date":
        out.append("\tdataCategory: Time")
    for column, kind in columns:
        out += ["", f"\tcolumn {column}", f"\t\tdataType: {TMDL_TYPES[kind]}"]
        if table == "dim_date" and column == "date_day":
            out.append("\t\tisKey")
        if column in HIDDEN:
            out.append("\t\tisHidden")
        if kind == "date":
            out.append("\t\tformatString: yyyy-mm-dd")
        elif kind == "ts":
            out.append("\t\tformatString: yyyy-mm-dd hh:nn:ss")
        elif kind == "int":
            out.append("\t\tformatString: 0")
        out.append(f"\t\tlineageTag: {tag(table, column)}")
        aggregate = kind == "num" or (
            kind == "int" and not column.endswith(("_id", "_number"))
        )
        out.append(
            f"\t\tsummarizeBy: {'sum' if aggregate and table.startswith('fct') else 'none'}"
        )
        out.append(f"\t\tsourceColumn: {column}")
        if (table, column) in SORT_BY:
            out.append(f"\t\tsortByColumn: {SORT_BY[(table, column)]}")
        out += ["", "\t\tannotation SummarizationSetBy = Automatic"]
        if kind == "date":
            out += ["", "\t\tannotation UnderlyingDateTimeDataType = Date"]
    out += ["", f"\tpartition {table} = m", "\t\tmode: import", "\t\tsource ="]
    out.append(indent(m_query(table, columns), 4))
    out += ["", "\tannotation PBI_ResultType = Table"]
    return "\n".join(out)


def measures_tmdl(measures: dict[str, str]) -> str:
    folder_of = {m: f for f, names in MEASURE_FOLDERS.items() for m in names}
    out = [f"table {tmdl_name(MEASURES_TABLE)}", f"\tlineageTag: {tag(MEASURES_TABLE)}"]
    for name, expression in measures.items():
        out += [
            "",
            f"\tmeasure {tmdl_name(name)} = ```",
            indent(expression, 3),
            "\t\t\t```",
        ]
        out.append(f"\t\tformatString: {MEASURE_FORMATS.get(name, INT)}")
        out.append(f"\t\tdisplayFolder: {folder_of.get(name, 'Platform & FinOps')}")
        out.append(f"\t\tlineageTag: {tag(MEASURES_TABLE, name)}")
    out += [
        "",
        "\tcolumn Value",
        "\t\tisHidden",
        "\t\tformatString: 0",
        f"\t\tlineageTag: {tag(MEASURES_TABLE, 'Value')}",
        "\t\tsummarizeBy: none",
        "\t\tisNameInferred",
        "\t\tsourceColumn: [Value]",
        "",
        "\t\tannotation SummarizationSetBy = Automatic",
        "",
        f"\tpartition {tmdl_name(MEASURES_TABLE)} = calculated",
        "\t\tmode: import",
        "\t\tsource = {0}",
    ]
    return "\n".join(out)


def build_model(measures: dict[str, str]) -> None:
    definition = MODEL_DIR / "definition"
    write_json(
        MODEL_DIR / "definition.pbism",
        {
            "$schema": f"{SCHEMAS}/item/semanticModel/definitionProperties/1.0.0/schema.json",
            "version": "4.1",
            "settings": {},
        },
    )
    write(definition / "database.tmdl", "database\n\tcompatibilityLevel: 1567")
    tables = [*TABLES, MEASURES_TABLE]
    query_order = json.dumps(["WarehouseServer", "WarehouseDatabase", *TABLES])
    model = [
        "model Model",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tdiscourageImplicitMeasures",
        "\tsourceQueryCulture: en-US",
        "\tdataAccessOptions",
        "\t\tlegacyRedirects",
        "\t\treturnErrorValuesAsNull",
        "",
        "annotation __PBI_TimeIntelligenceEnabled = 0",
        "",
        f"annotation PBI_QueryOrder = {query_order}",
        "",
        *[f"ref table {tmdl_name(t)}" for t in tables],
    ]
    write(definition / "model.tmdl", "\n".join(model))
    parameters = []
    for name, value in (("WarehouseServer", SERVER), ("WarehouseDatabase", DATABASE)):
        parameters += [
            f'expression {name} = "{value}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
            f"\tlineageTag: {tag('expression', name)}",
            "",
            "\tannotation PBI_ResultType = Text",
            "",
        ]
    write(definition / "expressions.tmdl", "\n".join(parameters))
    relationships = []
    for fact, fact_column, dim, dim_column in RELATIONSHIPS:
        relationships += [
            f"relationship {tag('rel', fact, fact_column, dim)}",
            f"\tfromColumn: {fact}.{fact_column}",
            f"\ttoColumn: {dim}.{dim_column}",
            "",
        ]
    write(definition / "relationships.tmdl", "\n".join(relationships))
    for table, columns in TABLES.items():
        write(definition / "tables" / f"{table}.tmdl", table_tmdl(table, columns))
    write(definition / "tables" / f"{MEASURES_TABLE}.tmdl", measures_tmdl(measures))


def field(entity: str, prop: str, kind: str) -> dict:
    return {kind: {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


LABELS = {
    "Revenue": "Chiffre d'affaires",
    "Revenue Previous Month": "Mois précédent",
    "Orders": "Commandes",
    "Average Order Value": "Panier moyen",
    "Cancellation Rate": "Taux d'annulation",
    "Product Revenue": "Chiffre d'affaires",
    "Units Sold": "Unités vendues",
    "Sessions": "Visites",
    "Sessions With Product View": "Produit consulté",
    "Sessions With Cart": "Ajout au panier",
    "Sessions With Checkout": "Paiement commencé",
    "Converted Sessions": "Achat",
    "Conversion Rate": "Taux de conversion",
    "Cart Abandonment Rate": "Abandon de panier",
    "Returning Customer Share": "Clients récurrents",
    "Customers": "Clients",
    "Visitors (5-min windows)": "Visiteurs",
    "Pipeline Runs": "Exécutions",
    "Success Rate": "Taux de succès",
    "DLQ Rate": "Taux de rejet (DLQ)",
    "Estimated Monthly Cost (USD)": "Coût mensuel estimé",
    "Rows Ingested": "Lignes ingérées",
    "Rejected Rows by Rule": "Lignes rejetées",
    "Failed Runs": "Échecs",
    "Avg Run Duration (s)": "Durée moyenne (s)",
    "P95 Run Duration (s)": "Durée P95 (s)",
    "Estimated Cost (USD)": "Coût estimé",
    "month_start_date": "Mois",
    "channel": "Canal",
    "category": "Catégorie",
    "product_name": "Produit",
    "device": "Appareil",
    "country_code": "Pays",
    "lifecycle_stage": "Cycle de vie",
    "window_start": "Fenêtre",
    "date_day": "Jour",
    "pipeline_name": "Pipeline",
    "rejection_reason": "Règle",
}


def projection(ref: str) -> dict:
    entity, prop = ref.split(".", 1)
    kind = "Measure" if entity == MEASURES_TABLE else "Column"
    result = {
        "field": field(entity, prop, kind),
        "queryRef": ref,
        "nativeQueryRef": prop,
    }
    if prop in LABELS:
        result["displayName"] = LABELS[prop]
    return result


def literal(value: str) -> dict:
    return {"expr": {"Literal": {"Value": value}}}


MARGIN, GAP, PAGE_WIDTH, PAGE_HEIGHT = 24, 16, 1280, 720
CONTENT = PAGE_WIDTH - 2 * MARGIN
WIDE, NARROW, HALF = 752, CONTENT - 752 - GAP, (CONTENT - GAP) // 2
ROW_KPI, ROW_MAIN, ROW_BOTTOM = (92, 92), (200, 248), (464, 232)


class Page:
    def __init__(self, name: str, title: str, subtitle: str) -> None:
        self.name, self.title, self.subtitle, self.visuals = name, title, subtitle, []
        self._header()

    def _append(self, visual: dict, box: tuple[int, int, int, int]) -> None:
        x, y, w, h = box
        index = len(self.visuals)
        self.visuals.append(
            {
                "$schema": f"{SCHEMAS}/item/report/definition/visualContainer/1.5.0/schema.json",
                "name": f"{self.name}_v{index:02d}",
                "position": {
                    "x": x,
                    "y": y,
                    "z": index * 1000,
                    "width": w,
                    "height": h,
                    "tabOrder": index * 1000,
                },
                "visual": visual,
            }
        )

    def add(
        self,
        visual_type: str,
        roles: dict[str, list[str]],
        box: tuple[int, int, int, int],
        title: str = "",
        sort_by: str | None = None,
        objects: dict | None = None,
    ) -> None:
        query: dict = {
            "queryState": {
                r: {"projections": [projection(f) for f in fields]}
                for r, fields in roles.items()
            }
        }
        if sort_by:
            entity, prop = sort_by.split(".", 1)
            kind = "Measure" if entity == MEASURES_TABLE else "Column"
            query["sortDefinition"] = {
                "sort": [
                    {"field": field(entity, prop, kind), "direction": "Descending"}
                ]
            }
        visual: dict = {"visualType": visual_type, "query": query}
        if objects:
            visual["objects"] = objects
        if title:
            visual["visualContainerObjects"] = {
                "title": [
                    {
                        "properties": {
                            "show": literal("true"),
                            "text": literal(f"'{title}'"),
                        }
                    }
                ]
            }
        self._append(visual, box)

    def _header(self) -> None:
        def run(text: str, size: str, color: str, bold: bool = False) -> dict:
            style = {"fontSize": size, "color": color, "fontFamily": "Segoe UI"}
            if bold:
                style["fontWeight"] = "bold"
            return {"textRuns": [{"value": text, "textStyle": style}]}

        paragraphs = [
            run(self.title, "20pt", "#111827", bold=True),
            run(self.subtitle, "10pt", "#6B7280"),
        ]
        self._append(
            {
                "visualType": "textbox",
                "objects": {"general": [{"properties": {"paragraphs": paragraphs}}]},
            },
            (MARGIN, 18, 820, 60),
        )
        self.add(
            "slicer",
            {"Values": ["dim_date.date_day"]},
            (PAGE_WIDTH - MARGIN - 320, 18, 320, 60),
            objects={"data": [{"properties": {"mode": literal("'Between'")}}]},
        )

    def kpis(self, measures: list[str]) -> None:
        y, h = ROW_KPI
        width = (CONTENT - GAP * (len(measures) - 1)) // len(measures)
        for i, measure in enumerate(measures):
            self.add(
                "card",
                {"Values": [f"{MEASURES_TABLE}.{measure}"]},
                (MARGIN + i * (width + GAP), y, width, h),
            )


def pages() -> list[Page]:
    m = MEASURES_TABLE
    main_y, main_h = ROW_MAIN
    bottom_y, bottom_h = ROW_BOTTOM
    left, right = MARGIN, MARGIN + WIDE + GAP
    half_right = MARGIN + HALF + GAP

    sales = Page(
        "sales",
        "Ventes",
        "Chiffre d'affaires des commandes payées, expédiées ou livrées",
    )
    sales.kpis(["Revenue", "Orders", "Average Order Value", "Cancellation Rate"])
    sales.add(
        "lineChart",
        {
            "Category": ["dim_date.month_start_date"],
            "Y": [f"{m}.Revenue", f"{m}.Revenue Previous Month"],
        },
        (left, main_y, WIDE, main_h),
        "Évolution mensuelle du chiffre d'affaires",
    )
    sales.add(
        "donutChart",
        {"Category": ["fct_orders.channel"], "Y": [f"{m}.Revenue"]},
        (right, main_y, NARROW, main_h),
        "Répartition par canal",
        sort_by=f"{m}.Revenue",
    )
    sales.add(
        "clusteredBarChart",
        {"Category": ["dim_products.category"], "Y": [f"{m}.Product Revenue"]},
        (left, bottom_y, HALF, bottom_h),
        "Chiffre d'affaires par catégorie",
        sort_by=f"{m}.Product Revenue",
    )
    sales.add(
        "tableEx",
        {
            "Values": [
                "dim_products.product_name",
                "dim_products.category",
                f"{m}.Product Revenue",
                f"{m}.Units Sold",
            ]
        },
        (half_right, bottom_y, HALF, bottom_h),
        "Meilleurs produits",
        sort_by=f"{m}.Product Revenue",
    )

    web = Page(
        "customers_web",
        "Clients & web",
        "Parcours de visite, conversion et fidélité des clients",
    )
    web.kpis(
        [
            "Sessions",
            "Conversion Rate",
            "Cart Abandonment Rate",
            "Returning Customer Share",
        ]
    )
    funnel = [
        f"{m}.{step}"
        for step in (
            "Sessions",
            "Sessions With Product View",
            "Sessions With Cart",
            "Sessions With Checkout",
            "Converted Sessions",
        )
    ]
    web.add(
        "clusteredBarChart",
        {"Y": funnel},
        (left, main_y, WIDE, main_h),
        "Entonnoir de conversion",
    )
    web.add(
        "clusteredColumnChart",
        {"Category": ["fct_sessions.device"], "Y": [f"{m}.Conversion Rate"]},
        (right, main_y, NARROW, main_h),
        "Conversion par appareil",
        sort_by=f"{m}.Conversion Rate",
    )
    web.add(
        "columnChart",
        {
            "Category": ["dim_customers.country_code"],
            "Series": ["dim_customers.lifecycle_stage"],
            "Y": [f"{m}.Customers"],
        },
        (left, bottom_y, HALF, bottom_h),
        "Clients par pays et cycle de vie",
        sort_by=f"{m}.Customers",
    )
    web.add(
        "lineChart",
        {
            "Category": ["fct_web_traffic_5min.window_start"],
            "Y": [f"{m}.Visitors (5-min windows)"],
        },
        (half_right, bottom_y, HALF, bottom_h),
        "Trafic : visiteurs par fenêtre de 5 minutes",
    )

    platform = Page(
        "platform",
        "Plateforme & FinOps",
        "Santé des pipelines, qualité des données et coût estimé",
    )
    platform.kpis(
        ["Pipeline Runs", "Success Rate", "DLQ Rate", "Estimated Monthly Cost (USD)"]
    )
    platform.add(
        "columnChart",
        {
            "Category": ["dim_date.date_day"],
            "Series": ["fct_pipeline_runs.pipeline_name"],
            "Y": [f"{m}.Rows Ingested"],
        },
        (left, main_y, WIDE, main_h),
        "Lignes ingérées par jour",
        objects={
            "categoryAxis": [{"properties": {"axisType": literal("'Categorical'")}}]
        },
    )
    platform.add(
        "clusteredBarChart",
        {
            "Category": ["fct_pipeline_rejections.rejection_reason"],
            "Y": [f"{m}.Rejected Rows by Rule"],
        },
        (right, main_y, NARROW, main_h),
        "Rejets par règle qualité",
        sort_by=f"{m}.Rejected Rows by Rule",
        objects={"categoryAxis": [{"properties": {"maxMarginFactor": literal("45D")}}]},
    )
    platform.add(
        "tableEx",
        {
            "Values": [
                "fct_pipeline_runs.pipeline_name",
                f"{m}.Pipeline Runs",
                f"{m}.Failed Runs",
                f"{m}.DLQ Rate",
                f"{m}.Avg Run Duration (s)",
                f"{m}.P95 Run Duration (s)",
                f"{m}.Estimated Cost (USD)",
            ]
        },
        (left, bottom_y, CONTENT, bottom_h),
        "Performance et coût estimé par pipeline",
        sort_by=f"{m}.Pipeline Runs",
    )
    return [sales, web, platform]  # fmt: skip


def build_report() -> None:
    write_json(
        REPORT_DIR / "definition.pbir",
        {
            "$schema": f"{SCHEMAS}/item/report/definitionProperties/2.0.0/schema.json",
            "version": "4.0",
            "datasetReference": {"byPath": {"path": f"../{PROJECT}.SemanticModel"}},
        },
    )
    definition = REPORT_DIR / "definition"
    write_json(
        definition / "version.json",
        {
            "$schema": f"{SCHEMAS}/item/report/definition/versionMetadata/1.0.0/schema.json",
            "version": "2.0.0",
        },
    )
    theme_file = "stratum-theme.json"
    resources = REPORT_DIR / "StaticResources" / "RegisteredResources"
    resources.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(HERE / "theme" / theme_file, resources / theme_file)
    write_json(
        definition / "report.json",
        {
            "$schema": f"{SCHEMAS}/item/report/definition/report/1.2.0/schema.json",
            "themeCollection": {
                "baseTheme": {
                    "name": "CY24SU10",
                    "reportVersionAtImport": "5.61",
                    "type": "SharedResources",
                },
                "customTheme": {
                    "name": theme_file,
                    "reportVersionAtImport": "5.61",
                    "type": "RegisteredResources",
                },
            },
            "layoutOptimization": "None",
            "resourcePackages": [
                {
                    "name": "RegisteredResources",
                    "type": "RegisteredResources",
                    "items": [
                        {"name": theme_file, "path": theme_file, "type": "CustomTheme"}
                    ],
                }
            ],
        },
    )
    report_pages = pages()
    write_json(
        definition / "pages" / "pages.json",
        {
            "$schema": f"{SCHEMAS}/item/report/definition/pagesMetadata/1.0.0/schema.json",
            "pageOrder": [p.name for p in report_pages],
            "activePageName": report_pages[0].name,
        },
    )
    for page in report_pages:
        folder = definition / "pages" / page.name
        write_json(
            folder / "page.json",
            {
                "$schema": f"{SCHEMAS}/item/report/definition/page/1.3.0/schema.json",
                "name": page.name,
                "displayName": page.title,
                "displayOption": "FitToPage",
                "height": 720,
                "width": 1280,
            },
        )
        for visual in page.visuals:
            write_json(folder / "visuals" / visual["name"] / "visual.json", visual)


def main() -> None:
    for folder in (MODEL_DIR, REPORT_DIR):
        if folder.exists():
            shutil.rmtree(folder)
    measures = read_measures()
    build_model(measures)
    build_report()
    write_json(
        HERE / f"{PROJECT}.pbip",
        {
            "$schema": f"{SCHEMAS}/pbip/pbipProperties/1.0.0/schema.json",
            "version": "1.0",
            "artifacts": [{"report": {"path": f"{PROJECT}.Report"}}],
            "settings": {"enableAutoRecovery": True},
        },
    )
    files = sum(
        1
        for _ in HERE.rglob("*")
        if _.is_file() and (MODEL_DIR in _.parents or REPORT_DIR in _.parents)
    )
    print(
        f"{PROJECT}.pbip: {len(TABLES)} tables, {len(measures)} measures, {len(RELATIONSHIPS)} relationships, {files} files"
    )


if __name__ == "__main__":
    main()
