import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
#------------------------------------------------------------------------
from datetime import date
from typing import List, Optional
# Pydantic v2 import for validator
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    BigInteger, Text, Integer, Date, TIMESTAMP, ForeignKey, func, text
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select
#---------------------------------------------------------------------------
app = Flask(__name__)

def get_conn():
    return psycopg2.connect(
        host="avo-adb-002.postgres.database.azure.com",
        user="administrationSTS",
        password="St$@0987",
        dbname="Sales_feedback"
    )

# --- Schéma attendu & validation basique ---
STEP_KEYS = [
    "table_structure_and_layout",
    "usability_and_data_handling",
    "speed_and_performance",
    "data_relevance_and_sts_support",
    "suggestions_and_needs",
    "overall_satisfaction_and_impact"
]

def validate_payload(payload: dict):
    # Champs de tête
    if "sales_person_text" not in payload or not isinstance(payload["sales_person_text"], str):
        return False, "Missing or invalid 'sales_person_text'"
    # date facultative (ISO YYYY-MM-DD si fournie)
    if "date" in payload and payload["date"]:
        try:
            datetime.strptime(payload["date"], "%Y-%m-%d")
        except ValueError:
            return False, "Invalid 'date' format. Use YYYY-MM-DD."
    # 6 sections présentes et de type dict (on stockera en JSON)
    for k in STEP_KEYS:
        if k not in payload:
            return False, f"Missing section '{k}'"
        if not isinstance(payload[k], dict):
            return False, f"Section '{k}' must be an object (dict)"
    return True, None

@app.route("/api/feedback", methods=["POST"])
def insert_feedback():
    try:
        data = request.get_json(force=True, silent=False)

        ok, err = validate_payload(data)
        if not ok:
            return jsonify({"error": err}), 400

        # Sérialiser les 6 sections en JSON
        serialized = {k: json.dumps(data.get(k, {}), ensure_ascii=False) for k in STEP_KEYS}

        sales_person_text = data.get("sales_person_text")
        date_str = data.get("date") or datetime.utcnow().strftime("%Y-%m-%d")

        sql = """
            INSERT INTO feedback_survey (
                sales_person_text, date,
                table_structure_and_layout,
                usability_and_data_handling,
                speed_and_performance,
                data_relevance_and_sts_support,
                suggestions_and_needs,
                overall_satisfaction_and_impact
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
        """

        vals = (
            sales_person_text, date_str,
            serialized["table_structure_and_layout"],
            serialized["usability_and_data_handling"],
            serialized["speed_and_performance"],
            serialized["data_relevance_and_sts_support"],
            serialized["suggestions_and_needs"],
            serialized["overall_satisfaction_and_impact"]
        )

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, vals)
                new_id = cur.fetchone()["id"]

        return jsonify({"message": "Feedback saved", "id": new_id}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

#-------------------------------------------------------------------------------------------------------------------------------------------------------
DATABASE_URL = "postgresql+psycopg2://administrationSTS:St%24%400987@avo-adb-002.postgres.database.azure.com:5432/test_db"
engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData(schema="public")  # your tables are in public

# Reflect/define just the columns we need
sujet = Table(
    "sujet", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("code", Text, unique=True),
    Column("titre", Text, nullable=False),
    Column("description", Text),
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now()),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()),
    Column("parent_sujet_id", BigInteger, ForeignKey("public.sujet.id", onupdate="CASCADE", ondelete="SET NULL"))
)

action = Table(
    "action", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("sujet_id", BigInteger, ForeignKey("public.sujet.id", ondelete="CASCADE"), nullable=False),
    Column("parent_action_id", BigInteger, ForeignKey("public.action.id", ondelete="CASCADE")),
    Column("type", Text, nullable=False),  # {action|sub_action|sub_sub_action} per your CHECK (we’ll just store 'action')
    Column("titre", Text, nullable=False),
    Column("description", Text),
    Column("status", Text, server_default=text("'open'")),
    Column("priorite", Integer),
    Column("responsable", Text),
    Column("due_date", Date),
    Column("ordre", Integer),
    Column("depth", Integer),  # generated column in DB; we won't write to it
    Column("created_at", TIMESTAMP(timezone=True), server_default=func.now()),
    Column("updated_at", TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()),
)

# --------------------------------------------
# Pydantic models (strict validation)
# --------------------------------------------
VALID_STATUSES = {"open", "closed", "blocked"}

class ActionNode(BaseModel):
    titre: str = Field(..., min_length=1)
    description: Optional[str] = None
    responsable: Optional[str] = None
    priorite: Optional[int] = Field(None, ge=0)
    due_date: Optional[date] = None
    status: Optional[str] = Field("open")

    sous_actions: List["ActionNode"] = Field(default_factory=list)

    # --- FIX 1: Use @field_validator instead of @validator ---
    @field_validator("status")
    def status_must_be_valid(cls, v):
        if v is None:
            return "open"
        if v not in VALID_STATUSES:
            raise ValueError(f"status must be one of {sorted(VALID_STATUSES)}")
        return v

# --- FIX 2: Use .model_rebuild() instead of .update_forward_refs() ---
ActionNode.model_rebuild()

class SujetNode(BaseModel):
    titre: str = Field(..., min_length=1)
    code: Optional[str] = None
    description: Optional[str] = None

    sous_sujets: List["SujetNode"] = Field(default_factory=list)
    actions: List[ActionNode] = Field(default_factory=list)

# --- FIX 3: Use .model_rebuild() instead of .update_forward_refs() ---
SujetNode.model_rebuild()

class PlanV1(BaseModel):
    # --- FIX 4: Use 'pattern' instead of 'regex' ---
    version: str = Field(..., pattern=r"^1\.0$")
    plan_code: Optional[str] = None
    plan_title: str = Field(..., min_length=1)
    sujets: List[SujetNode] = Field(default_factory=list)

# --------------------------------------------
# DB helper functions
# --------------------------------------------
def upsert_sujet(conn: Connection,
                 titre: str,
                 parent_sujet_id: Optional[int],
                 code: Optional[str],
                 description: Optional[str]) -> int:
    """
    Prefer 'code' uniqueness when provided, otherwise rely on (parent_sujet_id, titre) unique index.
    Your DB already has:
      - UNIQUE (code)
      - UNIQUE (parent_sujet_id, titre)
    """
    if code:
        # This block works because the UNIQUE (code) constraint exists.
        stmt = pg_insert(sujet).values(
            code=code, titre=titre, description=description, parent_sujet_id=parent_sujet_id
        ).on_conflict_do_update(
            index_elements=["code"],
            set_=dict(
                titre=titre,
                description=description,
                parent_sujet_id=parent_sujet_id,
                updated_at=func.now()
            )
        ).returning(sujet.c.id)
        return conn.execute(stmt).scalar_one()
    else:
        # --- THIS IS THE FIX ---
        # No 'code' provided. Use SELECT-then-UPDATE/INSERT to avoid
        # ON CONFLICT, which fails if the (parent, titre) index is missing.
        
        # We must handle NULL parents correctly in the SELECT
        if parent_sujet_id is None:
            sel_stmt = select(sujet.c.id).where(
                sujet.c.parent_sujet_id.is_(None),
                sujet.c.titre == titre
            )
        else:
            sel_stmt = select(sujet.c.id).where(
                sujet.c.parent_sujet_id == parent_sujet_id,
                sujet.c.titre == titre
            )
        
        existing_id_row = conn.execute(sel_stmt).first()
        
        if existing_id_row:
            # --- IT EXISTS: UPDATE ---
            existing_id = existing_id_row[0]
            upd_stmt = sujet.update().where(
                sujet.c.id == existing_id
            ).values(
                description=description,
                updated_at=func.now()
            ).returning(sujet.c.id)
            return conn.execute(upd_stmt).scalar_one()
        else:
            # --- IT DOES NOT EXIST: INSERT ---
            ins_stmt = sujet.insert().values(
                titre=titre,
                description=description,
                parent_sujet_id=parent_sujet_id
            ).returning(sujet.c.id)
            return conn.execute(ins_stmt).scalar_one()

def insert_action_recursive(conn: Connection,
                             sujet_id: int,
                             parent_action_id: Optional[int],
                             node: ActionNode) -> int:
    # Store everything as type='action' to avoid the 3-level limit in your CHECK.
    # If your CHECK enforces {'action','sub_action','sub_sub_action'}, we’ll set:
    #   root action      -> 'action'
    #   1st-level child  -> 'sub_action'
    #   2nd+ level child -> 'sub_sub_action' (cap at deepest allowed)
    def level_type(level: int) -> str:
        if level <= 0: return "action"
        if level == 1: return "sub_action"
        return "sub_sub_action"

    # Compute action level by walking up the parent chain quickly (one select)
    # If parent_action_id is None -> level 0; else derive parent's depth from DB if stored, else approximate.
    act_level = 0
    if parent_action_id:
        # try to read parent depth (if your generated column is 0/1 only, this still helps cap type)
        parent = conn.execute(
            select(action.c.depth).where(action.c.id == parent_action_id)
        ).first()
        if parent and parent[0] is not None:
            # parent.depth + 1 (may be capped to 1 in your schema, but we only need it to produce a valid 'type')
            act_level = min(int(parent[0]) + 1, 2)
        else:
            act_level = 1

    row = conn.execute(
        action.insert().values(
            sujet_id=sujet_id,
            parent_action_id=parent_action_id,
            type=level_type(act_level),
            titre=node.titre,
            description=node.description,
            status=node.status or "open",
            priorite=node.priorite,
            responsable=node.responsable,
            due_date=node.due_date,
            ordre=None  # you can set/compute ordering here if needed
        ).returning(action.c.id)
    ).first()
    new_id = int(row[0])

    # recurse
    for child in node.sous_actions:
        insert_action_recursive(conn, sujet_id, new_id, child)

    return new_id

def ingest_sujet_tree(conn: Connection, node: SujetNode, parent_id: Optional[int]) -> int:
    this_id = upsert_sujet(conn,
                           titre=node.titre,
                           parent_sujet_id=parent_id,
                           code=node.code,
                           description=node.description)

    # actions directly under this sujet
    for a in node.actions:
        insert_action_recursive(conn, sujet_id=this_id, parent_action_id=None, node=a)

    # nested sujets
    for s in node.sous_sujets:
        ingest_sujet_tree(conn, s, this_id)

    return this_id

def ingest_plan(conn: Connection, plan: PlanV1) -> int:
    """
    Creates/upsers a root sujet for the plan (by plan_code if present, else by title),
    then ingests all sujets/actions under it.
    Returns: root sujet id.
    """
    root_code = plan.plan_code
    root_titre = plan.plan_title
    root_desc = "Action plan root (ingested by assistant)"

    root_id = upsert_sujet(conn,
                           titre=root_titre,
                           parent_sujet_id=None,
                           code=root_code,
                           description=root_desc)

    for s in plan.sujets:
        ingest_sujet_tree(conn, s, root_id)

    return root_id

# --------------------------------------------
# Flask app & endpoints
# --------------------------------------------
app = Flask(__name__)

@app.route("/health", methods=["GET"])
def health():
    return {"ok": True}

@app.route("/api/plans", methods=["POST"])
def post_plan():
    """
    Body: JSON matching PlanV1 (see /api/schema for shape)
    Returns: { root_sujet_id }
    """
    data = request.get_json(force=True, silent=False)
    try:
        # Use model_validate instead of parse_obj for Pydantic v2
        plan = PlanV1.model_validate(data)
    except Exception as e:
        return jsonify({"error": "validation_error", "detail": str(e)}), 400

    with engine.begin() as conn:
        try:
            root_id = ingest_plan(conn, plan)
            return jsonify({"root_sujet_id": root_id})
        except IntegrityError as ie:
            return jsonify({"error": "db_integrity_error", "detail": str(ie.orig)}), 409
        except Exception as e:
            return jsonify({"error": "server_error", "detail": str(e)}), 500

@app.route("/api/schema", methods=["GET"])
def get_schema():
    """
    Returns the JSON "pattern" the GPT assistant must produce (v1).
    """
    return jsonify({
        "version": "1.0",
        "plan_code": "AP-2025-10-OPS-001",
        "plan_title": "Q4 Operations Readiness",
        "sujets": [
            {
                "titre": "Maintenance",
                "code": "OPS-MNT",
                "description": "Preventive and corrective maintenance plan.",
                "sous_sujets": [
                    {
                        "titre": "Compressors",
                        "description": "Air compressor reliability",
                        "sous_sujets": []
                    }
                ],
                "actions": [
                    {
                        "titre": "Create weekly PM checklist",
                        "description": "Draft and validate PM checklist with production.",
                        "responsable": "jane.doe",
                        "priorite": 2,
                        "due_date": "2025-11-15",
                        "status": "open",
                        "sous_actions": [
                            {
                                "titre": "Collect OEM manuals",
                                "due_date": "2025-10-31",
                                "sous_actions": [
                                    {
                                        "titre": "Request missing manuals from supplier",
                                        "due_date": "2025-10-27",
                                        "sous_actions": []
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    })

#-------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
