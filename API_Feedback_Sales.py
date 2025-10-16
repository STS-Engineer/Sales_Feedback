import os
import json
from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor


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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
