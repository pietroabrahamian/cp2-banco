import oracledb
from flask import Flask, jsonify, render_template
from flask_cors import CORS
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DSN      = os.getenv("DB_DSN")

app = Flask(__name__)
CORS(app)

# Bloco PL/SQL escrito em arquivo separado para evitar problemas de encoding
PLSQL_CASHBACK = open(
    os.path.join(os.path.dirname(__file__), "cashback.sql"),
    encoding="utf-8"
).read()


def get_connection():
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/executar-cashback", methods=["POST"])
def executar_cashback():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                out_ok  = cur.var(oracledb.NUMBER)
                out_err = cur.var(oracledb.NUMBER)
                cur.execute(PLSQL_CASHBACK, {"out_ok": out_ok, "out_err": out_err})
                return jsonify({
                    "status": "success",
                    "processados_ok":  int(out_ok.getvalue() or 0),
                    "processados_err": int(out_err.getvalue() or 0),
                    "timestamp": datetime.now().isoformat()
                })
    except oracledb.DatabaseError as e:
        error, = e.args
        return jsonify({"status": "error", "oracle_code": error.code, "message": error.message.strip()}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/usuarios", methods=["GET"])
def listar_usuarios():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        u.ID,
                        u.NOME,
                        u.EMAIL,
                        u.PRIORIDADE,
                        u.SALDO,
                        COUNT(CASE WHEN i.STATUS = 'PRESENT' THEN 1 END) AS qtd_presencas,
                        MAX(i.TIPO) AS tipo
                    FROM USUARIOS u
                    LEFT JOIN INSCRICOES i ON i.USUARIO_ID = u.ID
                    GROUP BY u.ID, u.NOME, u.EMAIL, u.PRIORIDADE, u.SALDO
                    ORDER BY u.SALDO DESC
                """)
                cols = [d[0].lower() for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                for r in rows:
                    if r.get('saldo') is not None:
                        r['saldo'] = float(r['saldo'])
                return jsonify({"status": "success", "data": rows})
    except oracledb.DatabaseError as e:
        error, = e.args
        return jsonify({"status": "error", "message": error.message.strip()}), 500


@app.route("/api/logs", methods=["GET"])
def listar_logs():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        l.ID,
                        l.INSCRICAO_ID,
                        l.MOTIVO,
                        TO_CHAR(l.DATA, 'DD/MM/YYYY HH24:MI:SS') AS data_fmt,
                        u.NOME AS usuario_nome
                    FROM LOG_AUDITORIA l
                    JOIN INSCRICOES i ON i.ID = l.INSCRICAO_ID
                    JOIN USUARIOS   u ON u.ID = i.USUARIO_ID
                    ORDER BY l.DATA DESC
                    FETCH FIRST 50 ROWS ONLY
                """)
                cols = [d[0].lower() for d in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                return jsonify({"status": "success", "data": rows})
    except oracledb.DatabaseError as e:
        error, = e.args
        return jsonify({"status": "error", "message": error.message.strip()}), 500


@app.route("/api/resumo", methods=["GET"])
def resumo():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(DISTINCT u.ID) AS total_usuarios,
                        COUNT(CASE WHEN i.STATUS = 'PRESENT' THEN 1 END) AS total_presentes,
                        SUM(CASE WHEN i.STATUS = 'PRESENT'
                                 THEN i.VALOR_PAGO * CASE
                                     WHEN (SELECT COUNT(*) FROM INSCRICOES i2
                                           WHERE i2.USUARIO_ID = i.USUARIO_ID
                                             AND i2.STATUS = 'PRESENT') > 3 THEN 0.25
                                     WHEN i.TIPO = 'VIP' THEN 0.20
                                     ELSE 0.10 END
                                 ELSE 0 END) AS total_cashback,
                        ROUND(AVG(u.SALDO), 2) AS saldo_medio
                    FROM USUARIOS u
                    LEFT JOIN INSCRICOES i ON i.USUARIO_ID = u.ID
                """)
                row = cur.fetchone()
                return jsonify({
                    "status": "success",
                    "total_usuarios":  int(row[0] or 0),
                    "total_presentes": int(row[1] or 0),
                    "total_cashback":  float(row[2] or 0),
                    "saldo_medio":     float(row[3] or 0)
                })
    except oracledb.DatabaseError as e:
        error, = e.args
        return jsonify({"status": "error", "message": error.message.strip()}), 500


if __name__ == "__main__":
    print(f"Conectando em: {DB_DSN} com usuario: {DB_USER}")
    app.run(debug=True, port=5000)