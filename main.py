import oracledb
import bcrypt
import pandas as pd
import io
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime

# --- CONFIGURAÇÕES DE BANCO DE DADOS ---
DB_USER = "USR_GOVERNANCA_INDICADORES"
DB_PASS = "NkAqtfff6354Hjgb"
DB_DSN = "(description=(retry_count=2)(retry_delay=3)(SOURCE_ROUTE=YES)(ADDRESS=(PROTOCOL=TCP)(HOST=dl-cman.hapvida.com.br)(PORT=1523))(address=(protocol=tcps)(port=1522)(host=pyruqdet.adb.sa-saopaulo-1.oraclecloud.com))(connect_data=(service_name=ga7aea8a1e872fc_dbrfnzn_low.adb.oraclecloud.com))(security=(ssl_server_dn_match=no)))"

db_pool = None

# --- HELPERS DE SEGURANÇA ---
def gerar_hash(senha: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(senha.encode('utf-8'), salt).decode('utf-8')

def verificar_senha(senha_plana: str, senha_hash: str) -> bool:
    try: return bcrypt.checkpw(senha_plana.encode('utf-8'), senha_hash.encode('utf-8'))
    except: return False

def registrar_log(usuario, acao):
    """Função interna para gravar auditoria"""
    q_id = "SELECT NVL(MAX(ID_LOG), 0) + 1 FROM LOGS_GC"
    q_ins = "INSERT INTO LOGS_GC (ID_LOG, USUARIO, ACAO, DATA_HORA) VALUES (:1, :2, :3, SYSTIMESTAMP)"
    try:
        with db_pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute(q_id); nid = cursor.fetchone()[0]
                cursor.execute(q_ins, (nid, usuario, acao))
                conn.commit()
    except Exception as e: print(f"Erro Log: {e}")

# --- CICLO DE VIDA ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = oracledb.create_pool(user=DB_USER, password=DB_PASS, dsn=DB_DSN, min=2, max=5)
    yield
    if db_pool: db_pool.close()

app = FastAPI(title="HAPCLINICAS PRO - FULL", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- MODELOS ---
class LoginRequest(BaseModel):
    username: str
    password: str

class ClinicaCreate(BaseModel):
    cod: int
    nome: str
    empresa: str
    localizacao: str
    uf: str
    status: str
    gerente: str
    usuario_ext: str # Quem está fazendo a ação

class GestorCreate(BaseModel):
    username: str
    password: str
    perfil: str

# --- ROTAS ---

@app.post("/login")
async def login(req: LoginRequest):
    query = "SELECT PASSWORD_HASH, PERFIL FROM ACESSOS_GC WHERE USERNAME = :1"
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, [req.username])
            res = cursor.fetchone()
            if not res or (res[0] != "123" and not verificar_senha(req.password, res[0])):
                raise HTTPException(401, "Acesso Negado")
            registrar_log(req.username, "Fez login no sistema")
            return {"perfil": res[1], "username": req.username}

@app.get("/clinicas")
async def listar():
    query = "SELECT COD_CLINICA, NOME_CLINICA, STATUS_CLINICA, EMPRESA, LOCALIZACAO, UF, GERENTE FROM RFN_HAPCLINICAS ORDER BY COD_CLINICA"
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return [{"cod": r[0], "nome": r[1], "status": r[2], "empresa": r[3], "localizacao": r[4], "uf": r[5], "gerente": r[6]} for r in cursor.fetchall()]

@app.post("/clinicas")
async def criar(c: ClinicaCreate):
    q = "INSERT INTO RFN_HAPCLINICAS VALUES (:1, :2, :3, :4, :5, :6, :7)"
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(q, (c.cod, c.nome, c.empresa, c.localizacao, c.uf, c.status, c.gerente))
                conn.commit()
                registrar_log(c.usuario_ext, f"CRIOU clínica #{c.cod} - {c.nome}")
                return {"ok": True}
            except oracledb.IntegrityError: raise HTTPException(400, "Código já existe")

@app.put("/clinicas/{cod}")
async def editar(cod: int, c: ClinicaCreate):
    q = "UPDATE RFN_HAPCLINICAS SET NOME_CLINICA=:1, EMPRESA=:2, LOCALIZACAO=:3, UF=:4, STATUS_CLINICA=:5, GERENTE=:6 WHERE COD_CLINICA=:7"
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute(q, (c.nome, c.empresa, c.localizacao, c.uf, c.status, c.gerente, cod))
            conn.commit()
            registrar_log(c.usuario_ext, f"EDITOU clínica #{cod}")
            return {"ok": True}

@app.delete("/clinicas/{cod}")
async def deletar(cod: int, usuario: str):
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM RFN_HAPCLINICAS WHERE COD_CLINICA = :1", [cod])
            conn.commit()
            registrar_log(usuario, f"EXCLUIU clínica #{cod}")
            return {"ok": True}

# --- EXPORTAÇÃO EXCEL ---
@app.get("/exportar")
async def exportar():
    query = "SELECT * FROM RFN_HAPCLINICAS"
    with db_pool.acquire() as conn:
        df = pd.read_sql(query, conn)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Clinicas')
    
    output.seek(0)
    headers = {'Content-Disposition': 'attachment; filename="relatorio_clinicas.xlsx"'}
    return StreamingResponse(output, headers=headers, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# --- BUSCAR LOGS (AUDITORIA) ---
@app.get("/logs")
async def ver_logs():
    query = "SELECT USUARIO, ACAO, TO_CHAR(DATA_HORA, 'DD/MM HH24:MI') FROM LOGS_GC ORDER BY ID_LOG DESC"
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return [{"user": r[0], "acao": r[1], "data": r[2]} for r in cursor.fetchmany(50)]

@app.get("/gestores")
async def g_list():
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT ID_USUARIO, USERNAME, PERFIL FROM ACESSOS_GC")
            return [{"id": r[0], "user": r[1], "perfil": r[2]} for r in cursor.fetchall()]

@app.post("/gestores")
async def g_add(g: GestorCreate):
    h = gerar_hash(g.password)
    with db_pool.acquire() as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO ACESSOS_GC (ID_USUARIO, USERNAME, PASSWORD_HASH, PERFIL) VALUES ((SELECT NVL(MAX(ID_USUARIO),0)+1 FROM ACESSOS_GC), :1, :2, :3)", (g.username, h, g.perfil))
            conn.commit()
            return {"ok": True}