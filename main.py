from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import bcrypt
from datetime import datetime
import pandas as pd
from fastapi.responses import FileResponse
import os

app = FastAPI()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# COLE A SUA SENHA REAL NO LUGAR DE SUA_SENHA_AQUI
DB_URL = "postgresql://postgres:F%40ntasyWmnnwlhtscuclh%402026@db.uxwanmswcnamvsqjqgyu.supabase.co:5432/postgres"

def get_db():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def registrar_log(usuario, acao):
    data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO logs (data, usuario, acao) VALUES (%s, %s, %s)", (data_hora, usuario, acao))
        conn.commit()

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/login")
def login(req: LoginRequest):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM gestores WHERE username = %s", (req.username.lower(),))
            user = cur.fetchone()
            
            if user:
                # Checa a senha raw (como '123') ou a senha criptografada
                if req.password == user['password'] or (len(user['password']) > 20 and bcrypt.checkpw(req.password.encode('utf-8'), user['password'].encode('utf-8'))):
                    registrar_log(user['username'], "Fez login no sistema")
                    return {"username": user['username'], "perfil": user['perfil']}
    
    raise HTTPException(status_code=401, detail="Credenciais inválidas")

class Clinica(BaseModel):
    cod: int
    nome: str
    empresa: str
    localizacao: str
    uf: str
    status: str
    gerente: str
    usuario_ext: str

@app.get("/clinicas")
def listar_clinicas():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM clinicas ORDER BY cod ASC")
            return cur.fetchall()

@app.post("/clinicas")
def criar_clinica(c: Clinica):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO clinicas (cod, nome, empresa, localizacao, uf, gerente, status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (c.cod, c.nome, c.empresa, c.localizacao, c.uf, c.gerente, c.status))
            conn.commit()
        registrar_log(c.usuario_ext, f"Cadastrou a clínica #{c.cod} - {c.nome}")
        return {"msg": "Clínica criada"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/clinicas/{cod}")
def editar_clinica(cod: int, c: Clinica):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE clinicas SET nome=%s, empresa=%s, localizacao=%s, uf=%s, gerente=%s, status=%s 
                    WHERE cod=%s
                """, (c.nome, c.empresa, c.localizacao, c.uf, c.gerente, c.status, cod))
            conn.commit()
        registrar_log(c.usuario_ext, f"Editou a clínica #{cod} - {c.nome}")
        return {"msg": "Clínica atualizada"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/clinicas/{cod}")
def deletar_clinica(cod: int, usuario: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM clinicas WHERE cod=%s", (cod,))
        conn.commit()
    registrar_log(usuario, f"Excluiu a clínica #{cod}")
    return {"msg": "Clínica excluída"}

class Gestor(BaseModel):
    username: str
    password: str
    perfil: str

@app.get("/gestores")
def listar_gestores():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username as user, perfil FROM gestores ORDER BY id ASC")
            return cur.fetchall()

@app.post("/gestores")
def criar_gestor(g: Gestor):
    hashed_pw = bcrypt.hashpw(g.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO gestores (username, password, perfil) VALUES (%s, %s, %s)", 
                            (g.username.lower(), hashed_pw, g.perfil))
            conn.commit()
        return {"msg": "Gestor criado"}
    except:
        raise HTTPException(status_code=400, detail="Usuário já existe")

@app.get("/logs")
def listar_logs():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT data, usuario as user, acao FROM logs ORDER BY id DESC LIMIT 100")
            return cur.fetchall()

@app.get("/exportar")
def exportar_excel():
    with get_db() as conn:
        df = pd.read_sql_query("SELECT * FROM clinicas ORDER BY cod ASC", conn)
    
    arquivo = "relatorio_clinicas.xlsx"
    df.to_excel(arquivo, index=False)
    return FileResponse(arquivo, filename="Hapclinicas_Base.xlsx")