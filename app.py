from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import pandas as pd
import requests
import os
import threading
import json
import time
from datetime import datetime
from queue import Queue

app = Flask(__name__)

RATE_SECONDS = 1.5

MENSAGENS_RELEVANTES = [
    "CPF não possui saldo disponível",
    "Operação não permitida",
    "não possui saldo para um contrato mínimo",
    "Existe uma Operação Fiduciária em andamento. Tente mais tarde.",
    "Mudanças cadastrais",
    "Mudanças cadastrais ou lançamentos a débito foram realizadas na conta do FGTS, impedindo a contratação. Entre em contato com o setor de FGTS da CAIXA para regularização ou acompanhe a data do débito no APP FGTS.",
    "Instituição fiduciária",
    "Aniversariante",
    "Data de Aniversário informada não condiz com a Data de Aniversário do trabalhador",
    "Autorizado",
    "Não autorizado",
    "Sem saldo",
    "Cliente não autorizou a instituição financeira a realizar a consulta",
    "Sem retorno da CEF, tente novamente.",
    "Serviço CEF indisponível no momento.",
    "Trabalhador não possui adesão ao saque aniversário vigente na data corrente."
]

LIMITE_PATTERNS = [
    "limite de requisições",
    "máximo de 1 por segundo",
    "limite de consultas excedido"
]

RESULT_FOLDER = "resultados"
PROGRESS_FILE = os.path.join(RESULT_FOLDER, "progresso.json")
CONTADOR_ARQUIVO = "contador.json"
os.makedirs(RESULT_FOLDER, exist_ok=True)

TOKEN = ""
TOKEN_EXPIRA = 0

fila_cpfs = Queue()
progress_lock = threading.Lock()

progress_data = {
    "total": 0,
    "concluidos": 0,
    "resultados": [],
    "finalizado": False,
    "pausado": False
}

API_SIMULATE = "https://simplix-integration.partner1.com.br/api/Proposal/Simulate"

def resposta(cpf, saldo=0, valor=0, situacao="Erro", info="Sem info", final=True, oculto=False):
    return {
        "cpf": cpf,
        "saldoBruto": saldo,
        "valorLiberado": valor,
        "situacao": situacao,
        "informacao": info,
        "final": final,
        "oculto": oculto  
    }

def gerar_token():
    url = "https://simplix-integration.partner1.com.br/api/Login"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    dados = {
        "username": "477f702a-4a6f-4b02-b5eb-afcd38da99f8",
        "password": "b5iTIZ2n"
    }
    try:
        response = requests.post(url, json=dados, headers=headers, timeout=10)
        if response.status_code == 200 and response.json().get("success"):
            token = response.json()["objectReturn"]["access_token"]
            expira_em = 3600
            global TOKEN_EXPIRA
            TOKEN_EXPIRA = time.time() + expira_em - 60
            print(f"[TOKEN GERADO] {token}")
            return token
    except Exception as e:
        print("Erro ao gerar token:", e)
    return ""

def obter_token():
    global TOKEN
    if not TOKEN or time.time() >= TOKEN_EXPIRA:
        TOKEN = gerar_token()
    return TOKEN

def obter_contador():
    hoje = datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(CONTADOR_ARQUIVO):
        with open(CONTADOR_ARQUIVO, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("data") == hoje:
            return data.get("contador", 0)
    return 0

def registrar_consulta():
    hoje = datetime.now().strftime("%Y-%m-%d")
    contador = obter_contador() + 1
    with open(CONTADOR_ARQUIVO, "w", encoding="utf-8") as f:
        json.dump({"data": hoje, "contador": contador}, f)

@app.route("/")
def index():
    with progress_lock:
        progress_data.update({
            "total": 0,
            "concluidos": 0,
            "resultados": [],
            "finalizado": False,
            "pausado": False
        })
    return render_template("index.html", contador=obter_contador())

@app.route("/simular-lote", methods=["POST"])
def simular_lote():
    cpfs_texto = request.form.get("cpfs")

    lista_cpfs = [
        cpf.strip().zfill(11)
        for cpf in cpfs_texto.strip().splitlines()
        if cpf.strip()
    ]
    lista_cpfs = list(dict.fromkeys(lista_cpfs))

    with progress_lock:
        progress_data.update({
            "total": len(lista_cpfs),
            "concluidos": 0,
            "resultados": [],
            "finalizado": False,
            "pausado": False,
            "todos_cpfs": lista_cpfs.copy()
        })

    for cpf in lista_cpfs:
        fila_cpfs.put(cpf)

    threading.Thread(target=worker, daemon=True).start()    

    return render_template("progresso.html", contador=obter_contador())

@app.route("/progresso-status")
def progresso_status():
    with progress_lock:
        concluidos = progress_data["concluidos"]
        total = progress_data["total"]
        porcentagem = int((concluidos / total) * 100) if total else 0

        cpfs_status = [
            f"{r['cpf']} - {r['informacao']} | Saldo: R$ {r['saldoBruto']} | Liberado: R$ {r['valorLiberado']}"
            for r in progress_data["resultados"]
            if not r.get("oculto")
        ]

        autorizados = sum(1 for r in progress_data["resultados"] if r.get("situacao") == "Consulta OK")

    return jsonify({
        "concluidos": concluidos,
        "total": total,
        "porcentagem": min(porcentagem, 100),
        "finalizado": concluidos == total,
        "pausado": progress_data["pausado"],
        "cpfs": cpfs_status,
        "contador": obter_contador(),
        "autorizados": autorizados
    })

@app.route("/pausar", methods=["POST"])
def pausar():
    with progress_lock:
        progress_data["pausado"] = True
    return jsonify({"status": "pausado"})

@app.route("/retomar", methods=["POST"])
def retomar():
    with progress_lock:
        progress_data["pausado"] = False
    return jsonify({"status": "retomado"})

@app.route("/download")
def download_resultado():
    with progress_lock:
        resultados = progress_data["resultados"]
        consultados_cpfs = {r["cpf"] for r in resultados}
        todos_cpfs = progress_data.get("todos_cpfs", [])

        pendentes_restantes = [
            resposta(cpf, 0, 0, "Pendente", "Ainda não consultado")
            for cpf in todos_cpfs if cpf not in consultados_cpfs
        ]

        todos = resultados + pendentes_restantes

        df = pd.DataFrame(todos)
        filename = f"resultados_{time.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
        path = os.path.join(RESULT_FOLDER, filename)
        df.to_excel(path, index=False)

    return send_file(path, as_attachment=True)

@app.route("/recuperar-progresso", methods=["GET"])
def recuperar_progresso():
    if not os.path.exists(PROGRESS_FILE):
        return "Nenhum progresso salvo encontrado.", 400

    with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
        try:
            resultados = json.load(f)
            if not resultados:
                raise ValueError("Arquivo vazio")
        except Exception:
            return "Erro ao recuperar o progresso.", 500

    resultados_prontos = [
        {
            "cpf": r.get("cpf"),
            "saldoBruto": r.get("saldoBruto", 0),
            "valorLiberado": r.get("valorLiberado", 0),
            "situacao": r.get("situacao", "Consulta realizada"),
            "informacao": r.get("informacao", "Sem info")
        }
        for r in resultados
    ]

    cpfs_resultados = [r["cpf"] for r in resultados_prontos]
    cpfs_todos = progress_data.get("todos_cpfs", cpfs_resultados)

    pendentes = [
        {
            "cpf": cpf,
            "saldoBruto": 0,
            "valorLiberado": 0,
            "situacao": "Pendente",
            "informacao": "Ainda não consultado"
        }
        for cpf in cpfs_todos if cpf not in cpfs_resultados
    ]

    todos = resultados_prontos + pendentes

    df = pd.DataFrame(todos)
    caminho_excel = os.path.join(RESULT_FOLDER, "recuperados.xlsx")
    df.to_excel(caminho_excel, index=False)

    return send_file(caminho_excel, as_attachment=True, download_name="recuperados.xlsx")

def worker():
    while not fila_cpfs.empty():
        cpf = fila_cpfs.get()
        if cpf is None:
            break

        while progress_data["pausado"]:
            time.sleep(1)

        try:
            print(f"[WORKER] Pegando CPF da fila: {cpf}")
            resultado = consultar_cpf(cpf)

            with progress_lock:
                progress_data["resultados"] = [r for r in progress_data["resultados"] if r["cpf"] != cpf]
                progress_data["resultados"].append(resultado)

                if not resultado.get("oculto"):
                    progress_data["concluidos"] += 1
                    registrar_consulta()

                with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                    json.dump(progress_data["resultados"], f, ensure_ascii=False, indent=2)

            print(f"[WORKER] Finalizei CPF: {cpf}")

            time.sleep(RATE_SECONDS)

            if progress_data["concluidos"] % 20 == 0:
                print("Pausa técnica de 5s para evitar bloqueio da API...")
                time.sleep(5)

        except Exception as e:
            print(f"Erro ao processar CPF {cpf}: {e}")
        finally:
            fila_cpfs.task_done()

    with progress_lock:
        progress_data["finalizado"] = True

    print("✅ Todas as consultas foram finalizadas. Encerrando worker.")

def retry_limite(cpf, payload, headers):
    for tentativa in range(3):
        print(f"[{cpf}] ⚠️ Tentativa {tentativa+1}/3 após erro de limite...")
        time.sleep(60)
        try:
            response = requests.post(API_SIMULATE, json=payload, headers=headers, timeout=30)
            data = response.json()
            desc = (data.get("objectReturn", {}) or {}).get("description", "") or response.text

            sim = (data.get("objectReturn", {}) or {}).get("retornoSimulacao", [])
            if sim:
                detalhes = sim[0].get("detalhes", {}) or {}
                msg_ok = sim[0].get("mensagem", "") or "Autorizado"
                resultado = resposta(
                    cpf,
                    detalhes.get("saldoTotalBloqueado", 0),
                    sim[0].get("valorLiquido", 0),
                    "Consulta OK",
                    msg_ok,
                    True
                )
            elif "excedido o limite de requisições" not in desc.lower():
                resultado = resposta(cpf, 0, 0, "Consulta realizada", desc, True)
            else:
                continue

            with progress_lock:
                progress_data["resultados"] = [r for r in progress_data["resultados"] if r["cpf"] != cpf]
                progress_data["resultados"].append(resultado)

                if not resultado.get("oculto"):
                    progress_data["concluidos"] += 1
                    if resultado["situacao"] == "Consulta OK":
                        registrar_consulta()

                with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                    json.dump(progress_data["resultados"], f, ensure_ascii=False, indent=2)
            print(f"[{cpf}] 🔄 Reconsulta OK (fora da fila principal)")
            return
        except Exception as e:
            print(f"[{cpf}] Erro na reconsulta: {e}")

    resultado = resposta(cpf, 0, 0, "Erro", "Limite de tentativas atingido", True)
    with progress_lock:
        progress_data["resultados"] = [r for r in progress_data["resultados"] if r["cpf"] != cpf]
        progress_data["resultados"].append(resultado)
        progress_data["concluidos"] += 1

        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress_data["resultados"], f, ensure_ascii=False, indent=2)
    print(f"[{cpf}] ❌ Limite de tentativas atingido (3x). Gravado como erro final.")

def consultar_cpf(cpf):
    payload = {"cpf": cpf, "parcelas": 0, "convenio": 1, "produto": 1}
    headers = {
        "Authorization": f"Bearer {obter_token()}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    try:
        time.sleep(RATE_SECONDS)
        response = requests.post(API_SIMULATE, json=payload, headers=headers, timeout=60)
        txt = response.text

        print(f"[{cpf}] 📡 Status Code: {response.status_code}")
        try:
            data = response.json()
            print(f"[{cpf}] RAW JSON:\n{json.dumps(data, indent=2, ensure_ascii=False)}")
        except Exception:
            data = {}
            print(f"[{cpf}] RAW TEXT:\n{txt}")

        sim = (data.get("objectReturn", {}) or {}).get("retornoSimulacao", [])
        if sim:
            detalhes = sim[0].get("detalhes", {}) or {}
            msg_ok = sim[0].get("mensagem", "") or "Autorizado"

            return {
                "cpf": cpf,
                "saldoBruto": detalhes.get("saldoTotalBloqueado", 0),
                "valorLiberado": sim[0].get("valorLiquido", 0),
                "situacao": "Consulta OK",
                "informacao": msg_ok,
                "final": True
            }

        desc = (data.get("objectReturn", {}) or {}).get("description", "") or txt

        if "excedido o limite de requisições" in desc.lower():
            threading.Thread(target=retry_limite, args=(cpf, payload, headers), daemon=True).start()
            return {
                "cpf": cpf, "saldoBruto": 0, "valorLiberado": 0,
                "situacao": "Erro", "informacao": desc,
                "oculto": True
            }

        return {
            "cpf": cpf, "saldoBruto": 0, "valorLiberado": 0,
            "situacao": "Consulta realizada", "informacao": desc,
        }

    except requests.exceptions.ReadTimeout:
        return {
            "cpf": cpf, "saldoBruto": 0, "valorLiberado": 0,
            "situacao": "Erro", "informacao": "Timeout na API",
        }

    except Exception as e:
        return {
            "cpf": cpf, "saldoBruto": 0, "valorLiberado": 0,
            "situacao": "Erro", "informacao": f"Erro inesperado: {e}",
        }

@app.route("/historico")
def historico():
    arquivos = sorted(
        [f for f in os.listdir("resultados") if f.startswith("resultados_") and f.endswith(".xlsx")],
        reverse=True
    )
    return render_template("historico.html", arquivos=arquivos)

@app.route("/baixar-resultado/<filename>")
def baixar_resultado(filename):
    return send_from_directory("resultados", filename, as_attachment=True)

@app.route("/static/<filename>")
def baixar_estatico(filename):
    return send_from_directory("static", filename, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True, port=8890)
