from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import pandas as pd
import requests
import os, re
import threading
import json
import time
import uuid
from datetime import datetime, timedelta
from queue import Queue
from flask import redirect, url_for

app = Flask(__name__, static_folder="static")

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

progress_lock = threading.Lock()

progress_data = {}
filas = {}

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

    lote_id = str(uuid.uuid4())
    with progress_lock:
        progress_data[lote_id] = {
            "total": len(lista_cpfs),
            "concluidos": 0,
            "resultados": [],
            "finalizado": False,
            "pausado": False,
            "todos_cpfs": lista_cpfs.copy()
        }

    filas[lote_id] = Queue()
    for cpf in lista_cpfs:
        filas[lote_id].put(cpf)

    threading.Thread(target=worker, args=(lote_id,), daemon=True).start()    

    return redirect(url_for("progresso", lote_id=lote_id))

@app.route("/progresso")
def progresso():
    lote_id = request.args.get("lote_id")
    if not lote_id or lote_id not in progress_data:
        return "Lote não encontrado", 404

    return render_template("progresso.html", contador=obter_contador(), lote_id=lote_id)

@app.route("/progresso-status")
def progresso_status():
    lote_id = request.args.get("lote_id")
    if not lote_id or lote_id not in progress_data:
        return jsonify({"erro": "Lote não encontrado"}), 404

    with progress_lock:
        dados = progress_data[lote_id]
        concluidos = dados["concluidos"]
        total = dados["total"]
        porcentagem = int((concluidos / total) * 100) if total else 0

        cpfs_status = [
            f"{r['cpf']} - {r['informacao']} | Saldo: R$ {r['saldoBruto']} | Liberado: R$ {r['valorLiberado']}"
            for r in dados["resultados"] if not r.get("oculto")
        ]

        autorizados = sum(1 for r in dados["resultados"] if r.get("situacao") == "Consulta OK")

    return jsonify({
        "concluidos": concluidos,
        "total": total,
        "porcentagem": min(porcentagem, 100),
        "finalizado": concluidos == total and dados.get("pendentes_retry", 0) == 0,
        "pausado": dados["pausado"],
        "cpfs": cpfs_status,
        "contador": obter_contador(),
        "autorizados": autorizados
    })

@app.route("/pausar", methods=["POST"])
def pausar():
    lote_id = request.args.get("lote_id")
    if lote_id and lote_id in progress_data:
        with progress_lock:
            progress_data[lote_id]["pausado"] = True
        return jsonify({"status": "pausado"})
    return jsonify({"erro": "Lote não encontrado"}), 404

@app.route("/retomar", methods=["POST"])
def retomar():
    lote_id = request.args.get("lote_id")
    if lote_id and lote_id in progress_data:
        with progress_lock:
            progress_data[lote_id]["pausado"] = False
        return jsonify({"status": "retomado"})
    return jsonify({"erro": "Lote não encontrado"}), 404

def worker(lote_id):
    while not filas[lote_id].empty():
        cpf = filas[lote_id].get()
        if cpf is None:
            break

        while progress_data[lote_id]["pausado"]:
            time.sleep(1)

        try:
            print(f"[WORKER {lote_id}] Pegando CPF da fila: {cpf}")
            resultado = consultar_cpf(cpf, lote_id)

            with progress_lock:
                progress_data[lote_id]["resultados"] = [
                    r for r in progress_data[lote_id]["resultados"] if r["cpf"] != cpf
                ]
                progress_data[lote_id]["resultados"].append(resultado)

                if not resultado.get("oculto"):
                    progress_data[lote_id]["concluidos"] += 1
                    registrar_consulta()

                caminho = os.path.join(RESULT_FOLDER, f"progresso_{lote_id}.json")
                with open(caminho, "w", encoding="utf-8") as f:
                    json.dump({
                        "resultados": progress_data[lote_id]["resultados"],
                        "todos_cpfs": progress_data[lote_id].get("todos_cpfs", []),
                        "concluidos": progress_data[lote_id]["concluidos"],
                        "total": progress_data[lote_id]["total"],
                        "pendentes_retry": progress_data[lote_id].get("pendentes_retry", 0)
                    }, f, ensure_ascii=False, indent=2)

            print(f"[WORKER {lote_id}] Finalizei CPF: {cpf}")
            time.sleep(RATE_SECONDS)

            if progress_data[lote_id]["concluidos"] % 20 == 0:
                print("Pausa técnica de 5s para evitar bloqueio da API...")
                time.sleep(5)

        except Exception as e:
            print(f"Erro ao processar CPF {cpf}: {e}")
        finally:
            filas[lote_id].task_done()

    with progress_lock:
        if progress_data[lote_id].get("pendentes_retry", 0) == 0:
            progress_data[lote_id]["finalizado"] = True

    print(f"✅ Todas as consultas do lote {lote_id} foram finalizadas.")


def retry_limite(cpf, payload, headers, lote_id):
    with progress_lock:
        progress_data[lote_id]["pendentes_retry"] = progress_data[lote_id].get("pendentes_retry", 0) + 1

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
                progress_data[lote_id]["resultados"] = [
                    r for r in progress_data[lote_id]["resultados"] if r["cpf"] != cpf
                ]
                progress_data[lote_id]["resultados"].append(resultado)

                if not resultado.get("oculto"):
                    progress_data[lote_id]["concluidos"] += 1
                    if resultado["situacao"] == "Consulta OK":
                        registrar_consulta()

                # diminuir pendentes
                progress_data[lote_id]["pendentes_retry"] -= 1
                if progress_data[lote_id]["pendentes_retry"] == 0 and filas[lote_id].empty():
                    progress_data[lote_id]["finalizado"] = True

                caminho = os.path.join(RESULT_FOLDER, f"progresso_{lote_id}.json")
                with open(caminho, "w", encoding="utf-8") as f:
                    json.dump(progress_data[lote_id]["resultados"], f, ensure_ascii=False, indent=2)

            print(f"[{cpf}] 🔄 Reconsulta OK (fora da fila principal)")
            return
        except Exception as e:
            print(f"[{cpf}] Erro na reconsulta: {e}")

    resultado = resposta(cpf, 0, 0, "Erro", "Limite de tentativas atingido", True)
    with progress_lock:
        progress_data[lote_id]["resultados"] = [
            r for r in progress_data[lote_id]["resultados"] if r["cpf"] != cpf
        ]
        progress_data[lote_id]["resultados"].append(resultado)
        progress_data[lote_id]["concluidos"] += 1
        progress_data[lote_id]["pendentes_retry"] -= 1

        if progress_data[lote_id]["pendentes_retry"] == 0 and filas[lote_id].empty():
            progress_data[lote_id]["finalizado"] = True

        caminho = os.path.join(RESULT_FOLDER, f"progresso_{lote_id}.json")
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(progress_data[lote_id]["resultados"], f, ensure_ascii=False, indent=2)

    print(f"[{cpf}] ❌ Limite de tentativas atingido (3x). Gravado como erro final.")

def consultar_cpf(cpf, lote_id):
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
            threading.Thread(
                target=retry_limite,
                args=(cpf, payload, headers, lote_id),
                daemon=True
            ).start()
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

@app.route("/download")
def download_resultado():
    lote_id = request.args.get("lote_id")
    if not lote_id or lote_id not in progress_data:
        return "Lote não encontrado", 404

    with progress_lock:
        resultados = progress_data[lote_id]["resultados"]
        consultados_cpfs = {r["cpf"] for r in resultados}
        todos_cpfs = progress_data[lote_id].get("todos_cpfs", [])

        pendentes_restantes = [
            resposta(cpf, 0, 0, "Pendente", "Ainda não consultado")
            for cpf in todos_cpfs if cpf not in consultados_cpfs
        ]

        todos = resultados + pendentes_restantes

        df = pd.DataFrame(todos)

        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"resultados_{lote_id}_{timestamp}.xlsx"
        path = os.path.join(RESULT_FOLDER, filename)
        df.to_excel(path, index=False)

    return send_file(path, as_attachment=True, download_name=filename)

@app.route("/recuperar-progresso", methods=["GET"])
def recuperar_progresso():
    lote_id = request.args.get("lote_id")

    if not lote_id:
        return "É necessário informar o lote_id para recuperar.", 400

    caminho = os.path.join(RESULT_FOLDER, f"progresso_{lote_id}.json")
    if not os.path.exists(caminho):
        return "Nenhum progresso salvo para esse lote.", 404

    with open(caminho, "r", encoding="utf-8") as f:
        try:
            dados = json.load(f)

            if isinstance(dados, list):
                resultados = dados
                cpfs_todos = [r["cpf"] for r in resultados]
            else:
                resultados = dados.get("resultados", [])
                cpfs_todos = dados.get("todos_cpfs", [r["cpf"] for r in resultados])

        except Exception:
            return "Erro ao ler o progresso salvo.", 500

    cpfs_resultados = [r["cpf"] for r in resultados]
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

    todos = resultados + pendentes

    caminho_excel = os.path.join(RESULT_FOLDER, f"recuperados_{lote_id}.xlsx")
    df = pd.DataFrame(todos)
    df.to_excel(caminho_excel, index=False)

    return send_file(
        caminho_excel,
        as_attachment=True,
        download_name=f"recuperados_{lote_id}.xlsx"
    )

@app.route("/listar-lotes")
def listar_lotes():
    arquivos = [f for f in os.listdir(RESULT_FOLDER) if f.startswith("progresso_") and f.endswith(".json")]
    lotes = [f.replace("progresso_", "").replace(".json", "") for f in arquivos]

    return render_template("listar_lotes.html", lotes=lotes)

@app.route("/historico")
def historico():
    agora = datetime.now()
    limite = timedelta(days=5)
    arquivos = []

    for f in os.listdir(RESULT_FOLDER):
        if f.startswith("resultados_") and f.endswith(".xlsx"):
            try:
                match = re.search(r"_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.xlsx$", f)
                if match:
                    data_str = match.group(1) + " " + match.group(2).replace("-", ":")
                    data_arquivo = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")

                    if agora - data_arquivo <= limite:
                        arquivos.append(f)
                    else:
                        os.remove(os.path.join(RESULT_FOLDER, f))
                        print(f"[HISTÓRICO] Apagado {f} (mais de 5 dias).")
            except Exception as e:
                print(f"[HISTÓRICO] Erro ao processar {f}: {e}")

    arquivos = sorted(arquivos, reverse=True)
    return render_template("historico.html", arquivos=arquivos)

@app.route("/baixar-resultado/<filename>")
def baixar_resultado(filename):
    return send_from_directory("resultados", filename, as_attachment=True)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
