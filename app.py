from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from calendar import monthrange
from pathlib import Path
from typing import Optional, Literal
from decimal import Decimal, ROUND_CEILING, InvalidOperation
import io
import re
import tempfile
import traceback
import zipfile

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse

app = FastAPI(title="RMtech", version="0.4.2")

TipoValor = Literal["H", "R$"]


@dataclass
class EventoRegra:
    nome_regra: str
    codigo_evento: str
    tipo_padrao: TipoValor
    palavras_chave: list[str]
    colunas_matricula: list[str]
    colunas_valor: list[str]


COLUNAS_MATRICULA_PADRAO = [
    "MATRICULA", "MATRÍCULA", "COD", "DOC", "CHAPA", "ID", "COD FUNC", "CODIGO", "CÓDIGO"
]

COLUNAS_VALOR_R = [
    "VALOR", "DESCONTO", "TOTAL", "VALOR/HRS", "VALOR HRS", "1", "VALOR TOTAL"
]

COLUNAS_VALOR_H = [
    "HORAS", "TOTAL HORAS", "BH FINAL", "BH", "ADC NOTURNO", "1", "TOTAL", "QTD HORAS"
]

ABAS_IGNORAR = [
    "SALARIO SUBSTITUICAO",
    "SALÁRIO SUBSTITUIÇÃO",
    "MUDANCA DE FUNCAO",
    "MUDANÇA DE FUNÇÃO",
]

REGRAS_EVENTOS: list[EventoRegra] = [
    EventoRegra(
        nome_regra="Desconto Alimentação",
        codigo_evento="600",
        tipo_padrao="R$",
        palavras_chave=["ALIMENTAÇÃO", "ALIMENTACAO", "ALIMENT", "VR", "VALE REFEICAO", "VALE REFEIÇÃO"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_R,
    ),
    EventoRegra(
        nome_regra="Vale Transporte",
        codigo_evento="604",
        tipo_padrao="R$",
        palavras_chave=["VALE TRANSPORTE", "TRANSPORTE", "DESCONTO VT", "VT"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_R,
    ),
    EventoRegra(
        nome_regra="Mensalidade Sindical",
        codigo_evento="615",
        tipo_padrao="R$",
        palavras_chave=["MENSALIDADE SINDICAL", "SINDICAL", "SINDICATO"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_R,
    ),
    EventoRegra(
        nome_regra="Empréstimo Consignado",
        codigo_evento="663",
        tipo_padrao="R$",
        palavras_chave=["EMPRESTIMO", "EMPRÉSTIMO", "CONSIGNADO", "CONSIGN"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_R,
    ),
    EventoRegra(
        nome_regra="Adicional Noturno",
        codigo_evento="187",
        tipo_padrao="H",
        palavras_chave=["ADC NOTURNO", "ADICIONAL NOTURNO", "NOTURNO"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_H,
    ),
    EventoRegra(
        nome_regra="Hora Extra 100%",
        codigo_evento="402",
        tipo_padrao="H",
        palavras_chave=["100", "HR 100", "HORA 100", "HORA EXTRA 100", "HORA EXTRA 100%"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_H,
    ),
    EventoRegra(
        nome_regra="Sobreaviso",
        codigo_evento="347",
        tipo_padrao="H",
        palavras_chave=["SOBREAVISO"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_H,
    ),
    EventoRegra(
        nome_regra="Banco Positivo ES 60%",
        codigo_evento="176",
        tipo_padrao="H",
        palavras_chave=["BANCO ES 60", "BANCO POSITIVO 60", "POS"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_H,
    ),
    EventoRegra(
        nome_regra="Banco Positivo RJ 50%",
        codigo_evento="364",
        tipo_padrao="H",
        palavras_chave=["BANCO 50 RJ", "BANCO 50% RJ", "BANCO POSITIVO RJ 50", "RJ 50"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_H,
    ),
    EventoRegra(
        nome_regra="Banco Horas Negativas",
        codigo_evento="736",
        tipo_padrao="H",
        palavras_chave=["NEGATIVO", "NEG", "BANCO NEGATIVO", "BANCO HORAS NEGATIVAS"],
        colunas_matricula=COLUNAS_MATRICULA_PADRAO,
        colunas_valor=COLUNAS_VALOR_H,
    ),
]

MAPA_INTELIGENTE = {
    "REEMBOLSO": [
        {"codigo": "512", "termos": [
            "REEMBOLSO FALTA",
            "FALTA INDEVIDA",
            "FALTA ERRADA",
            "DESCONTO FALTA INDEVIDO",
            "REEMBOLSO DE FALTA",
            "REEMBOLSO FALTA INDEVIDA",
            "FALTA",
        ]},
        {"codigo": "363", "termos": [
            "REEMBOLSO SOBREAVISO",
            "SOBREAVISO",
        ]},
        {"codigo": "174", "termos": [
            "REEMBOLSO BANCO DE HRS",
            "REEMBOLSO BANCO DE HORAS",
            "BANCO DE HRS",
            "BANCO DE HORAS",
            "BANCO HRS",
        ]},
        {"codigo": "523", "termos": [
            "AUXILIO CRECHE",
            "AUXÍLIO CRECHE",
            "DESCONTO ALIMENTACAO ERRADO",
            "DESCONTO ALIMENTAÇÃO ERRADO",
            "DIFERENCA VT",
            "DIFERENÇA VT",
            "FERRAMENTAL",
            "MEDICACAO",
            "MEDICAÇÃO",
            "MEDICAMENTO",
            "REEMBOLSO",
        ]},
    ],
    "DESCONTO": [
        {"codigo": "741", "termos": [
            "FERRAMENTAL",
        ]},
        {"codigo": "751", "termos": [
            "MULTA DE TRANSITO",
            "MULTA TRANSITO",
            "MULTA DE TRÂNSITO",
        ]},
        {"codigo": "745", "termos": [
            "AVARIA",
            "AVARIA DE VEICULO",
            "AVARIA DE VEÍCULO",
            "DANO VEICULO",
            "DANO VEÍCULO",
        ]},
        {"codigo": "747", "termos": [
            "2 VIA CARTAO VT",
            "2 VIA CARTÃO VT",
            "2A VIA CARTAO VT",
            "2A VIA CARTÃO VT",
            "SEGUNDA VIA CARTAO VT",
            "SEGUNDA VIA CARTÃO VT",
            "CARTAO VT 2 VIA",
            "CARTÃO VT 2 VIA",
        ]},
        {"codigo": "746", "termos": [
            "2 VIA VR",
            "2A VIA VR",
            "SEGUNDA VIA VR",
        ]},
        {"codigo": "748", "termos": [
            "CELULAR",
            "TELEFONE",
        ]},
        {"codigo": "754", "termos": [
            "PLANO",
            "COPARTICIPACAO",
            "COPARTICIPAÇÃO",
        ]},
        {"codigo": "798", "termos": [
            "EPI",
        ]},
    ]
}


def normalizar_texto(valor: object) -> str:
    texto = str(valor or "").strip().upper()
    texto = (
        texto.replace("Á", "A")
        .replace("À", "A")
        .replace("Ã", "A")
        .replace("Â", "A")
        .replace("É", "E")
        .replace("Ê", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ô", "O")
        .replace("Õ", "O")
        .replace("Ú", "U")
        .replace("Ç", "C")
        .replace("ª", "A")
        .replace("º", "O")
        .replace("°", "O")
    )
    texto = texto.replace("/", " ")
    texto = texto.replace("\\", " ")
    texto = texto.replace("-", " ")
    texto = texto.replace("_", " ")
    texto = texto.replace(".", " ")
    texto = texto.replace(",", " ")
    texto = texto.replace(":", " ")
    texto = texto.replace(";", " ")
    texto = texto.replace("(", " ")
    texto = texto.replace(")", " ")
    texto = texto.replace("[", " ")
    texto = texto.replace("]", " ")
    texto = texto.replace("{", " ")
    texto = texto.replace("}", " ")
    texto = texto.replace("%", " ")
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def nome_arquivo_seguro(texto: str) -> str:
    texto = normalizar_texto(texto).replace(" ", "_")
    texto = re.sub(r"[^A-Z0-9_]+", "", texto)
    return texto[:80] or "ARQUIVO"


def gerar_variantes_termo(termo: str) -> list[str]:
    base = normalizar_texto(termo)
    variantes = {base}

    substituicoes = [
        ("SEGUNDA", "2A"),
        ("2A", "2"),
        ("HORAS", "HRS"),
        ("HRS", "HORAS"),
        ("DIFERENCA", "DIF"),
        ("MEDICACAO", "MEDICAMENTO"),
        ("MEDICAMENTO", "MEDICACAO"),
        ("CARTAO", "CARTAO"),
    ]

    for origem, destino in substituicoes:
        if origem in base:
            variantes.add(base.replace(origem, destino))

    return list(variantes)


def texto_corresponde(texto_base: str, termo: str) -> bool:
    base = f" {normalizar_texto(texto_base)} "
    for variante in gerar_variantes_termo(termo):
        chave = f" {variante} "
        if chave in base:
            return True
    return False


def classificar_por_contexto(texto: str, contexto: str, codigo_padrao: str) -> str:
    texto_norm = normalizar_texto(texto)
    regras = MAPA_INTELIGENTE.get(contexto, [])

    for regra in regras:
        for termo in regra["termos"]:
            if texto_corresponde(texto_norm, termo):
                return regra["codigo"]

    return codigo_padrao


def deve_ignorar_aba(nome_aba: str) -> bool:
    nome = normalizar_texto(nome_aba)
    return any(texto_corresponde(nome, item) for item in ABAS_IGNORAR)


def encontrar_coluna(df: pd.DataFrame, candidatos: list[str]) -> Optional[str]:
    mapa = {normalizar_texto(col): col for col in df.columns}
    for candidato in candidatos:
        chave = normalizar_texto(candidato)
        if chave in mapa:
            return mapa[chave]
    return None


def texto_amostra_df(df: pd.DataFrame, limite_linhas: int = 20) -> str:
    partes = [normalizar_texto(c) for c in df.columns]
    amostra = df.head(limite_linhas)
    for _, row in amostra.iterrows():
        for val in row:
            partes.append(normalizar_texto(val))
    return " ".join(partes)


def detectar_regra_por_nome_aba(nome_aba: str) -> Optional[EventoRegra]:
    nome = normalizar_texto(nome_aba)
    for regra in REGRAS_EVENTOS:
        for palavra in regra.palavras_chave:
            if texto_corresponde(nome, palavra):
                return regra
    return None


def proxima_regra_por_codigo(codigo: str) -> Optional[EventoRegra]:
    for regra in REGRAS_EVENTOS:
        if regra.codigo_evento == codigo:
            return regra
    return None


def detectar_regra_por_conteudo(df: pd.DataFrame, nome_aba: str = "") -> Optional[EventoRegra]:
    texto_total = f"{normalizar_texto(nome_aba)} {texto_amostra_df(df)}"

    prioridades = [
        "ADC NOTURNO",
        "ADICIONAL NOTURNO",
        "SOBREAVISO",
        "BANCO 50 RJ",
        "BANCO 50% RJ",
        "RJ 50",
        "NEGATIVO",
        "BANCO HORAS NEGATIVAS",
        "BANCO ES 60",
        "BANCO POSITIVO 60",
        "100",
        "HR 100",
        "HORA EXTRA 100",
        "MENSALIDADE SINDICAL",
        "EMPRESTIMO",
        "VALE TRANSPORTE",
        "DESCONTO VT",
        "ALIMENTACAO",
        "ALIMENTAÇÃO",
    ]

    for termo in prioridades:
        if texto_corresponde(texto_total, termo):
            for regra in REGRAS_EVENTOS:
                if any(texto_corresponde(termo, p) or texto_corresponde(p, termo) for p in regra.palavras_chave):
                    return regra

    colunas_norm = [normalizar_texto(c) for c in df.columns]

    if "ADC NOTURNO" in colunas_norm:
        return proxima_regra_por_codigo("187")

    if "TOTAL HORAS" in colunas_norm or "HORAS" in colunas_norm:
        if texto_corresponde(texto_total, "SOBREAVISO"):
            return proxima_regra_por_codigo("347")

    if "BH FINAL" in colunas_norm or "BH" in colunas_norm:
        if texto_corresponde(texto_total, "NEG") or texto_corresponde(texto_total, "NEGATIVO"):
            return proxima_regra_por_codigo("736")
        if texto_corresponde(texto_total, "RJ") and "50" in texto_total:
            return proxima_regra_por_codigo("364")
        return proxima_regra_por_codigo("176")

    if "1" in colunas_norm and "100" in texto_total:
        return proxima_regra_por_codigo("402")

    for regra in REGRAS_EVENTOS:
        for palavra in regra.palavras_chave:
            if texto_corresponde(texto_total, palavra):
                return regra

    return None


def calcular_referencias(
    data_base: Optional[date] = None,
    usar_mes_anterior: bool = False,
) -> tuple[str, str, str]:
    data_base = data_base or date.today()
    ano = data_base.year
    mes = data_base.month

    if usar_mes_anterior:
        if mes == 1:
            mes = 12
            ano -= 1
        else:
            mes -= 1

    ultimo_dia = monthrange(ano, mes)[1]
    referencia1 = f"01{mes:02d}{str(ano)[-2:]}"
    referencia2 = f"{ultimo_dia:02d}{mes:02d}{str(ano)[-2:]}"
    competencia = f"{mes:02d}/{ano}"
    return referencia1, referencia2, competencia


def precisa_perguntar_competencia(data_base: Optional[date] = None) -> bool:
    data_base = data_base or date.today()
    return data_base.day <= 15


def horas_para_minutos(valor) -> int:
    """
    Regras do RMtech:
    - 05:47 -> 5h47
    - 05:37:00 -> 5h37
    - 159:49 -> 159h49
    - 1 days 01:43:00 -> 25h43
    - timedelta/pandas -> total em horas e minutos
    - decimal (1.5) -> 1h30
    """
    if pd.isna(valor):
        raise ValueError("Valor de horas vazio.")

    if hasattr(valor, "total_seconds"):
        total_min = int(round(valor.total_seconds() / 60))
        return total_min

    s = str(valor).strip()
    if not s:
        raise ValueError("Valor de horas vazio.")

    s_lower = s.lower().replace(",", " ").strip()

    m_days = re.match(r"^(?P<days>\d+)\s+days?\s+(?P<time>\d{1,}:\d{2}(:\d{2})?)$", s_lower)
    if m_days:
        days = int(m_days.group("days"))
        time_part = m_days.group("time")
        partes = time_part.split(":")
        if len(partes) == 2:
            h, m = map(int, partes)
            sec = 0
        else:
            h, m, sec = map(int, partes)
        return days * 24 * 60 + h * 60 + m + int(round(sec / 60))

    if ":" in s:
        partes = s.split(":")
        if len(partes) == 2:
            h = int(partes[0])
            m = int(partes[1])
            return h * 60 + m
        elif len(partes) == 3:
            h = int(partes[0])
            m = int(partes[1])
            sec = int(partes[2])
            return h * 60 + m + int(round(sec / 60))
        else:
            raise ValueError(f"Formato de horas inválido: '{valor}'")

    s = s.replace(",", ".")
    return int(round(float(s) * 60))


def minutos_em_formato_alterdata(minutos: int) -> str:
    return str(int(minutos) * 100).zfill(14)


def normalizar_numero_brasileiro(valor) -> str:
    if valor is None:
        return ""

    s = str(valor).strip()
    if not s:
        return ""

    s = s.replace("R$", "").replace(" ", "")

    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    return s


def arredondar_monetario_sempre_para_cima(valor) -> Decimal:
    s = normalizar_numero_brasileiro(valor)

    if s == "":
        raise ValueError("Valor monetário vazio.")

    try:
        dec = Decimal(s)
    except (InvalidOperation, ValueError):
        raise ValueError(f"Valor monetário inválido: {valor}")

    return dec.quantize(Decimal("0.01"), rounding=ROUND_CEILING)


def dinheiro_em_formato_alterdata(valor) -> str:
    """
    Regra monetária:
    - arredondar sempre para cima na 2ª casa decimal
    - aplicar somente para valores em R$
    Exemplos:
        122,175 -> 00000000012218
        122,171 -> 00000000012218
        122,170 -> 00000000012217
    """
    if pd.isna(valor):
        raise ValueError("Valor monetário vazio.")

    valor_decimal = arredondar_monetario_sempre_para_cima(valor)
    centavos = int((valor_decimal * 100).to_integral_value(rounding=ROUND_CEILING))
    return str(centavos).zfill(14)


def limpar_matricula(valor: object) -> str:
    s = str(valor).strip()
    s = s.split(".")[0]
    s = re.sub(r"\D", "", s)
    return s.zfill(6)


def montar_linha_txt(
    sequencial: int,
    codigo_empresa: str,
    referencia1: str,
    referencia2: str,
    codigo_evento: str,
    valor_evento: str,
    matricula: str,
    processo: str = "F",
    cnpj_empresa: str = "",
    pis_funcionario: str = "",
    departamento: str = "0000",
) -> str:
    return (
        str(sequencial).zfill(6) +
        codigo_empresa.zfill(5) +
        referencia1.zfill(6) +
        referencia2.zfill(6) +
        "000000" +
        "000000" +
        "00" +
        codigo_evento.zfill(3) +
        valor_evento +
        matricula +
        processo +
        str(cnpj_empresa).zfill(14) +
        str(pis_funcionario).zfill(11) +
        departamento.zfill(4) +
        "0" * 14 +
        "0000" +
        "00000" +
        "0" * 11 +
        "NNNN"
    )


def gerar_txt_por_dataframe(
    df: pd.DataFrame,
    codigo_empresa: str,
    codigo_evento: str,
    referencia1: str,
    referencia2: str,
    tipo_padrao: TipoValor,
    coluna_matricula: str,
    coluna_valor: str,
    coluna_tipo: Optional[str] = None,
) -> list[str]:
    linhas: list[str] = []
    df = df.dropna(how="all")

    for _, row in df.iterrows():
        if pd.isna(row[coluna_matricula]) or pd.isna(row[coluna_valor]):
            continue

        matricula = limpar_matricula(row[coluna_matricula])
        if not matricula or matricula == "000000":
            continue

        tipo_linha = tipo_padrao
        if coluna_tipo and coluna_tipo in df.columns and not pd.isna(row[coluna_tipo]):
            tipo_linha = str(row[coluna_tipo]).strip().upper()

        valor_original = row[coluna_valor]

        if tipo_linha == "H":
            valor_evento = minutos_em_formato_alterdata(
                horas_para_minutos(valor_original)
            )
        else:
            valor_evento = dinheiro_em_formato_alterdata(valor_original)

        linhas.append(
            montar_linha_txt(
                sequencial=len(linhas) + 1,
                codigo_empresa=codigo_empresa,
                referencia1=referencia1,
                referencia2=referencia2,
                codigo_evento=codigo_evento,
                valor_evento=valor_evento,
                matricula=matricula,
            )
        )

    return linhas


def gerar_txt_por_dataframe_evento_por_linha(
    df: pd.DataFrame,
    codigo_empresa: str,
    referencia1: str,
    referencia2: str,
    coluna_matricula: str,
    coluna_valor: str,
    coluna_texto_evento: str,
    tipo_padrao: TipoValor,
    contexto: str,
) -> tuple[list[str], list[str]]:
    linhas: list[str] = []
    nao_reconhecidas: list[str] = []
    df = df.dropna(how="all")

    for _, row in df.iterrows():
        if pd.isna(row[coluna_matricula]) or pd.isna(row[coluna_valor]):
            continue

        matricula = limpar_matricula(row[coluna_matricula])
        if not matricula or matricula == "000000":
            continue

        texto_evento = ""
        if coluna_texto_evento in df.columns and not pd.isna(row[coluna_texto_evento]):
            texto_evento = str(row[coluna_texto_evento]).strip()

        codigo_padrao = "523" if contexto == "REEMBOLSO" else "600"
        codigo_evento = classificar_por_contexto(texto_evento, contexto, codigo_padrao)

        if codigo_evento == codigo_padrao:
            nao_reconhecidas.append(f"{contexto}: '{texto_evento}' -> {codigo_padrao}")

        valor_original = row[coluna_valor]

        if tipo_padrao == "H":
            valor_evento = minutos_em_formato_alterdata(
                horas_para_minutos(valor_original)
            )
        else:
            valor_evento = dinheiro_em_formato_alterdata(valor_original)

        linha = montar_linha_txt(
            sequencial=len(linhas) + 1,
            codigo_empresa=codigo_empresa,
            referencia1=referencia1,
            referencia2=referencia2,
            codigo_evento=codigo_evento,
            valor_evento=valor_evento,
            matricula=matricula,
        )

        linhas.append(linha)

    return linhas, nao_reconhecidas


def ler_excel_temporario(arquivo: UploadFile) -> Path:
    sufixo = Path(arquivo.filename or "arquivo.xlsx").suffix or ".xlsx"
    with tempfile.NamedTemporaryFile(delete=False, suffix=sufixo) as tmp:
        tmp.write(arquivo.file.read())
        return Path(tmp.name)


def detectar_evento_inteligente(df: pd.DataFrame, nome_aba: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    nome_aba_norm = normalizar_texto(nome_aba)
    texto = texto_amostra_df(df)

    if deve_ignorar_aba(nome_aba):
        return None, None, "IGNORAR"

    if (
        texto_corresponde(nome_aba_norm, "REEMBOLSO") or
        texto_corresponde(texto, "REEMBOLSO") or
        texto_corresponde(nome_aba_norm, "REMBOLSO") or
        texto_corresponde(texto, "REMBOLSO")
    ):
        return "VARIOS", "R$", "REEMBOLSO"

    if (
        texto_corresponde(nome_aba_norm, "DESCONTO") and
        not texto_corresponde(nome_aba_norm, "ALIMENT") and
        not texto_corresponde(nome_aba_norm, "VT") and
        not texto_corresponde(nome_aba_norm, "SIND")
    ):
        return "VARIOS", "R$", "DESCONTO"

    regra = detectar_regra_por_nome_aba(nome_aba)
    if not regra:
        regra = detectar_regra_por_conteudo(df, nome_aba)

    if regra:
        return regra.codigo_evento, regra.tipo_padrao, regra.nome_regra

    return None, None, "NAO_IDENTIFICADO"


def obter_colunas_para_evento(df: pd.DataFrame, codigo_evento: str, tipo_padrao: TipoValor) -> tuple[Optional[str], Optional[str], Optional[str]]:
    coluna_matricula = encontrar_coluna(df, COLUNAS_MATRICULA_PADRAO)
    coluna_tipo = encontrar_coluna(df, ["TIPO"])

    if tipo_padrao == "H":
        coluna_valor = encontrar_coluna(df, COLUNAS_VALOR_H + COLUNAS_VALOR_R)
    else:
        coluna_valor = encontrar_coluna(df, COLUNAS_VALOR_R + COLUNAS_VALOR_H)

    return coluna_matricula, coluna_valor, coluna_tipo


def analisar_abas(path_excel: Path) -> tuple[list[dict], list[dict]]:
    reconhecidas = []
    ignoradas = []

    with pd.ExcelFile(path_excel) as xls:
        for aba in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=aba).dropna(how="all")
            if df.empty:
                continue

            codigo_evento, tipo_sugerido, nome_regra = detectar_evento_inteligente(df, aba)

            item = {
                "aba": aba,
                "colunas": [str(c) for c in df.columns.tolist()],
                "linhas": int(len(df)),
                "evento_sugerido": codigo_evento,
                "tipo_sugerido": tipo_sugerido,
                "nome_regra": nome_regra,
            }

            if nome_regra == "IGNORAR":
                ignoradas.append(item)
            else:
                reconhecidas.append(item)

    return reconhecidas, ignoradas


def gerar_txt_de_uma_aba(
    path_excel: Path,
    aba: str,
    codigo_empresa: str,
    usar_mes_anterior: bool = False,
) -> dict:
    df = pd.read_excel(path_excel, sheet_name=aba).dropna(how="all")
    df.columns = [str(col).strip() for col in df.columns]

    codigo_evento, tipo_padrao, nome_regra = detectar_evento_inteligente(df, aba)

    if nome_regra == "IGNORAR":
        raise HTTPException(status_code=400, detail=f"A aba '{aba}' está configurada para ser ignorada.")

    if not tipo_padrao:
        raise HTTPException(status_code=400, detail=f"Não consegui identificar automaticamente o tipo da aba '{aba}'.")

    referencia1, referencia2, competencia = calcular_referencias(usar_mes_anterior=usar_mes_anterior)

    coluna_matricula, coluna_valor, coluna_tipo = obter_colunas_para_evento(
        df,
        codigo_evento or "",
        tipo_padrao
    )

    if not coluna_matricula or not coluna_valor:
        raise HTTPException(
            status_code=400,
            detail=f"Não foi possível identificar colunas da aba '{aba}'. Colunas encontradas: {list(df.columns)}",
        )

    if nome_regra == "DESCONTO":
        coluna_status = encontrar_coluna(df, ["STATUS"])
        if not coluna_status:
            raise HTTPException(
                status_code=400,
                detail=f"A aba '{aba}' foi detectada como DESCONTO, mas não encontrei a coluna STATUS."
            )

        linhas, nao_reconhecidas = gerar_txt_por_dataframe_evento_por_linha(
            df=df,
            codigo_empresa=codigo_empresa,
            referencia1=referencia1,
            referencia2=referencia2,
            coluna_matricula=coluna_matricula,
            coluna_valor=coluna_valor,
            coluna_texto_evento=coluna_status,
            tipo_padrao="R$",
            contexto="DESCONTO",
        )

        nome_saida = f"DESCONTO_{nome_arquivo_seguro(aba)}_{competencia.replace('/', '_')}.txt"

        return {
            "modo": "SIMPLES",
            "arquivo_saida": nome_saida,
            "competencia": competencia,
            "aba": aba,
            "codigo_evento": "VARIOS",
            "tipo_padrao": "R$",
            "nome_regra": nome_regra,
            "coluna_matricula": coluna_matricula,
            "coluna_valor": coluna_valor,
            "qtd_linhas": len(linhas),
            "preview": linhas[:5],
            "conteudo": "\n".join(linhas),
            "nao_reconhecidas": nao_reconhecidas,
        }

    if nome_regra == "REEMBOLSO":
        coluna_obs = encontrar_coluna(df, ["OBSERVAÇÃO", "OBSERVACAO", "OBS", "STATUS"])
        if not coluna_obs:
            raise HTTPException(
                status_code=400,
                detail=f"A aba '{aba}' foi detectada como REEMBOLSO, mas não encontrei a coluna OBSERVAÇÃO/STATUS."
            )

        linhas, nao_reconhecidas = gerar_txt_por_dataframe_evento_por_linha(
            df=df,
            codigo_empresa=codigo_empresa,
            referencia1=referencia1,
            referencia2=referencia2,
            coluna_matricula=coluna_matricula,
            coluna_valor=coluna_valor,
            coluna_texto_evento=coluna_obs,
            tipo_padrao="R$",
            contexto="REEMBOLSO",
        )

        nome_saida = f"REEMBOLSO_{nome_arquivo_seguro(aba)}_{competencia.replace('/', '_')}.txt"

        return {
            "modo": "SIMPLES",
            "arquivo_saida": nome_saida,
            "competencia": competencia,
            "aba": aba,
            "codigo_evento": "VARIOS",
            "tipo_padrao": "R$",
            "nome_regra": nome_regra,
            "coluna_matricula": coluna_matricula,
            "coluna_valor": coluna_valor,
            "qtd_linhas": len(linhas),
            "preview": linhas[:5],
            "conteudo": "\n".join(linhas),
            "nao_reconhecidas": nao_reconhecidas,
        }

    if not codigo_evento:
        raise HTTPException(status_code=400, detail=f"Não consegui identificar automaticamente o evento da aba '{aba}'.")

    linhas = gerar_txt_por_dataframe(
        df=df,
        codigo_empresa=codigo_empresa,
        codigo_evento=codigo_evento,
        referencia1=referencia1,
        referencia2=referencia2,
        tipo_padrao=tipo_padrao,
        coluna_matricula=coluna_matricula,
        coluna_valor=coluna_valor,
        coluna_tipo=coluna_tipo,
    )

    nome_saida = f"{codigo_evento}_{nome_arquivo_seguro(aba)}_{competencia.replace('/', '_')}.txt"

    return {
        "modo": "SIMPLES",
        "arquivo_saida": nome_saida,
        "competencia": competencia,
        "aba": aba,
        "codigo_evento": codigo_evento,
        "tipo_padrao": tipo_padrao,
        "nome_regra": nome_regra,
        "coluna_matricula": coluna_matricula,
        "coluna_valor": coluna_valor,
        "qtd_linhas": len(linhas),
        "preview": linhas[:5],
        "conteudo": "\n".join(linhas),
        "nao_reconhecidas": [],
    }


HTML_INICIAL = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>RMtech</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    :root{
      --bg:#07111f;
      --card:#0d1b2a;
      --line:#233a5a;
      --text:#e9f1ff;
      --muted:#98abc8;
      --c1:#27b6ff;
      --c2:#8b5cf6;
      --ok:#19c37d;
      --warn:#f59e0b;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family:Segoe UI,Arial,sans-serif;
      background:linear-gradient(135deg,#06101c,#0d1b2a 55%,#111827);
      color:var(--text);
      min-height:100vh;
    }
    .wrap{max-width:1150px;margin:0 auto;padding:32px 20px}
    .hero{display:flex;align-items:center;gap:18px;margin-bottom:24px}
    .logo{
      width:64px;height:64px;border-radius:18px;
      background:linear-gradient(135deg,var(--c1),var(--c2));
      display:flex;align-items:center;justify-content:center;
      font-weight:800;font-size:28px;color:white;
      box-shadow:0 12px 30px rgba(39,182,255,.22);
    }
    h1{margin:0;font-size:38px}
    .sub{color:var(--muted);margin-top:6px}
    .grid{display:grid;grid-template-columns:1.15fr .85fr;gap:20px}
    .card{
      background:rgba(13,27,42,.92);
      border:1px solid var(--line);
      border-radius:22px;
      padding:22px;
      box-shadow:0 18px 40px rgba(0,0,0,.22);
    }
    .drop{
      border:2px dashed #34537d;
      border-radius:18px;
      padding:28px;
      text-align:center;
      background:rgba(19,36,58,.6);
    }
    .drop input{margin-top:12px}
    label{display:block;font-size:14px;color:var(--muted);margin:14px 0 8px}
    input,select,button{
      width:100%;border-radius:14px;border:1px solid var(--line);
      background:#0a1625;color:var(--text);padding:14px 14px;font-size:15px;
    }
    button{
      background:linear-gradient(135deg,var(--c1),var(--c2));
      border:none;font-weight:700;cursor:pointer;
    }
    button.sec{
      background:#11233a;border:1px solid var(--line);
    }
    .row{display:grid;grid-template-columns:1fr 1fr;gap:14px}
    .actions{display:flex;gap:12px;margin-top:16px}
    .actions button{width:auto;min-width:180px}
    .result,.sheet{
      background:rgba(19,36,58,.75);
      border:1px solid var(--line);
      border-radius:16px;
      padding:14px 16px;
      margin-top:12px;
    }
    .badge{
      display:inline-block;padding:4px 10px;border-radius:999px;
      background:rgba(39,182,255,.12);border:1px solid rgba(39,182,255,.35);
      color:#b7e7ff;font-size:12px;margin-right:8px
    }
    .ok{color:#86efac}
    .warn{color:#fcd34d}
    .muted{color:var(--muted)}
    pre{
      white-space:pre-wrap;word-break:break-word;
      background:#07111f;border:1px solid var(--line);
      padding:12px;border-radius:14px;max-height:260px;overflow:auto;
    }
    @media (max-width:900px){
      .grid{grid-template-columns:1fr}
      .row{grid-template-columns:1fr}
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="logo">RM</div>
      <div>
        <h1>RMtech</h1>
        <div class="sub">Importador inteligente Alterdata • V4.2</div>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2 style="margin-top:0">Gerar arquivos</h2>
        <div class="drop">
          <div style="font-size:18px;font-weight:700">Selecione a planilha da empresa</div>
          <div class="muted" style="margin-top:8px">Agrupada ou solta. O RMtech tenta reconhecer tudo automaticamente.</div>
          <input type="file" id="arquivo" accept=".xlsx,.xls">
        </div>

        <div class="row">
          <div>
            <label>Código da empresa</label>
            <input id="codigo_empresa" value="00469">
          </div>
          <div>
            <label>Competência</label>
            <select id="usar_mes_anterior">
              <option value="false">Mês atual</option>
              <option value="true">Mês anterior</option>
            </select>
          </div>
        </div>

        <div class="actions">
          <button type="button" onclick="analisarPlanilha()">Analisar planilha</button>
          <button type="button" class="sec" onclick="gerarTodos()">Gerar todos em ZIP</button>
        </div>

        <div id="resultado"></div>
      </div>

      <div class="card">
        <h2 style="margin-top:0">Inteligência ativa</h2>
        <div class="result"><span class="badge">1</span> Regra fixa de horas e minutos</div>
        <div class="result"><span class="badge">2</span> Normalização forte de texto</div>
        <div class="result"><span class="badge">3</span> Sinônimos e equivalências</div>
        <div class="result"><span class="badge">4</span> Contexto por aba: reembolso/desconto</div>
        <div class="result"><span class="badge">5</span> Prioridade do específico para o genérico</div>
        <div class="result"><span class="badge">6</span> DESCONTO e REEMBOLSO usam evento por linha</div>
        <div class="result"><span class="badge">7</span> Resumo de textos não reconhecidos</div>
        <div class="result"><span class="badge">8</span> Arredondamento monetário sempre para cima</div>
      </div>
    </div>
  </div>

<script>
async function analisarPlanilha() {
  const arquivo = document.getElementById('arquivo').files[0];
  const resultado = document.getElementById('resultado');
  if (!arquivo) {
    alert('Selecione uma planilha.');
    return;
  }

  resultado.innerHTML = '<div class="result">Analisando...</div>';

  const formData = new FormData();
  formData.append('arquivo', arquivo);

  const resp = await fetch('/analisar-planilha', { method:'POST', body: formData });
  const data = await resp.json();

  if (!resp.ok) {
    resultado.innerHTML = '<div class="result"><b>Erro:</b><pre>' + JSON.stringify(data, null, 2) + '</pre></div>';
    return;
  }

  let html = '<div class="result"><b class="ok">Análise concluída</b></div>';

  if (data.reconhecidas && data.reconhecidas.length > 0) {
    html += '<div class="result"><b>Abas reconhecidas</b></div>';
    data.reconhecidas.forEach(item => {
      html += `
        <div class="sheet">
          <div><b>${item.aba}</b></div>
          <div class="muted">Colunas: ${item.colunas.join(', ')}</div>
          <div class="muted">Linhas: ${item.linhas}</div>
          <div style="margin-top:8px">
            <span class="badge">Evento ${item.evento_sugerido || 'não identificado'}</span>
            <span class="badge">${item.tipo_sugerido || 'sem tipo'}</span>
            <span class="muted">${item.nome_regra || ''}</span>
          </div>
        </div>
      `;
    });
  }

  if (data.ignoradas && data.ignoradas.length > 0) {
    html += '<div class="result"><b class="warn">Abas ignoradas</b></div>';
    data.ignoradas.forEach(item => {
      html += `
        <div class="sheet">
          <div><b>${item.aba}</b></div>
          <div class="muted">Motivo: ignorar automaticamente</div>
        </div>
      `;
    });
  }

  resultado.innerHTML = html;
}

async function gerarTodos() {
  const arquivo = document.getElementById('arquivo').files[0];
  const codigoEmpresa = document.getElementById('codigo_empresa').value.trim();
  const usarMesAnterior = document.getElementById('usar_mes_anterior').value;
  const resultado = document.getElementById('resultado');

  if (!arquivo) {
    alert('Selecione uma planilha.');
    return;
  }
  if (!codigoEmpresa) {
    alert('Informe o código da empresa.');
    return;
  }

  resultado.innerHTML = '<div class="result">Gerando ZIP...</div>';

  const formData = new FormData();
  formData.append('arquivo', arquivo);
  formData.append('codigo_empresa', codigoEmpresa);
  formData.append('usar_mes_anterior', usarMesAnterior);

  const resp = await fetch('/gerar-todos-zip', { method:'POST', body: formData });

  if (!resp.ok) {
    const txt = await resp.text();
    resultado.innerHTML = '<div class="result"><b>Erro:</b><pre>' + txt + '</pre></div>';
    return;
  }

  const blob = await resp.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'RMtech_Arquivos_Alterdata.zip';
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);

  resultado.innerHTML = '<div class="result"><b class="ok">ZIP gerado com sucesso.</b><div class="muted">O download foi iniciado.</div></div>';
}
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home():
    return HTML_INICIAL


@app.get("/health")
def healthcheck():
    ref1, ref2, comp = calcular_referencias()
    return {
        "sistema": "RMtech",
        "versao": "0.4.2",
        "competencia_sugerida": comp,
        "referencia1": ref1,
        "referencia2": ref2,
        "perguntar_mes": precisa_perguntar_competencia(),
    }


@app.post("/analisar-planilha")
async def analisar_planilha(arquivo: UploadFile = File(...)):
    temp_path: Optional[Path] = None
    try:
        temp_path = ler_excel_temporario(arquivo)
        reconhecidas, ignoradas = analisar_abas(temp_path)
        return JSONResponse({"reconhecidas": reconhecidas, "ignoradas": ignoradas})
    except Exception as e:
        erro = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e}\n\n{erro}")
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


@app.post("/gerar-txt")
async def gerar_txt(
    arquivo: UploadFile = File(...),
    aba: str = Form(...),
    codigo_empresa: str = Form(...),
    usar_mes_anterior: bool = Form(False),
):
    temp_path: Optional[Path] = None
    try:
        temp_path = ler_excel_temporario(arquivo)
        resultado = gerar_txt_de_uma_aba(temp_path, aba, codigo_empresa, usar_mes_anterior)
        return JSONResponse(resultado)
    except HTTPException:
        raise
    except Exception as e:
        erro = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e}\n\n{erro}")
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


@app.post("/gerar-todos-zip")
async def gerar_todos_zip(
    arquivo: UploadFile = File(...),
    codigo_empresa: str = Form(...),
    usar_mes_anterior: bool = Form(False),
):
    temp_path: Optional[Path] = None
    try:
        temp_path = ler_excel_temporario(arquivo)
        reconhecidas, ignoradas = analisar_abas(temp_path)

        buffer_zip = io.BytesIO()

        with zipfile.ZipFile(buffer_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            resumo = []
            nao_reconhecidas_total = []

            for item in reconhecidas:
                aba = item["aba"]
                if not item.get("evento_sugerido") and item.get("nome_regra") not in ["DESCONTO", "REEMBOLSO"]:
                    continue

                resultado = gerar_txt_de_uma_aba(
                    temp_path,
                    aba=aba,
                    codigo_empresa=codigo_empresa,
                    usar_mes_anterior=usar_mes_anterior,
                )

                zf.writestr(resultado["arquivo_saida"], resultado["conteudo"])
                resumo.append(
                    f"{resultado['arquivo_saida']} | aba={aba} | regra={resultado['nome_regra']} | linhas={resultado['qtd_linhas']}"
                )

                if resultado.get("nao_reconhecidas"):
                    nao_reconhecidas_total.extend(resultado["nao_reconhecidas"])

            if ignoradas:
                resumo.append("")
                resumo.append("ABAS IGNORADAS:")
                for item in ignoradas:
                    resumo.append(f"{item['aba']}")

            if nao_reconhecidas_total:
                resumo.append("")
                resumo.append("TEXTOS COM FALLBACK:")
                for item in nao_reconhecidas_total:
                    resumo.append(item)

            zf.writestr("resumo_geracao.txt", "\n".join(resumo) if resumo else "Nenhum arquivo gerado.")

        buffer_zip.seek(0)

        return StreamingResponse(
            buffer_zip,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="RMtech_Arquivos_Alterdata.zip"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        erro = traceback.format_exc()
        raise HTTPException(status_code=500, detail=f"{e}\n\n{erro}")
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
