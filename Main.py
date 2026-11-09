import argparse
import csv
import os
import sys
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

SEP = ";"
ENCODING = "latin-1"

NOME_ARQUIVO = "votacao_candidato_munzona_2026_SP.csv"
PASTA_LOCAL = r"C:\Users\delio\Downloads\votacao_candidato_munzona_2026"
MUNICIPIO_PADRAO = "BAURU"


def obter_caminho_arquivo():
    if os.path.exists(NOME_ARQUIVO):
        return NOME_ARQUIVO
    caminho_downloads = os.path.join(PASTA_LOCAL, NOME_ARQUIVO)
    if os.path.exists(caminho_downloads):
        return caminho_downloads
    return NOME_ARQUIVO


def normalizar_texto(texto):
    if not texto:
        return ""
    return unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("ASCII").strip().upper()


def ler_cabecalho(caminho):
    with open(caminho, "r", encoding=ENCODING, newline="") as f:
        cabecalho = [col.strip().strip('"') for col in f.readline().strip().split(SEP)]
    return {nome: i for i, nome in enumerate(cabecalho)}


def dividir_intervalos(caminho, n):
    tam = os.path.getsize(caminho)
    passo = max(1, tam // n)
    limites = [i * passo for i in range(n)] + [tam]
    return list(zip(limites[:-1], limites[1:]))


def _linhas_do_intervalo(caminho, inicio, fim):
    with open(caminho, "rb") as f:
        if inicio > 0:
            f.seek(inicio - 1)
            f.readline()
        while f.tell() < fim:
            linha = f.readline()
            if not linha:
                break
            yield linha.decode(ENCODING, errors="ignore")


def processar_bloco(args):
    caminho, inicio, fim, idx, municipio_norm = args

    i_votos = idx.get("QT_VOTOS_NOMINAIS_VALIDOS") or idx.get("QT_VOTOS_NOMINAIS") or idx.get("QT_VOTOS")
    i_partido = idx.get("SG_PARTIDO")
    i_cargo = idx.get("DS_CARGO")
    i_municipio = idx.get("NM_MUNICIPIO")

    total_votos = 0
    total_registros = 0
    por_partido = defaultdict(lambda: [0, 0])
    por_cargo = defaultdict(lambda: [0, 0])

    for linha in _linhas_do_intervalo(caminho, inicio, fim):
        linha = linha.strip()
        if not linha or linha.startswith("DT_GERACAO"):
            continue

        campos = [c.strip('"') for c in linha.split(SEP)]
        if len(campos) <= max(i_votos, i_partido, i_cargo, i_municipio):
            continue

        if municipio_norm:
            municipio_linha = normalizar_texto(campos[i_municipio])
            if municipio_linha != municipio_norm:
                continue

        try:
            votos = int(campos[i_votos].strip())
            if votos < 0:
                votos = 0
        except ValueError:
            continue

        partido = campos[i_partido].strip()
        cargo = campos[i_cargo].strip()

        total_votos += votos
        total_registros += 1

        por_partido[partido][0] += votos
        por_partido[partido][1] += 1

        por_cargo[cargo][0] += votos
        por_cargo[cargo][1] += 1

    return total_votos, total_registros, dict(por_partido), dict(por_cargo)


def combinar(parciais):
    soma_votos = total_registros = 0
    partidos = defaultdict(lambda: [0, 0])
    cargos = defaultdict(lambda: [0, 0])

    for s_votos, n_reg, p_dict, c_dict in parciais:
        soma_votos += s_votos
        total_registros += n_reg

        for k, (v, count) in p_dict.items():
            partidos[k][0] += v
            partidos[k][1] += count

        for k, (v, count) in c_dict.items():
            cargos[k][0] += v
            cargos[k][1] += count

    medias_partido = {k: round(a / b, 4) for k, (a, b) in partidos.items() if b > 0}
    medias_cargo = {k: round(a / b, 4) for k, (a, b) in cargos.items() if b > 0}

    return {
        "media_geral": round(soma_votos / total_registros, 4) if total_registros > 0 else 0.0,
        "registros": total_registros,
        "medias_partido": medias_partido,
        "medias_cargo": medias_cargo,
    }


def executar_processos(caminho, n_workers, municipio_norm):
    idx = ler_cabecalho(caminho)
    tarefas = [(caminho, i, f, idx, municipio_norm) for i, f in dividir_intervalos(caminho, n_workers)]
    with Pool(processes=n_workers) as pool:
        parciais = pool.map(processar_bloco, tarefas)
    return combinar(parciais)


def executar_threads(caminho, n_workers, municipio_norm):
    idx = ler_cabecalho(caminho)
    tarefas = [(caminho, i, f, idx, municipio_norm) for i, f in dividir_intervalos(caminho, n_workers)]
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        parciais = list(pool.map(processar_bloco, tarefas))
    return combinar(parciais)


def main():
    ap = argparse.ArgumentParser(description="Processamento Concorrente - Eleições")
    ap.add_argument("ficheiro", nargs="?", default=obter_caminho_arquivo())
    ap.add_argument("--municipio", type=str, default=MUNICIPIO_PADRAO)
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 4)
    args = ap.parse_args()

    caminho = args.ficheiro
    if not os.path.exists(caminho):
        print(f"Erro: Arquivo não encontrado: {caminho}")
        sys.exit(1)

    municipio_norm = normalizar_texto(args.municipio)

    print("=" * 70)
    print(" SISTEMA DE PROCESSAMENTO CONCORRENTE (ELEIÇÕES SP)")
    print("=" * 70)
    print(f"Arquivo analisado   : {os.path.basename(caminho)}")
    print(f"Município escolhido : {args.municipio.upper()}")
    print(f"Workers configurados: {args.workers}")
    print("=" * 70)

    print("\nExecutando com PROCESSOS (Multiprocessing)...")
    t0_proc = time.perf_counter()
    res_proc = executar_processos(caminho, args.workers, municipio_norm)
    t_proc = time.perf_counter() - t0_proc
    print(f"Concluído em: {t_proc:.4f} s")

    print("\nExecutando com THREADS (Multithreading)...")
    t0_thr = time.perf_counter()
    res_thr = executar_threads(caminho, args.workers, municipio_norm)
    t_thr = time.perf_counter() - t0_thr
    print(f"Concluído em: {t_thr:.4f} s")

    assert res_proc == res_thr, "Erro de consistência entre abordagens!"

    r = res_proc
    if r["registros"] == 0:
        print(f"\nAviso: Nenhum registro encontrado para '{args.municipio}'.")
        return

    melhor_partido = max(r["medias_partido"], key=r["medias_partido"].get)
    melhor_cargo = max(r["medias_cargo"], key=r["medias_cargo"].get)

    print("\n" + "=" * 70)
    print(f" RESULTADOS FINAIS - MUNICÍPIO: {args.municipio.upper()}")
    print("=" * 70)
    print(f"Total de registros analisados : {r['registros']:,}")
    print(f"1. Média geral do município   : {r['media_geral']:.4f} votos nominais válidos")
    print(f"2. Partido com maior média    : {melhor_partido} ({r['medias_partido'][melhor_partido]:.4f} votos)")
    print(f"3. Cargo com maior média      : {melhor_cargo} ({r['medias_cargo'][melhor_cargo]:.4f} votos)")
    print("-" * 70)
    print(" TEMPO TOTAL DE EXECUÇÃO:")
    print(f" • Versão com Processos : {t_proc:.4f} s")
    print(f" • Versão com Threads   : {t_thr:.4f} s")
    print("-" * 70)

    if t_proc < t_thr:
        fator = t_thr / t_proc if t_proc > 0 else 0
        print(f" DESEMPENHO: A abordagem com PROCESSOS obteve o menor tempo ({fator:.2f}x mais rápida)!")
    elif t_thr < t_proc:
        fator = t_proc / t_thr if t_thr > 0 else 0
        print(f" DESEMPENHO: A abordagem com THREADS obteve o menor tempo ({fator:.2f}x mais rápida)!")
    else:
        print(" DESEMPENHO: Empate técnico entre processos e threads.")

    print("\nRanking de Médias por Partido:")
    for k, v in sorted(r["medias_partido"].items(), key=lambda x: -x[1]):
        print(f"  {k:<15} : {v:10.4f} votos")

    print("\nRanking de Médias por Cargo:")
    for k, v in sorted(r["medias_cargo"].items(), key=lambda x: -x[1]):
        print(f"  {k:<20} : {v:10.4f} votos")
    print("=" * 70)


if __name__ == "__main__":
    main()