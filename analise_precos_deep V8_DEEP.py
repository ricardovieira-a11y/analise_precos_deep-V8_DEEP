import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import warnings
import json
warnings.filterwarnings('ignore')

class MonitorPrecosCorrigido:
    def __init__(self, pasta_base, prefixo_arquivos, arquivo_caracteristicas=None):
        self.pasta_base = pasta_base
        self.prefixo_arquivos = prefixo_arquivos
        self.arquivo_caracteristicas = arquivo_caracteristicas
        self.df_completo = None
        self.df_resumo = None
        
        # Configurações padrão
        self.config = {
            'variacao_minima': 0.5,
            'dias_alerta_reajuste': 90,
            'faturamento_minimo': 1000,
            'meses_analise': 6
        }
    
    def carregar_configuracao(self):
        """Permite customizar a configuração"""
        print("\n⚙️ CONFIGURAÇÃO ATUAL:")
        print(f"  • Variação mínima considerada: {self.config['variacao_minima']}%")
        print(f"  • Dias para alerta de reajuste: {self.config['dias_alerta_reajuste']}")
        print(f"  • Faturamento mínimo relevante: R$ {self.config['faturamento_minimo']:,.2f}")
        print(f"  • Período análise faturamento: {self.config['meses_analise']} meses")
        
        alterar = input("\nDeseja alterar estas configurações? (s/N): ").lower()
        if alterar == 's':
            try:
                self.config['variacao_minima'] = float(input("Variação mínima (%): ") or 0.5)
                self.config['dias_alerta_reajuste'] = int(input("Dias para alerta reajuste: ") or 90)
                self.config['faturamento_minimo'] = float(input("Faturamento mínimo (R$): ") or 1000)
                self.config['meses_analise'] = int(input("Meses análise faturamento: ") or 6)
            except:
                print("⚠️ Usando configurações padrão")
    
    def carregar_dados(self):
        """Carrega e processa os arquivos CSV"""
        print(f"\n📁 CARREGANDO DADOS...")
        print(f"Pasta: {self.pasta_base}")
        
        if not os.path.exists(self.pasta_base):
            print(f"❌ ERRO: Pasta não existe!")
            return None
        
        arquivos_csv = []
        for arquivo in os.listdir(self.pasta_base):
            if arquivo.startswith(self.prefixo_arquivos) and arquivo.endswith(".csv"):
                caminho_completo = os.path.join(self.pasta_base, arquivo)
                arquivos_csv.append(caminho_completo)
        
        if not arquivos_csv:
            print("❌ Nenhum arquivo CSV encontrado!")
            return None
        
        print(f"📊 Encontrados {len(arquivos_csv)} arquivos")
        
        dfs = []
        for arquivo in arquivos_csv:
            try:
                # Tenta utf-8 com ponto como separador de milhares e vírgula como decimal (Como no Power Query)
                df = pd.read_csv(arquivo, delimiter=';', encoding='utf-8', 
                               decimal=',', thousands='.', low_memory=False)
                df['Arquivo'] = os.path.basename(arquivo)
                dfs.append(df)
                print(f"✅ {os.path.basename(arquivo)} - {len(df)} linhas (utf-8)")
            except Exception as e:
                try:
                    # Tenta latin-1 como fallback
                    df = pd.read_csv(arquivo, delimiter=';', encoding='latin-1', 
                                   decimal=',', thousands='.', low_memory=False)
                    df['Arquivo'] = os.path.basename(arquivo)
                    dfs.append(df)
                    print(f"✅ {os.path.basename(arquivo)} - {len(df)} linhas (latin-1)")
                except Exception as e:
                    print(f"❌ Erro em {arquivo}: {e}")
        
        if not dfs:
            return None
        
        self.df_completo = pd.concat(dfs, ignore_index=True)
        self.df_completo.columns = self.df_completo.columns.str.strip()
        
        # Garante que a coluna Total e Quantidade são numéricas
        for col in ['Total', 'Quantidade', 'ICMS', 'PIS Débito', 'Cofins Débito']:
            if col in self.df_completo.columns:
                # O Power Query usa Number.FromText com "pt-BR", que inverte ponto e vírgula.
                # O `thousands='.'` no pd.read_csv deve resolver. Aqui apenas forçamos para numérico.
                self.df_completo[col] = pd.to_numeric(
                    self.df_completo[col], errors='coerce'
                ).fillna(0)

        print(f"\n🎯 DADOS CARREGADOS:")
        print(f"  • Total de registros: {len(self.df_completo):,}")
        print(f"  • Colunas disponíveis: {list(self.df_completo.columns)}")
        
        return self.df_completo
    
    def processar_dados(self):
        """Processa os dados seguindo a lógica do Power Query"""
        print(f"\n⚙️ PROCESSANDO DADOS...")
        
        if self.df_completo is None:
            return None
        
        # Filtra grupos desejados - MESMA LÓGICA DO POWER QUERY
        if 'Grupo' in self.df_completo.columns:
            grupos_validos = ['PRODUTO ACABADO', 'PRODUTO INDUSTRIALIZADO']
            antes = len(self.df_completo)
            self.df_completo = self.df_completo[
                self.df_completo['Grupo'].isin(grupos_validos)
            ]
            print(f"  • Filtro grupos: {antes:,} → {len(self.df_completo):,}")
        
        # Converte datas
        if 'Data NF' in self.df_completo.columns:
            self.df_completo['Data NF'] = pd.to_datetime(
                self.df_completo['Data NF'], format='%d/%m/%Y', errors='coerce'
            )
            print("  • Data NF convertida")
        
        # Calcula PV_NET - MESMA FÓRMULA DO POWER QUERY
        colunas_calc = ['Total', 'ICMS', 'PIS Débito', 'Cofins Débito', 'Quantidade']
        if all(col in self.df_completo.columns for col in colunas_calc):
            # Evita divisão por zero
            self.df_completo['PV_NET'] = self.df_completo.apply(
                lambda x: (x['Total'] - x['ICMS'] - x['PIS Débito'] - x['Cofins Débito']) / x['Quantidade'] 
                if x['Quantidade'] != 0 else 0, 
                axis=1
            ).round(2) # Arredondamento para 2 casas como no Power Query
            print("  • PV_NET calculado")
        
        # Adiciona período
        if 'Data NF' in self.df_completo.columns:
            self.df_completo['AnoMes'] = self.df_completo['Data NF'].dt.strftime('%Y-%m')
            print("  • Períodos adicionados")
        
        # CORREÇÃO SIMPLIFICADA: Carrega características
        if self.arquivo_caracteristicas and os.path.exists(self.arquivo_caracteristicas):
            try:
                print(f"  • Carregando arquivo de características: {self.arquivo_caracteristicas}")
                
                # Carrega o arquivo Excel
                df_carac = pd.read_excel(self.arquivo_caracteristicas)
                df_carac.columns = df_carac.columns.str.strip()
                
                print(f"  • Colunas no arquivo de características: {list(df_carac.columns)}")
                
                # CORREÇÃO: Renomeia diretamente para os nomes que vamos usar
                mapeamento = {}
                if 'Produto' in df_carac.columns:
                    mapeamento['Produto'] = 'COD_PRODUTO_CARAC'
                if 'cliente' in df_carac.columns:
                    mapeamento['cliente'] = 'CLIENTE_CARAC'
                if 'Projeto' in df_carac.columns:
                    mapeamento['Projeto'] = 'PROJETO_CARAC'
                if 'status' in df_carac.columns:
                    mapeamento['status'] = 'STATUS_CARAC'
                
                df_carac = df_carac.rename(columns=mapeamento)
                
                # Seleciona apenas as colunas que existem
                colunas_para_merge = ['COD_PRODUTO_CARAC']
                if 'CLIENTE_CARAC' in df_carac.columns:
                    colunas_para_merge.append('CLIENTE_CARAC')
                if 'PROJETO_CARAC' in df_carac.columns:
                    colunas_para_merge.append('PROJETO_CARAC')
                if 'STATUS_CARAC' in df_carac.columns:
                    colunas_para_merge.append('STATUS_CARAC')
                
                # Realiza o merge apenas com as colunas que existem
                antes_merge = len(self.df_completo)
                self.df_completo = self.df_completo.merge(
                    df_carac[colunas_para_merge], 
                    left_on='Cód. Produto', 
                    right_on='COD_PRODUTO_CARAC', 
                    how='left'
                )
                
                print(f"  • Merge realizado: {antes_merge:,} → {len(self.df_completo):,} registros")
                print(f"  • Colunas usadas no merge: {colunas_para_merge}")
                
            except Exception as e:
                print(f"  ❌ ERRO ao carregar/mesclar características: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"  ⚠️ Arquivo de características não encontrado ou não especificado")
        
        return self.df_completo
    
    def calcular_metricas_corrigidas(self):
        """Calcula métricas com correção no faturamento e adiciona campos de características."""
        print(f"\n📈 CALCULANDO MÉTRICAS CORRIGIDAS...")
        
        if self.df_completo is None:
            return None
        
        df_filtrado = self.df_completo.dropna(subset=['Data NF', 'PV_NET'])
        
        if df_filtrado.empty:
            print("❌ Nenhum dado válido para análise")
            return None
        
        # Períodos de análise
        data_maxima = df_filtrado['Data NF'].max()
        data_limite = data_maxima - pd.DateOffset(months=self.config['meses_analise'])
        
        print(f"  • Período análise: {data_limite.strftime('%d/%m/%Y')} a {data_maxima.strftime('%d/%m/%Y')}")
        
        # Encontra coluna de produto
        coluna_produto = 'Cód. Produto'
        
        resultados = []
        
        for cod_produto, grupo in df_filtrado.groupby(coluna_produto):
            # Garante que grupo não está vazio
            if grupo.empty: continue
            
            grupo = grupo.sort_values('Data NF')
            
            # Informações básicas
            primeira = grupo.iloc[0]
            ultima = grupo.iloc[-1]
            
            # CORREÇÃO: Obtém características do arquivo mesclado
            cliente_carac = "N/A"
            projeto_carac = "N/A"
            
            if 'CLIENTE_CARAC' in grupo.columns:
                clientes_validos = grupo['CLIENTE_CARAC'].dropna()
                if not clientes_validos.empty:
                    cliente_carac = clientes_validos.iloc[0]
                    # Converte para string e limpa
                    cliente_carac = str(cliente_carac).strip()
            
            if 'PROJETO_CARAC' in grupo.columns:
                projetos_validos = grupo['PROJETO_CARAC'].dropna()
                if not projetos_validos.empty:
                    projeto_carac = projetos_validos.iloc[0]
                    # Converte para string e limpa
                    projeto_carac = str(projeto_carac).strip()
            
            # Filtra vendas para o cálculo de variação
            grupo_vendas_validas = grupo[grupo['PV_NET'] > 0]
            if grupo_vendas_validas.empty:
                preco_inicial = preco_atual = 0
                variacao_pct = 0
            else:
                # Preço Inicial/Atual usa a primeira/última venda com preço > 0
                preco_inicial = grupo_vendas_validas.iloc[0]['PV_NET']
                preco_atual = grupo_vendas_validas.iloc[-1]['PV_NET']
                
                if preco_inicial and preco_inicial > 0:
                    variacao_pct = ((preco_atual - preco_inicial) / preco_inicial) * 100
                else:
                    variacao_pct = 0
            
            # Detecção de última variação significativa
            data_ult_var = primeira['Data NF'] # Assume a primeira venda como último reajuste se não houver variação
            dias_sem_reajuste = (datetime.now() - data_ult_var).days
            
            # Busca a última data onde o preço PV_NET mudou significativamente
            grupo_com_prev = grupo_vendas_validas.copy()
            if not grupo_com_prev.empty:
                grupo_com_prev['Preco_Anterior'] = grupo_com_prev['PV_NET'].shift(1)
                
                def variacao_significativa(atual, anterior):
                    if pd.isna(atual) or pd.isna(anterior) or anterior == 0:
                        return False
                    # Usa a variação mínima configurada (0.5% ou o que for definido)
                    return abs(atual - anterior) / anterior >= (self.config['variacao_minima'] / 100)
                
                grupo_com_prev['Variacao_Significativa'] = grupo_com_prev.apply(
                    lambda x: variacao_significativa(x['PV_NET'], x['Preco_Anterior']), axis=1
                )
                
                variacoes_sig = grupo_com_prev[grupo_com_prev['Variacao_Significativa']]
                if not variacoes_sig.empty:
                    data_ult_var = variacoes_sig['Data NF'].max()
                    dias_sem_reajuste = (datetime.now() - data_ult_var).days

            alerta_reajuste = dias_sem_reajuste > self.config['dias_alerta_reajuste']
            
            # ANÁLISE DE FATURAMENTO
            vendas_periodo = grupo[grupo['Data NF'] >= data_limite]
            
            if not vendas_periodo.empty:
                faturamento_total = vendas_periodo['Total'].sum()
                
                # CORREÇÃO: Meses de análise para cálculo da média (usamos o que está configurado)
                meses_no_periodo = self.config['meses_analise']
                media_mensal = faturamento_total / meses_no_periodo
                
                meses_com_venda = vendas_periodo['AnoMes'].nunique()
                clientes_unicos = vendas_periodo['Cód. Cliente'].nunique() if 'Cód. Cliente' in vendas_periodo.columns else 0
                
                # Cliente principal (o cliente que mais comprou no período de análise)
                cliente_principal = vendas_periodo.groupby('Nome')['Total'].sum().idxmax() if 'Nome' in vendas_periodo.columns else "N/A"
                    
                # Projeto principal (o projeto mais vendido no período de análise)
                projeto_principal = vendas_periodo['PROJETO'].mode()[0] if 'PROJETO' in vendas_periodo.columns and not vendas_periodo['PROJETO'].mode().empty else "N/A"
            else:
                faturamento_total = media_mensal = meses_com_venda = clientes_unicos = 0
                cliente_principal = projeto_principal = "N/A"
            
            # Classificação de importância CORRIGIDA
            if faturamento_total >= self.config['faturamento_minimo'] * 10:
                importancia = 'MUITO ALTA'
                prioridade = 1
            elif faturamento_total >= self.config['faturamento_minimo'] * 5:
                importancia = 'ALTA'
                prioridade = 2
            elif faturamento_total >= self.config['faturamento_minimo']:
                importancia = 'MÉDIA'
                prioridade = 3
            else:
                importancia = 'BAIXA'
                prioridade = 4
            
            # Status do produto
            if faturamento_total == 0:
                status = 'INATIVO'
                prioridade = 5
            elif meses_com_venda <= 1:
                status = 'OCASIONAL'
            elif alerta_reajuste:
                status = 'ATENÇÃO'
                prioridade = 1 if importancia in ['MUITO ALTA', 'ALTA'] else 2
            else:
                status = 'NORMAL'
            
            resultados.append({
                'COD_PRODUTO': cod_produto,
                'DESCRICAO': primeira.get('Descrição', 'N/A'),
                
                # CAMPOS USADOS PARA FILTRO (DO ARQUIVO DE CARACTERÍSTICAS)
                'CLIENTE': cliente_carac,
                'PROJETO': projeto_carac,
                
                # Campos calculados (mantidos para referência)
                'CLIENTE_PRINCIPAL': cliente_principal,
                'PROJETO_PRINCIPAL': projeto_principal,
                
                'PRIORIDADE': prioridade,
                
                # Histórico de preços
                'PRIMEIRA_VENDA': primeira['Data NF'],
                'PRECO_INICIAL': preco_inicial,
                'ULTIMA_VENDA': ultima['Data NF'],
                'PRECO_ATUAL': preco_atual,
                'VARIACAO_PCT': variacao_pct,
                
                # Tempo sem reajuste
                'ULTIMO_REAJUSTE': data_ult_var,
                'DIAS_SEM_REAJUSTE': dias_sem_reajuste,
                'ALERTA_REAJUSTE': alerta_reajuste,
                
                # Faturamento CORRIGIDO
                'FATURAMENTO_PERIODO': faturamento_total,
                'MEDIA_MENSAL': media_mensal,
                'MESES_COM_VENDAS': meses_com_venda,
                'CLIENTES_ATENDIDOS': clientes_unicos,
                'IMPORTANCIA': importancia,
                'STATUS': status,
                
                # Detalhes adicionais
                'QTD_VENDAS': len(grupo),
                'VOLUME_TOTAL': grupo['Quantidade'].sum()
            })
        
        self.df_resumo = pd.DataFrame(resultados)
        print(f"  • Produtos analisados: {len(self.df_resumo):,}")
        
        # DEBUG: Mostra estatísticas dos filtros
        if len(self.df_resumo) > 0:
            clientes_unicos = self.df_resumo['CLIENTE'].unique()
            projetos_unicos = self.df_resumo['PROJETO'].unique()
            
            print(f"  • Clientes únicos para filtro: {len([c for c in clientes_unicos if c != 'N/A'])}")
            print(f"  • Projetos únicos para filtro: {len([p for p in projetos_unicos if p != 'N/A'])}")
            
            total_faturamento = self.df_resumo['FATURAMENTO_PERIODO'].sum()
            print(f"  • Faturamento total período: R$ {total_faturamento:,.2f}")
        
        return self.df_resumo
    
    def _calcular_vendas_anuais(self, cod_produto=None):
        """Calcula o faturamento anual para um produto específico ou todos os produtos para o gráfico."""
        if cod_produto:
            # Gráfico para produto específico
            df_prod = self.df_completo[self.df_completo['Cód. Produto'] == cod_produto].copy()
        else:
            # Gráfico global - todos os produtos
            df_prod = self.df_completo.copy()
        
        if df_prod.empty or 'Data NF' not in df_prod.columns or 'Total' not in df_prod.columns:
            return {'anos': [], 'vendas': []}
            
        df_prod['Ano'] = df_prod['Data NF'].dt.year
        
        # Agrupa faturamento por ano e calcula a soma
        vendas_anuais = df_prod.groupby('Ano')['Total'].sum().reset_index()
        
        # Converte para formato de lista para injeção em JS
        return {
            'anos': vendas_anuais['Ano'].astype(str).tolist(),
            'vendas': vendas_anuais['Total'].round(0).tolist()
        }
    
    def _calcular_faturamento_filtrado(self, filtros):
        """Calcula o faturamento total baseado nos filtros aplicados."""
        df_filtrado = self.df_resumo.copy()
        
        # Aplica os mesmos filtros que são aplicados na tabela
        if filtros.get('status') != 'todos':
            df_filtrado = df_filtrado[df_filtrado['STATUS'] == filtros['status']]
        
        if filtros.get('importancia') != 'todos':
            df_filtrado = df_filtrado[df_filtrado['IMPORTANCIA'] == filtros['importancia']]
        
        if filtros.get('cliente') != 'todos':
            df_filtrado = df_filtrado[df_filtrado['CLIENTE'] == filtros['cliente']]
        
        if filtros.get('projeto') != 'todos':
            df_filtrado = df_filtrado[df_filtrado['PROJETO'] == filtros['projeto']]
        
        if filtros.get('busca'):
            busca = filtros['busca'].lower()
            mask = (df_filtrado['COD_PRODUTO'].astype(str).str.lower().str.contains(busca) |
                   df_filtrado['DESCRICAO'].str.lower().str.contains(busca) |
                   df_filtrado['CLIENTE'].str.lower().str.contains(busca) |
                   df_filtrado['PROJETO'].str.lower().str.contains(busca))
            df_filtrado = df_filtrado[mask]
        
        # Filtro de variação
        if filtros.get('variacao') != 'todos':
            if filtros['variacao'] == 'positiva':
                df_filtrado = df_filtrado[df_filtrado['VARIACAO_PCT'] > 0]
            elif filtros['variacao'] == 'negativa':
                df_filtrado = df_filtrado[df_filtrado['VARIACAO_PCT'] < 0]
        
        return df_filtrado['FATURAMENTO_PERIODO'].sum()
    
    def gerar_relatorio_corrigido(self, output_file):
        """Gera relatório HTML com design melhorado, filtros e gráfico."""
        print(f"\n🎨 GERANDO RELATÓRIO CORRIGIDO E MELHORADO...")
        
        if self.df_resumo is None or self.df_resumo.empty:
            return False
        
        try:
            # Ordena por prioridade e faturamento
            self.df_resumo = self.df_resumo.sort_values(['PRIORIDADE', 'FATURAMENTO_PERIODO'], ascending=[True, False])
            
            # Gera HTML
            html_content = self._criar_html_melhorado()
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"✅ RELATÓRIO GERADO: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao gerar relatório: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _criar_html_melhorado(self):
        """Cria HTML com design melhorado, filtros e gráfico interativo."""
        
        # Prepara dados para tabela
        df_display = self.df_resumo.copy()
        
        # Formata datas e valores para exibição
        df_display['PRIMEIRA_VENDA'] = df_display['PRIMEIRA_VENDA'].dt.strftime('%d/%m/%Y')
        df_display['ULTIMA_VENDA'] = df_display['ULTIMA_VENDA'].dt.strftime('%d/%m/%Y')
        df_display['ULTIMO_REAJUSTE'] = df_display['ULTIMO_REAJUSTE'].apply(lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else 'N/A')
        df_display['PRECO_INICIAL'] = df_display['PRECO_INICIAL'].apply(lambda x: f'R$ {x:,.2f}')
        df_display['PRECO_ATUAL'] = df_display['PRECO_ATUAL'].apply(lambda x: f'R$ {x:,.2f}')
        # Formato mais compacto para caber na célula
        df_display['FATURAMENTO_PERIODO'] = df_display['FATURAMENTO_PERIODO'].apply(lambda x: f'R$ {x:,.0f}')
        df_display['MEDIA_MENSAL'] = df_display['MEDIA_MENSAL'].apply(lambda x: f'R$ {x:,.0f}')
        df_display['VARIACAO_PCT'] = df_display['VARIACAO_PCT'].apply(lambda x: f'{x:+.1f}%')
        
        # CORREÇÃO: Filtra melhor os valores para os dropdowns
        clientes_unicos = sorted([c for c in df_display['CLIENTE'].unique().tolist() 
                                 if c not in ('N/A', '', 'nan', None) and pd.notna(c)])
        projetos_unicos = sorted([p for p in df_display['PROJETO'].unique().tolist() 
                                 if p not in ('N/A', '', 'nan', None) and pd.notna(p)])
        
        # Gera opções HTML
        opcoes_clientes = "".join([f'<option value="{c}">{c}</option>' for c in clientes_unicos])
        opcoes_projetos = "".join([f'<option value="{p}">{p}</option>' for p in projetos_unicos])
        
        # Se não houver opções, mostra mensagem
        if not opcoes_clientes:
            opcoes_clientes = '<option value="sem_dados">Nenhum cliente encontrado</option>'
        if not opcoes_projetos:
            opcoes_projetos = '<option value="sem_dados">Nenhum projeto encontrado</option>'
        
        # --- PREPARAÇÃO DO DATASET DE VENDAS PARA O GRÁFICO (JSON) ---
        codigos_produtos = df_display['COD_PRODUTO'].tolist()
        vendas_dataset = {}
        # Pré-calcula os dados do gráfico para todos os produtos
        for cod in codigos_produtos:
            vendas_dataset[cod] = self._calcular_vendas_anuais(cod)

        # Adiciona dados globais (sem filtro)
        vendas_dataset['GLOBAL'] = self._calcular_vendas_anuais()
        
        vendas_json = json.dumps(vendas_dataset)
        
        # CORREÇÃO: Calcula estatísticas para os cards ANTES da formatação
        total_produtos = len(df_display)
        produtos_atencao = len(df_display[df_display['STATUS'] == 'ATENÇÃO'])
        produtos_ocasionais = len(df_display[df_display['STATUS'] == 'OCASIONAL'])
        produtos_inativos = len(df_display[df_display['STATUS'] == 'INATIVO'])
        produtos_alta_importancia = len(df_display[df_display['IMPORTANCIA'].isin(['MUITO ALTA', 'ALTA'])])
        faturamento_total = self.df_resumo['FATURAMENTO_PERIODO'].sum()
        
        # Gera linhas da tabela
        linhas_tabela = ""
        for index, row in df_display.iterrows():
            status_class = ""
            if row['STATUS'] == 'ATENÇÃO':
                status_class = "status-atencao"
            elif row['STATUS'] == 'INATIVO':
                status_class = "status-inativo"
            elif row['STATUS'] == 'OCASIONAL':
                status_class = "status-ocasional"
            
            importancia_class = f"importancia-{row['IMPORTANCIA'].replace(' ', '-').lower()}"
            
            variacao_str = str(row['VARIACAO_PCT']).replace('%', '').replace('+', '').replace('R$', '').replace(',', '.')
            try:
                variacao = float(variacao_str)
            except ValueError:
                variacao = 0 

            variacao_class = "variacao-positiva" if variacao > 0 else "variacao-negativa" if variacao < 0 else ""
            
            alerta_class = "alerta-reajuste" if row['ALERTA_REAJUSTE'] else ""
            
            # Adiciona o evento onclick para carregar o gráfico
            linhas_tabela += f"""
            <tr class="{status_class} {alerta_class}" data-cod="{row['COD_PRODUTO']}" onclick="carregarGraficoProduto(this)">
                <td><strong>{row['COD_PRODUTO']}</strong></td>
                <td>{str(row['DESCRICAO'])[:50]}</td>
                <td>{str(row['CLIENTE'])[:25]}</td> <td>{str(row['PROJETO'])[:20]}</td> <td class="{importancia_class}">{row['IMPORTANCIA']}</td>
                <td>{row['PRIMEIRA_VENDA']}</td>
                <td>{row['PRECO_INICIAL']}</td>
                <td>{row['ULTIMA_VENDA']}</td>
                <td>{row['PRECO_ATUAL']}</td>
                <td class="{variacao_class}">{row['VARIACAO_PCT']}</td>
                <td>{row['ULTIMO_REAJUSTE']}</td>
                <td>{row['DIAS_SEM_REAJUSTE']}</td>
                <td class="text-compact text-end">{row['FATURAMENTO_PERIODO']}</td>
                <td class="text-compact text-end">{row['MEDIA_MENSAL']}</td>
                <td class="text-center">{row['MESES_COM_VENDAS']}</td>
                <td class="text-center">{row['CLIENTES_ATENDIDOS']}</td>
                <td class="status {status_class}">{row['STATUS']}</td>
            </tr>
            """
        
        # --- CÓDIGO HTML FINAL ---
        return f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MONITOR DE PREÇOS INTEP - Versão V10 (Filtros Dinâmicos)</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.7.1/dist/chart.min.js"></script>
    <style>
        body {{ 
            background-color: #f8f9fa; 
            font-family: 'Times New Roman', Times, serif !important;
            font-size: 0.85rem;
        }}
        .header {{ 
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white;
            padding: 1.5rem 0;
        }}
        .card {{ 
            border: none;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 1rem;
        }}
        /* AJUSTE CABEÇALHOS (RESUMOS) */
        .stats-card {{ 
            text-align: center;
            padding: 1rem 0.5rem; 
            height: 100%;
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        .stats-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
        }}
        .stats-number {{ 
            font-size: 1.3rem; 
            font-weight: bold;
        }}
        .stats-label {{ 
            font-size: 0.65rem;
            opacity: 0.9;
            line-height: 1.1;
        }}
        /* Status colors */
        .status-atencao {{ background-color: #fff3cd !important; }}
        .status-inativo {{ background-color: #f8d7da !important; }}
        .status-ocasional {{ background-color: #d1ecf1 !important; }}
        
        /* Importância colors */
        .importancia-muito-alta {{ background-color: #dc3545 !important; color: white; font-weight: bold; }}
        .importancia-alta {{ background-color: #fd7e14 !important; color: white; font-weight: bold; }}
        .importancia-média {{ background-color: #ffc107 !important; color: black; font-weight: bold; }}
        .importancia-baixa {{ background-color: #6c757d !important; color: white; }}
        
        /* Variação colors */
        .variacao-positiva {{ color: #198754; font-weight: bold; }} 
        .variacao-negativa {{ color: #dc3545; font-weight: bold; }}
        
        /* Alerta reajuste */
        .alerta-reajuste {{ border-left: 4px solid #dc3545 !important; }}
        
        .filtros-avancados {{
            background: white;
            padding: 1rem;
            border-radius: 10px;
            margin-bottom: 1rem;
        }}
        
        .table-responsive {{ 
            border-radius: 8px;
            overflow: hidden;
            font-size: 0.8rem;
            max-height
            max-height: 400px; /* Limita altura da tabela */
        }}
        
        .table thead th {{
            background-color: #2c3e50;
            color: white;
            border: none;
            position: sticky;
            top: 0;
            font-size: 0.75rem;
            padding: 0.5rem;
        }}
        
        .table tbody td {{
            padding: 0.4rem;
            vertical-align: middle;
            white-space: nowrap;
        }}
        
        .text-compact {{
            font-size: 0.75rem;
        }}
        
        .table tbody tr:hover {{ cursor: pointer; background-color: rgba(52, 152, 219, 0.1); }}
        .table tbody tr.selected {{ background-color: #d6eaf8 !important; font-weight: bold; }}

        .table-container {{
            max-height: 400px;
            overflow-y: auto;
        }}
        
        /* CORREÇÃO DO GRÁFICO - CONTAINER COM ALTURA FIXA */
        .grafico-container {{
            height: 300px;
            position: relative;
        }}
        
        #vendasChart {{
            width: 100% !important;
            height: 100% !important;
        }}
        
        /* Ajustes de layout */
        .analise-detalhada {{
            margin-bottom: 1rem;
        }}
        
        .grafico-section {{
            margin-top: 1rem;
        }}
        
        .btn-grafico {{
            font-size: 0.75rem;
            padding: 0.25rem 0.5rem;
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="container-fluid">
            <div class="row align-items-center">
                <div class="col-md-8">
                    <h1 style="font-size: 1.8rem; margin-bottom: 0.5rem; font-family: 'Times New Roman', Times, serif;">
                        <i class="fas fa-chart-line me-2"></i>MONITOR DE PREÇOS INTEP - V10
                    </h1>
                    <p class="mb-0" style="font-family: 'Times New Roman', Times, serif;">Análise detalhada de preços e faturamento</p>
                </div>
                <div class="col-md-4 text-end">
                    <button class="btn btn-outline-light btn-sm" onclick="exportarDados()">
                        <i class="fas fa-download me-1"></i>Exportar Dados da Tabela
                    </button>
                </div>
            </div>
        </div>
    </div>

    <div class="container-fluid mt-3">
        <!-- Cards Estatísticos Interativos -->
        <div class="row mb-3" id="cards-estatisticos">
            <div class="col-md-2 col-6">
                <div class="card bg-primary text-white stats-card" onclick="filtrarPorStatus('todos')">
                    <div class="card-body stats-card">
                        <div class="stats-number">{total_produtos}</div>
                        <div class="stats-label">TOTAL PRODUTOS</div>
                    </div>
                </div>
            </div>
            <div class="col-md-2 col-6">
                <div class="card bg-success text-white stats-card" onclick="filtrarPorStatus('ATENÇÃO')">
                    <div class="card-body stats-card">
                        <div class="stats-number" id="card-atencao">{produtos_atencao}</div>
                        <div class="stats-label">PRECISAM REAJUSTE</div>
                    </div>
                </div>
            </div>
            <div class="col-md-2 col-6">
                <div class="card bg-warning text-dark stats-card" onclick="filtrarPorStatus('OCASIONAL')">
                    <div class="card-body stats-card">
                        <div class="stats-number" id="card-ocasionais">{produtos_ocasionais}</div>
                        <div class="stats-label">OCASIONAIS</div>
                    </div>
                </div>
            </div>
            <div class="col-md-2 col-6">
                <div class="card bg-danger text-white stats-card" onclick="filtrarPorStatus('INATIVO')">
                    <div class="card-body stats-card">
                        <div class="stats-number" id="card-inativos">{produtos_inativos}</div>
                        <div class="stats-label">INATIVOS</div>
                    </div>
                </div>
            </div>
            <div class="col-md-2 col-6">
                <div class="card bg-info text-white stats-card" onclick="filtrarPorImportancia('alta')">
                    <div class="card-body stats-card">
                        <div class="stats-number" id="card-alta-importancia">{produtos_alta_importancia}</div>
                        <div class="stats-label">ALTA IMPORTÂNCIA</div>
                    </div>
                </div>
            </div>
            <div class="col-md-2 col-6">
                <div class="card bg-dark text-white stats-card" onclick="filtrarPorFaturamento()">
                    <div class="card-body stats-card">
                        <div class="stats-number" id="card-faturamento">R$ {faturamento_total:,.0f}</div>
                        <div class="stats-label">FATURAMENTO TOTAL</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Filtros -->
        <div class="filtros-avancados">
            <div class="row g-2">
                <div class="col-md-2">
                    <select id="filtro-status" class="form-select form-select-sm" onchange="filtrarTabela()">
                        <option value="todos">Status</option>
                        <option value="ATENÇÃO">Precisam Reajuste</option>
                        <option value="NORMAL">Normais</option>
                        <option value="OCASIONAL">Ocasionais</option>
                        <option value="INATIVO">Inativos</option>
                    </select>
                </div>
                <div class="col-md-2">
                    <select id="filtro-importancia" class="form-select form-select-sm" onchange="filtrarTabela()">
                        <option value="todos">Importância</option>
                        <option value="MUITO ALTA">Muito Alta</option>
                        <option value="ALTA">Alta</option>
                        <option value="MÉDIA">Média</option>
                        <option value="BAIXA">Baixa</option>
                    </select>
                </div>
                <div class="col-md-2">
                    <select id="filtro-variacao" class="form-select form-select-sm" onchange="filtrarTabela()">
                        <option value="todos">Variação</option>
                        <option value="positiva">Positiva (+)</option>
                        <option value="negativa">Negativa (-)</option>
                    </select>
                </div>
                <div class="col-md-2">
                    <select id="filtro-cliente" class="form-select form-select-sm" onchange="filtrarTabela()">
                        <option value="todos">Cliente (Caract.)</option>
                        {opcoes_clientes}
                    </select>
                </div>
                <div class="col-md-2">
                    <select id="filtro-projeto" class="form-select form-select-sm" onchange="filtrarTabela()">
                        <option value="todos">Projeto (Caract.)</option>
                        {opcoes_projetos}
                    </select>
                </div>
                <div class="col-md-2">
                    <input type="text" id="busca-geral" class="form-control form-control-sm" placeholder="Buscar Cód/Desc..." onkeyup="filtrarTabela()">
                </div>
            </div>
        </div>
        
        <!-- ANÁLISE DETALHADA DE PREÇOS (ACIMA DO GRÁFICO) -->
        <div class="row analise-detalhada">
            <div class="col-12">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center py-2">
                        <h5 class="card-title mb-0" style="font-size: 1rem; font-family: 'Times New Roman', Times, serif;">
                            <i class="fas fa-table me-2"></i>Análise Detalhada de Preços
                        </h5>
                        <span class="badge bg-primary" id="contador-registros">{total_produtos} produtos</span>
                    </div>
                    <div class="card-body p-0">
                        <div class="table-container">
                            <table id="tabela-precos" class="table table-hover table-sm mb-0">
                                <thead>
                                    <tr>
                                        <th>Código</th>
                                        <th>Descrição</th>
                                        <th>Cliente</th> <th>Projeto</th> <th>Importância</th>
                                        <th>Primeiro Reg PV</th>
                                        <th>PV Inic. NET</th>
                                        <th>Primeiro Reg PV</th>
                                        <th>PV Atual NET</th>
                                        <th>Variação</th>
                                        <th>Últ. Var. PV</th>
                                        <th>Dias Ult. Var. PV</th>
                                        <th>Faturamento</th>
                                        <th>Média/Mês</th>
                                        <th>Meses</th>
                                        <th>Clientes</th>
                                        <th>Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {linhas_tabela}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- GRÁFICO (ABAIXO DA ANÁLISE DETALHADA) -->
        <div class="row grafico-section">
            <div class="col-12">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center py-2">
                        <h6 class="card-title mb-0" style="font-size: 0.9rem; font-family: 'Times New Roman', Times, serif;">
                            <i class="fas fa-chart-area me-1"></i>
                            <span id="grafico-titulo">Faturamento Anual - Visão Global</span>
                        </h6>
                        <div>
                            <button class="btn btn-outline-primary btn-grafico me-1" onclick="carregarGraficoGlobal()">
                                <i class="fas fa-globe me-1"></i>Global
                            </button>
                            <button class="btn btn-outline-success btn-grafico" onclick="carregarGraficoFiltrado()">
                                <i class="fas fa-filter me-1"></i>Com Filtros
                            </button>
                        </div>
                    </div>
                    <div class="card-body d-flex flex-column">
                        <div class="grafico-container">
                            <canvas id="vendasChart"></canvas>
                        </div>
                        <p id="msg-grafico" class="text-center text-muted mt-3" style="font-size: 0.8rem; font-family: 'Times New Roman', Times, serif;">
                            Use os botões acima para visualizar o faturamento global ou aplicar os filtros atuais.
                        </p>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // --- 1. CONFIGURAÇÃO GLOBAL DE DADOS ---
        const VENDAS_DATASET = {vendas_json};
        let chartInstance = null;
        let modoGraficoAtual = 'global'; // 'global', 'filtrado', 'produto'
        
        // --- 2. FUNÇÕES DE FILTRO ---
        function filtrarTabela() {{
            const filtroStatus = document.getElementById('filtro-status').value;
            const filtroImportancia = document.getElementById('filtro-importancia').value;
            const filtroVariacao = document.getElementById('filtro-variacao').value;
            const filtroCliente = document.getElementById('filtro-cliente').value;
            const filtroProjeto = document.getElementById('filtro-projeto').value;
            const buscaGeral = document.getElementById('busca-geral').value.toLowerCase();
            
            const linhas = document.querySelectorAll('#tabela-precos tbody tr');
            let visiveis = 0;
            let faturamentoFiltrado = 0;
            
            linhas.forEach(linha => {{
                let mostrar = true;
                
                // Dados da linha (Índices da coluna)
                const codigo = linha.cells[0].textContent.toLowerCase();
                const descricao = linha.cells[1].textContent.toLowerCase();
                const clienteCarac = linha.cells[2].textContent.trim();
                const projetoCarac = linha.cells[3].textContent.trim(); 
                const importancia = linha.cells[4].textContent.trim();
                const variacaoText = linha.cells[9].textContent.trim();
                const variacao = parseFloat(variacaoText.replace('%', '').replace('+', '').replace(',', '.'));
                const status = linha.cells[16].textContent.trim();
                const faturamentoText = linha.cells[12].textContent.replace('R$ ', '').replace(/\./g, '').replace(',', '.');
                const faturamento = parseFloat(faturamentoText) || 0;
                
                // Aplicar filtros
                if (filtroStatus !== 'todos' && status !== filtroStatus) {{ mostrar = false; }}
                if (filtroImportancia !== 'todos' && importancia !== filtroImportancia) {{ mostrar = false; }}
                
                // Filtros Cliente e Projeto
                if (filtroCliente !== 'todos' && clienteCarac !== filtroCliente) {{ mostrar = false; }} 
                if (filtroProjeto !== 'todos' && projetoCarac !== filtroProjeto) {{ mostrar = false; }}
                
                // Filtro de variação
                if (filtroVariacao === 'positiva' && (isNaN(variacao) || variacao <= 0)) {{ mostrar = false; }}
                if (filtroVariacao === 'negativa' && (isNaN(variacao) || variacao >= 0)) {{ mostrar = false; }}
                
                // Busca geral
                if (buscaGeral && !codigo.includes(buscaGeral) && !descricao.includes(buscaGeral) && 
                    !clienteCarac.toLowerCase().includes(buscaGeral) && !projetoCarac.toLowerCase().includes(buscaGeral)) {{
                    mostrar = false;
                }}
                
                linha.style.display = mostrar ? '' : 'none';
                if (mostrar) {{ 
                    visiveis++; 
                    faturamentoFiltrado += faturamento;
                }}
            }});
            
            document.getElementById('contador-registros').textContent = visiveis + ' produtos';
            document.getElementById('card-faturamento').textContent = 'R$ ' + faturamentoFiltrado.toLocaleString('pt-BR', {{maximumFractionDigits: 0}});
            
            // Atualiza contadores dos cards
            atualizarContadoresCards();
            
            // Se estiver no modo filtrado, atualiza o gráfico
            if (modoGraficoAtual === 'filtrado') {{
                carregarGraficoFiltrado();
            }}
        }}

        // --- 3. FUNÇÕES DOS CARDS INTERATIVOS ---
        function filtrarPorStatus(status) {{
            document.getElementById('filtro-status').value = status;
            filtrarTabela();
        }}

        function filtrarPorImportancia(importancia) {{
            let valorFiltro = 'todos';
            if (importancia === 'alta') {{
                document.getElementById('filtro-importancia').value = 'MUITO ALTA';
                filtrarTabela();
                setTimeout(() => {{
                    document.getElementById('filtro-importancia').value = 'ALTA';
                    filtrarTabela();
                }}, 100);
                return;
            }}
            document.getElementById('filtro-importancia').value = valorFiltro;
            filtrarTabela();
        }}

        function filtrarPorFaturamento() {{
            // Limpa todos os filtros para mostrar todos os produtos
            document.getElementById('filtro-status').value = 'todos';
            document.getElementById('filtro-importancia').value = 'todos';
            document.getElementById('filtro-variacao').value = 'todos';
            document.getElementById('filtro-cliente').value = 'todos';
            document.getElementById('filtro-projeto').value = 'todos';
            document.getElementById('busca-geral').value = '';
            filtrarTabela();
        }}

        function atualizarContadoresCards() {{
            const linhas = document.querySelectorAll('#tabela-precos tbody tr');
            let atencao = 0, ocasionais = 0, inativos = 0, altaImportancia = 0;
            
            linhas.forEach(linha => {{
                if (linha.style.display !== 'none') {{
                    const status = linha.cells[16].textContent.trim();
                    const importancia = linha.cells[4].textContent.trim();
                    
                    if (status === 'ATENÇÃO') atencao++;
                    if (status === 'OCASIONAL') ocasionais++;
                    if (status === 'INATIVO') inativos++;
                    if (importancia === 'MUITO ALTA' || importancia === 'ALTA') altaImportancia++;
                }}
            }});
            
            document.getElementById('card-atencao').textContent = atencao;
            document.getElementById('card-ocasionais').textContent = ocasionais;
            document.getElementById('card-inativos').textContent = inativos;
            document.getElementById('card-alta-importancia').textContent = altaImportancia;
        }}

        // --- 4. FUNÇÕES DO GRÁFICO (MELHORADAS) ---
        function carregarGraficoProduto(linha) {{
            // Remove seleção anterior
            document.querySelectorAll('#tabela-precos tbody tr.selected').forEach(r => r.classList.remove('selected'));
            // Adiciona seleção na linha atual
            linha.classList.add('selected');
            
            const codProduto = linha.getAttribute('data-cod');
            const nomeProduto = linha.cells[1].textContent.trim();
            const dados = VENDAS_DATASET[codProduto];
            
            document.getElementById('grafico-titulo').textContent = 'Faturamento Anual - ' + codProduto + ' - ' + nomeProduto.substring(0, 20) + '...';
            document.getElementById('msg-grafico').style.display = 'none';
            modoGraficoAtual = 'produto';

            criarGrafico(dados, 'Produto: ' + codProduto);
        }}

        function carregarGraficoGlobal() {{
            const dados = VENDAS_DATASET['GLOBAL'];
            document.getElementById('grafico-titulo').textContent = 'Faturamento Anual - Visão Global';
            document.getElementById('msg-grafico').style.display = 'none';
            modoGraficoAtual = 'global';
            
            criarGrafico(dados, 'Faturamento Global');
        }}

        function carregarGraficoFiltrado() {{
            // Coleta os filtros atuais
            const filtroStatus = document.getElementById('filtro-status').value;
            const filtroImportancia = document.getElementById('filtro-importancia').value;
            const filtroVariacao = document.getElementById('filtro-variacao').value;
            const filtroCliente = document.getElementById('filtro-cliente').value;
            const filtroProjeto = document.getElementById('filtro-projeto').value;
            const buscaGeral = document.getElementById('busca-geral').value;
            
            // Encontra produtos que passam pelos filtros
            const produtosFiltrados = [];
            const linhas = document.querySelectorAll('#tabela-precos tbody tr');
            
            linhas.forEach(linha => {{
                if (linha.style.display !== 'none') {{
                    const codProduto = linha.getAttribute('data-cod');
                    produtosFiltrados.push(codProduto);
                }}
            }});
            
            // Combina dados de todos os produtos filtrados
            const dadosCombinados = combinarDadosProdutos(produtosFiltrados);
            
            let tituloFiltros = 'Faturamento com Filtros';
            if (filtroStatus !== 'todos') tituloFiltros += ' - Status: ' + filtroStatus;
            if (filtroImportancia !== 'todos') tituloFiltros += ' - Importância: ' + filtroImportancia;
            if (filtroCliente !== 'todos') tituloFiltros += ' - Cliente: ' + filtroCliente;
            if (filtroProjeto !== 'todos') tituloFiltros += ' - Projeto: ' + filtroProjeto;
            if (buscaGeral) tituloFiltros += ' - Busca: ' + buscaGeral;
            
            document.getElementById('grafico-titulo').textContent = tituloFiltros;
            document.getElementById('msg-grafico').style.display = 'none';
            modoGraficoAtual = 'filtrado';
            
            criarGrafico(dadosCombinados, 'Faturamento Filtrado');
        }}

        function combinarDadosProdutos(produtos) {{
            const todosAnos = new Set();
            const vendasPorAno = {{}};
            
            // Coleta todos os anos disponíveis
            produtos.forEach(codProduto => {{
                const dados = VENDAS_DATASET[codProduto];
                if (dados && dados.anos) {{
                    dados.anos.forEach(ano => todosAnos.add(ano));
                }}
            }});
            
            // Inicializa vendas por ano
            Array.from(todosAnos).sort().forEach(ano => {{
                vendasPorAno[ano] = 0;
            }});
            
            // Soma as vendas de todos os produtos
            produtos.forEach(codProduto => {{
                const dados = VENDAS_DATASET[codProduto];
                if (dados && dados.anos && dados.vendas) {{
                    dados.anos.forEach((ano, index) => {{
                        if (vendasPorAno.hasOwnProperty(ano)) {{
                            vendasPorAno[ano] += dados.vendas[index];
                        }} else {{
                            vendasPorAno[ano] = dados.vendas[index];
                        }}
                    }});
                }}
            }});
            
            // Converte para o formato esperado
            const anosOrdenados = Object.keys(vendasPorAno).sort();
            const vendasOrdenadas = anosOrdenados.map(ano => vendasPorAno[ano]);
            
            return {{
                anos: anosOrdenados,
                vendas: vendasOrdenadas
            }};
        }}

        function criarGrafico(dados, label) {{
            // Destrói gráfico anterior se existir
            if (chartInstance) {{
                chartInstance.destroy();
                chartInstance = null;
            }}

            const canvas = document.getElementById('vendasChart');
            const ctx = canvas.getContext('2d');
            
            if (dados && dados.anos.length > 0) {{
                chartInstance = new Chart(ctx, {{
                    type: 'bar',
                    data: {{
                        labels: dados.anos,
                        datasets: [{{
                            label: label,
                            data: dados.vendas,
                            backgroundColor: 'rgba(52, 152, 219, 0.8)',
                            borderColor: 'rgba(52, 152, 219, 1)',
                            borderWidth: 1
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{ 
                                display: true,
                                position: 'top'
                            }},
                            tooltip: {{ 
                                callbacks: {{ 
                                    label: function(context) {{
                                        return context.dataset.label + ': R$ ' + context.parsed.y.toLocaleString('pt-BR');
                                    }}
                                }} 
                            }}
                        }},
                        scales: {{
                            y: {{ 
                                beginAtZero: true, 
                                ticks: {{ 
                                    callback: function(value) {{ 
                                        return 'R$ ' + (value / 1000).toFixed(0) + 'k'; 
                                    }} 
                                }} 
                            }},
                            x: {{ 
                                grid: {{ display: false }} 
                            }}
                        }}
                    }}
                }});
                canvas.style.display = 'block';
            }} else {{
                document.getElementById('msg-grafico').textContent = 'Sem dados históricos de vendas para os filtros selecionados.';
                document.getElementById('msg-grafico').style.display = 'block';
                canvas.style.display = 'none';
            }}
        }}

        // --- 5. FUNÇÃO EXPORTAR ---
        function exportarDados() {{
            const tabela = document.getElementById('tabela-precos');
            let csv = [];
            
            // Cabeçalhos
            let headers = [];
            for (let i = 0; i < tabela.rows[0].cells.length; i++) {{
                headers.push(tabela.rows[0].cells[i].textContent.trim());
            }}
            csv.push(headers.join(';'));
            
            // Dados (apenas linhas visíveis)
            for (let i = 1; i < tabela.rows.length; i++) {{
                if (tabela.rows[i].style.display !== 'none') {{
                    let row = [];
                    for (let j = 0; j < tabela.rows[i].cells.length; j++) {{
                        row.push(tabela.rows[i].cells[j].textContent.replace(/\\s\\s+/g, ' ').trim());
                    }}
                    csv.push(row.join(';'));
                }}
            }}
            
            // Corrige problema de encoding e gera o link
            const csvContent = "data:text/csv;charset=utf-8,\\ufeff" + csv.join('\\n');
            const link = document.createElement("a");
            link.setAttribute("href", csvContent);
            link.setAttribute("download", "analise_precos_" + new Date().toISOString().split('T')[0] + ".csv");
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link); 
        }}
        
        // Inicialização
        document.addEventListener('DOMContentLoaded', function() {{
            filtrarTabela();
            carregarGraficoGlobal(); // Carrega gráfico global por padrão
        }});
    </script>
</body>
</html>
"""

def main():
    # Configuração direta
    PASTA = "V:\\G-VENDAS\\00_BI_VENDAS\\BASES\\VENDAS"
    PREFIXO = "Faturamento por Produto_Período por Itens_"
    CARACTERISTICAS = "V:\\G-VENDAS\\00_BI_VENDAS\\BASES\\DADOS TECNICOS\\CARACTERISCAS PRODUTO EGA.xlsx"
    SAIDA = "V:\\G-VENDAS\\00_BI_VENDAS\\BASES\\VENDAS\\dashboard_precos_V10_filtros_dinamicos.html"
    
    print("🚀 INICIANDO MONITOR DE PREÇOS INTEP - VERSÃO V10 (FILTROS DINÂMICOS)")
    print("=" * 60)
    
    monitor = MonitorPrecosCorrigido(PASTA, PREFIXO, CARACTERISTICAS)
    
    try:
        # Configuração
        monitor.carregar_configuracao()
        
        # Carrega dados
        if monitor.carregar_dados() is None:
            input("\n❌ Pressione Enter para sair...")
            return
            
        # Processa
        monitor.processar_dados()
        monitor.calcular_metricas_corrigidas()
        
        # Gera relatório
        if monitor.gerar_relatorio_corrigido(SAIDA):
            print(f"\n🎉 RELATÓRIO CRIADO COM SUCESSO!")
            print(f"📊 Abra o arquivo: {SAIDA}")
            print("🔍 NOVAS FUNCIONALIDADES:")
            print("   • Gráfico responde a TODOS os filtros")
            print("   • Card de faturamento atualiza com filtros")
            print("   • Botões para visualização Global e Com Filtros")
            print("   • Faturamento dinâmico baseado nos filtros aplicados")
            print("   • Combinação inteligente de dados de múltiplos produtos")
        else:
            print("\n❌ Erro ao criar relatório")
            
    except Exception as e:
        print(f"\n💥 ERRO: {e}")
        import traceback
        traceback.print_exc()
    
    input("\n👋 Pressione Enter para finalizar...")

if __name__ == "__main__":
    main()