# Morada: Engenharia de Dados em Tempo Real (Spark & Kafka)
Elder / Iandra / Assis / Adrinaldo

Este repositório contém a especificação de contratos de dados e a implementação de um pipeline de ETL em tempo real utilizando **Apache Kafka** e **PySpark Structured Streaming**, desenvolvido como projeto prático para a pós-graduação em **Engenharia de Dados (Unifametro)**.

O objetivo do projeto é demonstrar a ingestão, higienização, conformidade regulatória (LGPD) e exibição analítica de eventos de reservas de hospedagem para a plataforma **Morada**, um concorrente estratégico focado nas particularidades do mercado brasileiro (como Pix, parcelamento, divisão de contas e taxas de day-use).

---

## 🛠️ Arquitetura do Pipeline de Dados

O fluxo de dados segue a arquitetura de medalhão (Bronze, Silver e Gold):

```
┌─────────────────┐       ┌────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  Simulador App  │ ────> │  Apache Kafka  │ ────> │ PySpark Stream  │ ────> │ Dashboard Real  │
│ (Mock Producer) │       │ (Topic: Bookings)│     │ (Transformations)│      │  Time (Notebook)│
└─────────────────┘       └────────────────┘       └─────────────────┘       └─────────────────┘
                                                            │
                                        ┌───────────────────┴───────────────────┐
                                        ▼ (Camada Silver)                       ▼ (Camada Gold)
                                   - Limpeza de Telefone                   - Agregação Financeira
                                   - Máscara CPF (LGPD)                    - Cashback acumulado
                                   - Regra de Cashback                     - Ticket médio por estado
```

1.  **Geração / Produtor**: Um produtor fictício em Python simula em tempo real o comportamento de usuários realizando reservas na plataforma e publica no Kafka.
2.  **Mensageria (Kafka)**: O Broker Kafka recebe e enfileira as mensagens no tópico `morada-bookings`.
3.  **Processamento (PySpark)**: O Spark Structured Streaming lê continuamente os dados do Kafka, aplica os contratos de validação de dados e realiza as transformações de negócio.
4.  **Consumo Analítico / Dashboard**: O fluxo estruturado do Spark é persistido temporariamente em memória (`memory sink`), onde uma interface gráfica desenvolvida em `matplotlib` e `seaborn` consulta os dados e renderiza gráficos dinâmicos atualizados de 3 em 3 segundos.

---

## 📁 Estrutura de Arquivos Recomendada para o Git

```
├── .gitignore                      # Arquivos e pastas ignorados no Git (logs, binários do Kafka)
├── README.md                       # Documentação principal do projeto
├── morada_auto_stream.ipynb        # Notebook Jupyter com o pipeline completo + Dashboard Gráfico
├── morada_auto_stream.py           # Script Python equivalente para rodar direto no terminal
└── airbnb_clone_brazil_spec.txt    # Contratos de especificação inicial do banco de dados e APIs
```

---

## 🔒 Regras de Negócio e LGPD Aplicadas

Durante a transição da camada **Bronze** (dados brutos de streaming) para a **Silver** (dados higienizados e confiáveis), o pipeline aplica:
*   **Mascaramento de CPF (LGPD)**: Proteção de dados pessoais sensíveis transformando `123.456.789-01` em `***.456.789-**`.
*   **Limpeza de Contatos**: Higienização de strings de telefone eliminando parênteses, hifens e espaços em branco.
*   **Cálculo Dinâmico de Cashback**: Regra de fidelidade regional que concede `2%` de cashback do valor total da reserva se pago via **Pix**, e `1%` para demais métodos (cartão de crédito, cartão de débito, boleto ou split).

---

## 🚀 Como Executar

### Opção 1: Diretamente no Google Colab (Recomendado)
1. Faça o upload do arquivo `morada_auto_stream.ipynb` no seu ambiente do [Google Colab](https://colab.research.google.com).
2. Execute as células de forma sequencial. O notebook cuidará de baixar a JVM do Java, baixar os binários do Apache Kafka, iniciar o broker em background e disparar o streaming.
3. Para finalizar a execução do dashboard gráfico de tempo real, clique em **Stop** na célula do loop infinito.

### Opção 2: Localmente no Linux (Via terminal)
Como o projeto é autocontido e você já possui o Java 17 instalado, execute:

```bash
# 1. Clone o repositório
git clone <seu-repositorio-github>
cd <pasta-do-projeto>

# 2. Execute o script automatizado
python3 morada_auto_stream.py
```
*(O script se encarregará de baixar as dependências Python, baixar o Kafka localmente, subir os brokers em background e iniciar o fluxo de stream direto no seu console).*

---

## 🎓 Autoria
*   **Instituição**: Centro Universitário Fametro (Unifametro)
*   **Curso**: Especialização em Engenharia de Dados
*   **Disciplina**: Engenharia de Dados em Tempo Real / Processamento de Fluxos
