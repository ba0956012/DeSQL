# DeSQL

> Rethinking Text-to-SQL for Real-World Data Agents

---

## 🚀 Introduction

Do you really need Text-to-SQL?

In most Text-to-SQL systems, the goal is to generate a single SQL query that fully answers a user's question.

However, in real-world scenarios, user queries often involve complex reasoning:

- ranking (top-N)
- comparison
- ratio / percentage
- what-if analysis
- cross-table reasoning

As SQL complexity increases — with nested queries, window functions, and multi-level joins — the accuracy of LLM-generated SQL drops significantly.

> ❗The real problem is not SQL generation — it's answer correctness.

DeSQL addresses this by decoupling:

- SQL → data retrieval  
- Python → logical reasoning  

This leads to a more robust and practical solution for real-world data agents.

---

## 💡 Key Insight

> **Optimize Answer Accuracy, not SQL Accuracy**

Traditional Text-to-SQL focuses on:

- SQL syntax correctness
- SQL semantic correctness (Exact Match / Execution Match)

But in real-world applications:

> Users care about whether the **final answer is correct**, not whether the SQL is perfect.

---

## 🧠 Core Idea

Instead of forcing LLM to generate complex SQL in one step:

- ✅ SQL is used to retrieve raw data (simple, reliable)
- ✅ Python is used to perform reasoning (flexible, expressive)

Natural Language → SQL → Raw Data → Python → Final Answer

---

## ⚖️ Comparison with Traditional Text-to-SQL

| Approach | Pros | Cons |
|----------|------|------|
| Text-to-SQL (single-step) | Simple pipeline | SQL becomes complex → low accuracy |
| DeSQL (SQL + Python) | Higher flexibility, higher success rate | Slightly longer pipeline |

---

## ⚙️ Key Features

### 🧩 Progressive Retrieval Subgraph
- PHRASE → AND → SYNONYM → OR
- Independent and reusable module

---

### 🔁 SQL Self-Healing
- Detect execution errors
- Feed error messages back to LLM
- Auto-retry (max 2 times)

---

### 🧠 Python Reasoning Layer (**Core Innovation**)
Move complex logic out of SQL:

- ranking
- aggregation
- comparison
- what-if analysis

👉 More flexible and easier to debug than SQL

---

### 🔒 Safe Python Sandbox
- Restricted execution environment
- Built-in support:
  - datetime
  - Decimal
  - Counter
- Prevent unsafe operations

---

### 📊 Insight-aware Visualization
- LLM determines whether visualization adds value
- Avoids unnecessary charts

---

### 📈 Token Tracking
- Per-node token usage
- Helps optimize cost

---

## 🔁 Pipeline Behavior

DeSQL is not a simple linear pipeline.

It is a **recoverable system**:

- SQL node → retry on failure  
- Python node → retry on execution error  
- retrieval → progressive relaxation  

👉 Similar to self-healing agent pipelines

---

## 🎯 Use Cases

Suitable for:

- Business analytics
- POS / retail data analysis
- BI systems
- Natural language query interfaces

Especially when queries involve:

- multi-step reasoning
- aggregation + filtering
- comparison across groups

---

## ❌ Not Suitable For

- Simple CRUD queries
- Direct lookup tasks

---

## 🧪 Evaluation (Future Work)

We focus on:

- **Answer Accuracy** (primary metric)
- SQL Success Rate
- Python Execution Success Rate
- Token Cost per Query

Future directions:

- Compare with Text-to-SQL baseline
- Statistical evaluation (e.g., bootstrap confidence intervals)
- Prompt / agent optimization

---

## ⚡ Quick Start

```bash
pip install -r requirements.txt
./run.sh
```

Open: http://localhost:8501

## Configuration

Copy `.env.example` to `.env` and fill in your credentials：

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `AZURE_OPENAI_API_KEY` | Azure OpenAI API key |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI endpoint URL |
| `DATABASE_URL` | PostgreSQL connection string |
| `LLM_MODEL` | Model name (default: `gpt-4.1-mini`) |
| `LOG_MAX_FILES` | Max log files to keep (default: `50`) |

> ⚠️ **Database Support**: Currently only **PostgreSQL** is supported.
> The SQL generation prompts, `information_schema` queries (enum loading, column discovery), and the `psycopg2` driver are all PostgreSQL-specific.
> Supporting other databases (MySQL, SQLite, etc.) would require adapting `db.py`, `retrieval_subgraph.py`, and the prompt dialect in `nodes/sql.py`.

## Domain Customization

Edit `domain_rules.py` to add or remove business-specific SQL generation rules.
Clear the list for a fully generic pipeline:

```python
DOMAIN_SQL_RULES = []
```

## Project Structure

```
desql/
├── .env.example              # Environment variables template
├── config.py                 # Settings (reads from .env)
├── db.py                     # DB connection, schema, enum loading
├── llm.py                    # LLM initialization
├── logger.py                 # Per-run logging with auto-cleanup
├── utils.py                  # Shared utilities
├── domain_rules.py           # Domain-specific SQL rules
├── pipeline.py               # State, routing, graph assembly
├── retrieval_subgraph.py     # Progressive keyword retrieval
├── nodes/
│   ├── sql.py                # SQL generation & execution
│   ├── code.py               # Python sandbox & code generation
│   ├── answer.py             # Answer formatting
│   └── chart.py              # Chart generation
├── app.py                    # Streamlit UI
├── tests/
│   └── test_pipeline.py
├── run.sh
└── requirements.txt
```
