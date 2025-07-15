# Data Pipeline To Genrate Embeddings & RAG-based Document Q&A System using Databricks

### Project Overview

This project implements a **scalable, production-grade data engineering pipeline** using **Databricks** that enables **semantic search** and **LLM-powered Q&A** over a collection of documents (PDFs, text files, etc.).

It uses:
- the **Medallion architecture (Bronze → Silver → Gold)**,
- **Delta Lake** for data reliability,
- **Databricks Vector Search** for similarity retrieval,
- and **Databricks Foundation Model Serving** to power **Retrieval-Augmented Generation (RAG)**.

🧪 Full observability and traceability are implemented using **Model Context Protocol (MCP)**.

---

### 🛠️ Tech Stack

| Component              | Tools Used                                                                 |
|------------------------|----------------------------------------------------------------------------|
| Language               | Python, PySpark, SQL                                                       |
| Data Lakehouse         | Databricks, Delta Lake, Unity Catalog                                      |
| Ingestion              | Auto Loader, DLT                                                           |
| Transformation         | Spark UDFs, chunking, metadata extraction                                  |
| Vector Search          | Databricks Delta Sync Index (Vector Search)                                |
| Embedding Model        | `e5-small-v2`                                                              |
| LLM for RAG            | `databricks-llama-4-maverick` (via Foundation Model Serving)               |
| API Deployment         | Databricks Model Serving                                                   |
| Observability          | Model Context Protocol (MCP), logging to Delta Lake                        |

---

### 🧱 Medallion Architecture

```mermaid
graph TD;
    A[Ingest PDFs to Bronze] --> B[Parse & Clean to Silver];
    B --> C[Chunk + Metadata + Embeddings to Gold];
    C --> D[Vector Search Index];
    D --> E[Query Relevant Chunks];
    E --> F[Pass to LLM with RAG Prompt];
    F --> G[Return Final Answer via API];
    G --> H[Log via MCP to Delta Lake];
