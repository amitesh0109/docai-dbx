# Data Pipeline To Genrate Embeddings & RAG-based Document Q&A System using Databricks

### Project Overview

This project implements a scalable, production-grade data engineering pipeline using Databricks that enables semantic search and LLM-powered Q&A over a collection of documents (PDFs, text files, etc.).

It uses:
- the Medallion architecture (Bronze → Silver → Gold),
- Delta Lake for data reliability,
- Databricks Vector Search for similarity retrieval,
- Databricks Foundation Model Serving** to power Retrieval-Augmented Generation (RAG).

---

### Tech Stack

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

### Databricks Catalog Overview

![Alt text](https://github.com/amitesh0109/docai-dbx/blob/main/Databrick%20Catalog%20Overview.png)

---

### Instructions

- Configure files with prefix 01,02,03 respective under seperate task in a job.
- Upload a document in the source volumn under source schema.
- Run job to generate the embeddings table, it basically stores your documents data in chunks each chunk represented by a vector.
- Run notebook that creates vector endpoint and a vector index on your embeddings column in the table under gold schema.
- Run Main notebook with your query and user.

---

### Medallion Architecture

```mermaid
graph TD;
    A[Ingest PDFs to Bronze] --> B[Parse & Clean to Silver];
    B --> C[Chunk + Metadata + Embeddings to Gold];
    C --> D[Vector Search Index];
    D --> E[Query Relevant Chunks];
    E --> F[Pass to LLM with RAG Prompt];
    F --> G[Return Final Answer via API];
    G --> H[Log via MCP to Delta Lake];
