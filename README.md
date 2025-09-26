# MariaDb_HackerEarth
MariaDB Hackathon on HackerEarth.com

NATURAL LANGUAGE SEARCH SYSTEM FOR AIRPORT DATA

An AI-powered query system that understands natural language using MariaDB's vector capabilities

FEATURES
- Semantic Search = Vector embeddings for natural language
- Hybrid Algorithms = Cosine Similarity, Euclidean Distance, Geographic-Semantic Search
- MariaDB Integration = Native VECTOR(384) data type storage
- Role-Based Security = Secure access control system
- Interactive GUI = Real-time search demonstrations


PREREQUISITES 
- MariaDB with Vector Support
- Python
- MySQL


--- PACKAGES ---
Flask==3.1.1
mariadb==1.1.13
mysql-connector-python==9.4.0
numpy==2.3.3
openai==1.108.2
pandas==2.3.2
transformers==4.56.1

--- DATABASE SETUP ---
``` bash
python import_data.py
python generate_embeddings.py

RUN python vector_search.py
