# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install `psycopg2==2.9.9` in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` (if you do not have it)
4. Navigate to `globals.py` in the root folder to provide database path and credentials in `db_config`. Save the file.
    - This expects that the database is already populated from assignment 2.
    - If database is not populated, run assingment 2 again to populate the database. 
5. From the root folder, Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to create local data views (materialized and non materialized).
    - Run `q2.py` e.g., `python q2.py` from the terminal to create global data views (materialized and non materialized).
    - Run `q3.py` e.g., `python q3.py` from the terminal to execute queries on global sources.
    - Run `q4.py` e.g., `python q4.py` from the terminal to execute queries on local sources.
    - Run `q5.py` e.g., `python q5.py` from the terminal to execute queries with optimized exectuion. 
    - All output will be displayed in terminal.

## Directory structures
    ├── sql
    │   ├── __init__.py
    │   ├── queries.py
	│   └── db_utils.py
    ├── create_schema_basic.sql
    ├── create_schema_gav.sql
    ├── globals.py
    ├── q1.py
    ├── q2.py
    ├── q3.py
    ├── q4.py
    ├── q5.py
    ├── README.md
    └── requirements.txt