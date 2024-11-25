# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install `psycopg2==2.9.9` in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` (if you do not have it)
4. Navigate to `globals.py` in the root folder to provide database path and credentials in `db_config`. Save the file.
    - This expects that the database is already populated from assignment 2.
    - If database is not populated, run assingment 2 again to populate the database. 
5. From the root folder, Individually run following.
    - Run `q_123.py` e.g., `python q_123.py` from the terminal to L1, L2, L3 frequent itemsets. Observe output on console. (7 seconds)
    - Run `q4.py` e.g., `python q4.py` from the terminal to run sequential lattice creation. (10 seconds)
    - All output will be displayed in terminal.

## Directory structures
    ├── sql
    │   ├── __init__.py
    │   ├── db_utils.py
    │   └── queries.py
    ├── globals.py
    ├── q_123.py
    ├── q4.py
    ├── README.md
    └── requirements.txt