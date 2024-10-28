# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install psycopg2==2.9.9 in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` (if you do not have these.)
4. Navigate to `code/globals.py` to provide database path and credentials. Save the file.
5. Navigate to `code/globals.py` to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files in the data folder. Save the file.
6. Navigage back to root folder.
7. Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to create and populate the database.
    - Run `q2.py` e.g., `python q2.py` from the terminal to execute queries. This requires that database exists and populated.
    - Run `q3.py` e.g., `python q3.py` from the terminal to visualize and save query plan. This requires that database exists and populated.
    - Run `q5.py` e.g., `python q5.py` from the terminal to create indexes. This requires that database exists and populated. 
    - Restart the database server and run the question 2 and question 3 again to observe the difference. 
    - The query plans will be generated as txt files in the root directory.
    - Example to restart database server; `pg_ctl.exe restart -D "C:\Program Files\PostgreSQL\16\data` from the path your binary is installed for postgres (on Windows)


## Directory structures
    ├── code
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
	│   ├── globals.py
	│   └── queries.py
    ├── create_schema.sql
    ├── q1.py
    ├── q2.py
    ├── q3.py
    ├── q5.py
    ├── README.md
    └── requirements.txt