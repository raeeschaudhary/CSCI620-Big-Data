# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install psycopg2==2.9.9 and pandas==2.2.2 in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install psycopg2==2.9.9` and `pip install pandas==2.2.2` (if you do not have these.)
4. Navigate to `code/globals.py` to provide database path and credentials. Save the file.
5. If the database is not pouplated;
    - Navigate to `code/globals.py` and set `db_exists = False`. Save the file.
    - Navigate to `code/globals.py` to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files in the data folder. Save the file.
6. If the database is populated. Navigate to `code/globals.py` and set `db_exists = True`. Save the file. 
7. Navigage back to root folder.
7. Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to create and populate the database. (Takes 7 mins with populate, 40 seconds if already populated)
    - Run `q2.py` e.g., `python q2.py` from the terminal to run naive dependency execution. This requires that database exists and populated. Takes 5 minutes to run partially. 
    - Run `q3.py` e.g., `python q3.py` from the terminal to run prunning dependency execution. This requires that database exists and populated. Takes 65 minutes to run fully.
    - Question 4 and Question 5 are explained in the report.  
    - 


## Directory structures
    ├── code
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
	│   ├── globals.py
	│   └── queries.py
    ├── create_schema.sql
    ├── q1.py
    ├── q2_fdss.txt
    ├── q2.py
    ├── q3_fd_prune.txt
    ├── q3_fd_prune.txt
    ├── q3.py
    ├── q5_min_fds.txt
    ├── README.md
    └── requirements.txt