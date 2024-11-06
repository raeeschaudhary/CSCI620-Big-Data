# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install `pymongo==4.8.0` in your existing environment)
3. Install the python package from requirements.txt e.g., `pip install pymongo==4.8.0` (if you do not have it)
4. Navigate to `funcs/globals.py` to provide database path and credentials in `db_config`. Save the file.
5. Navigate to `funcs/globals.py` to provide the dataset path. Make sure the path contains the final slash. Also, the code expects to all input files (posts, comments, users, badges) exist in the data folder. Save the file.
6. Navigage back to root folder.
    - Optional:  Run `analyze_data.py` e.g., `python analyze_data.py` to view data statistics (keys, nulls, types). Resutls are stored in `file_info.txt` in the root folder. 
7. Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to create and populate the database. (takes around 25 mins)
    - Run `q2.py` e.g., `python q2.py` from the terminal to execute queries. This requires that database exists and populated.
    - Run `q3.py` e.g., `python q3.py` from the terminal to visualize and save query plan. This requires that database exists and populated.
    - Run `q4.py` e.g., `python q4.py` from the terminal to create indexes. This requires that database exists and populated. 
    - Run the question 2 and question 3 again to observe the difference. 
    - The query plans will be saved as txt files in the root directory.

## Directory structures
    ├── funcs
    │   ├── __init__.py
	│   ├── db_methods.py
    │   ├── db_utils.py
	│   ├── globals.py
	│   └── queries.py
    ├── analyze_data.py
    ├── q1.py
    ├── q2.py
    ├── q3.py
    ├── q4.py
    ├── README.md
    └── requirements.txt