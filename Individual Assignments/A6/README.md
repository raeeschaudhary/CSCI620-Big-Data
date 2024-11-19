# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install or have `pymongo`, `pandas`, and `matplotlib` in your existing environment)
3. Install the python package `pymongo` from requirements.txt e.g., `pip install pymongo==4.8.0` (if you do not have it)
    - For Question 4. Install the python package `pandas` from requirements.txt e.g., `pip install pandas==2.2.2` (if you do not have it)
    - For Question 4. Install the python package `matplotlib` from requirements.txt e.g., `pip install matplotlib==3.9.0` (if you do not have it)
4. Navigate to `globals.py` in the root folder to provide database path and credentials in `db_config`. Save the file.
5. Navigate to `globals.py` to provide the dataset path in `data_directory` where original data files are placed. Make sure the path contains the final slash. Save the file. 
    - Download the `extra-data.json` and place it in the same directory as provided in `data_directory` in `globals.py`
6. Assignment 4 code removed the Id keys, which are required to map account Ids with users. Therefore run `insert_data.py` e.g., `python insert_data.py` from the terminal to repopulate the data. (40 minutes)
7. From the root folder, Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to explore extra-data file. (1 seconds)
    - Run `q2.py` e.g., `python q2.py` from the terminal to first insert account IDs in users collection and then add sites data. (6 minutes)
    - Run `q3.py` e.g., `python q3.py` from the terminal to report matching with and without account id in the given file. (45 seconds)
    - Run `q4.py` e.g., `python q4.py` from the terminal to report and save plots in the root directory for given queries. (10 seconds)
    - All output will be displayed in terminal.

## Directory structures
    ├── mongo
    │   ├── __init__.py
    │   └── db_methods.py
    ├── globals.py
    ├── insert_data.py
    ├── q1.py
    ├── q2.py
    ├── q3.py
    ├── q4_1_plot.png
    ├── q4_2_plot.png
    ├── q4_3_plot.png
    ├── q4.py
    ├── README.md
    └── requirements.txt