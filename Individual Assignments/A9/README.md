# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install or have `pyspark` in your existing environment)
3. Install the python package `pyspark` from requirements.txt e.g., `pip install pyspark==3.5.1` (if you do not have it)
4. Navigate to `globals.py` to provide the dataset path on line 2 in `data_directory`. Make sure the path contains the final slash. Save the file. 
    - Make sure the JSON files are downloaded, extracted and are placed inside the `data_directory` folder. 
    - Expected files are `Users.json`, `Posts.json`, `Comments.json` and `Badges.json`.
5. From the root folder, Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to run query 1.
    - Run `q2.py` e.g., `python q2.py` from the terminal to run query 2.
    - Run `q3.py` e.g., `python q3.py` from the terminal to run query 3.
    - Run `q4.py` e.g., `python q4.py` from the terminal to run query 4.
    - All output will be displayed in terminal.


## Directory structures
    ├── globals.py
    ├── q1.py
    ├── q2.py
    ├── q3.py
    ├── q4.py
    ├── README.md
    └── requirements.txt