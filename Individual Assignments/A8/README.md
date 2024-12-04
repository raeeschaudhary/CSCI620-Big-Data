# Running the Code

1. Navigate to root folder.
2. Activate your virtual environment. (unless you have or can install or have `pymongo` and `matplotlib` in your existing environment)
3. Install the python package `pymongo` from requirements.txt e.g., `pip install pymongo==4.8.0` (if you do not have it)
    - For Question 4. Install the python package `matplotlib` from requirements.txt e.g., `pip install matplotlib==3.9.0` (if you do not have it)

4. Navigate to `globals.py` in the root folder to provide database path and credentials in `db_config`. Save the file.
5. Navigate to `globals.py` to provide the dataset path in `data_directory` where original data files are placed. Make sure the path contains the final slash. Save the file. 
    - We will need `Users.xml` and `Posts.xml` to repopulate users and posts with tags. Users are required for valid posts. 
6. Run `insert_data.py` e.g., `python insert_data.py` from the terminal to repopulate and users, posts (posts need valid users). (6 minutes)
7. From the root folder, Individually run each question.
    - Run `q1.py` e.g., `python q1.py` from the terminal to update collection with kmeans Norms. (15 seconds)
    - Run `q2.py` e.g., `python q2.py` from the terminal to select K samples from T tags. (15 seconds)
        - First navigate to `globals.py` and set `K` (samples) as a number and `T` as a string valid tag from `subset_tags`. 
    - Run `q3.py` e.g., `python q3.py` from the terminal to to run one step of k-means. For multiple steps, see instructions in `q3.py`. (5 seconds)
        - First navigate to `globals.py` and set `K` (samples) as a number and `T` as a string valid tag from `subset_tags`.
    - Run `q4.py` e.g., `python q4.py` from the terminal to to run K-means for each tag from `subset_tags`. See terminal for updates. (30 minutes)
        - For each tag, K is chosen from 10 to 50 with step of 5 for each execution. Each execution for K, runs upto 100 iterations if solution does not converge before that. 
    - Run `q5.py` e.g., `python q5.py` from the terminal to to run K-means for each tag from `subset_tags` with best value of K. See terminal for updates. (40 minutes)
        - Best K values are provided in `best_Ks` array in `globals.py` based on the data I have. If data is different update `best_Ks` by updating values from question 4 graphs. 
        - Analysis code is provided in `q5_nb.ipynb` notbook file. 
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
    ├── q4.py
    ├── q5.py
    ├── q5_nb.ipynb
    ├── README.md
    ├── requirements.txt
    ├── sse_plot_apt.png
    ├── sse_plot_boot.png
    ├── sse_plot_command-line.png
    ├── sse_plot_drivers.png
    └── sse_plot_networking.png