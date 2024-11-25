# Question 1
# Level 1 table with size 1 frequest itemset; tag and count
q1_level1 = """
    DROP TABLE IF EXISTS L1;
    CREATE TABLE L1 AS
    (
        SELECT PostTags.TagId as tag1, COUNT(PostTags.PostId) AS count
        FROM PostTags
        GROUP BY PostTags.TagId
        HAVING COUNT(PostTags.PostId) >= 100
        ORDER BY count DESC -- just for viewing 
    );
    
    """

# Question 2
# Level 2 table with size 2 frequest itemset; only using tags from L1
q2_level2 = """
    DROP TABLE IF EXISTS L2;
    CREATE TABLE L2 AS
    (
        SELECT L1_1.tag1 AS tag1, PostTags.TagId AS tag2, COUNT(PostTags.PostId) AS count
        FROM PostTags
        INNER JOIN 
        (
            SELECT L1.tag1 AS tag1, PT1.PostId AS PostId
            FROM L1
            INNER JOIN PostTags AS PT1 ON PT1.TagId = L1.tag1 
        ) AS L1_1
        ON PostTags.PostId = L1_1.PostId
        WHERE PostTags.TagId > L1_1.tag1
        GROUP BY PostTags.TagId, L1_1.tag1
        HAVING COUNT(PostTags.PostId) >= 100
        ORDER BY count DESC -- just for viewing
    );

    """

# Question 3
# Level 3 table with size 3 frequest itemset; only using tags from L2
q3_level3 = """
    DROP TABLE IF EXISTS L3;
    CREATE TABLE L3 AS
    (
        SELECT L2_1.tag1 AS tag1, L2_1.tag2 AS tag2, PostTags.TagId AS tag3, COUNT(L2_1.PostId) AS count
        FROM PostTags
        INNER JOIN 
        (
            SELECT L2.tag1 AS tag1, L2.tag2 AS tag2, PT1.PostId AS PostId
            FROM L2
            INNER JOIN PostTags AS PT1 ON PT1.TagId = L2.tag1
            INNER JOIN PostTags AS PT2 ON PT2.TagId = L2.tag2
            WHERE PT1.PostId = PT2.PostId
        ) AS L2_1
        ON PostTags.PostId = L2_1.PostId
        WHERE PostTags.TagId > L2_1.tag1 AND PostTags.TagId > L2_1.tag2
        GROUP BY PostTags.TagId, L2_1.tag1, L2_1.tag2
        HAVING COUNT(PostTags.PostId) >= 100
        ORDER BY count DESC -- just for viewing 
    );
    
    """

# Question 4
# Level (L) 2...N table with size L frequest itemset; only using tags from LN - 1
def q4_lattice_query(level, prev_level, curr_level):
    """
    creates a dynamic query based on the lattice levels.
    
    :param level: The level to keep track of the levels processed.
    :param prev_level: Previous level to access last created level.
    :param curr_level: Current level to be created or accessed.
    :returns query to create a new level in database.
    """
    # query part for creating joins based on the level from columns in the previous level
    # for each level, generate the joins upto previous levels 
    inner_joins = " ".join([f"INNER JOIN PostTags AS PT{i} ON PT{i}.TagId = L{level - 1}.tag{i}" for i in range(1, level)])
    # WHERE clause is added after level 2 to enable self-match with post id as used in q3_level3 explained above
    where_clause = ""
    if level > 2:
        where_clause = "WHERE " + " AND ".join([f"PT{i}.PostId = PT1.PostId" for i in range(2, level)])
    # generate SQL query for the current level; first drop the table if exisits, then use a lower level to build upon previous level
    # then use PostTags to match going to a level up, join with post tags for all previous levels, add where clause to map post ids,
    # then join after matching using aliases for post tags
    query = f"""
        DROP TABLE IF EXISTS {curr_level};
        CREATE TABLE {curr_level} AS (
            SELECT {", ".join([f"L{level - 1}_1.tag{i}" for i in range(1, level)])}, PostTags.TagId AS tag{level}, COUNT(PostTags.PostId) AS count
            FROM PostTags
            INNER JOIN (
                SELECT {", ".join([f"L{level - 1}.tag{i}" for i in range(1, level)])}, PT1.PostId
                FROM {prev_level} AS L{level - 1}
                {inner_joins}
                {where_clause}
            ) AS L{level-1}_1
            ON PostTags.PostId = L{level - 1}_1.PostId
            WHERE {" AND ".join([f"PostTags.TagId > L{level - 1}_1.tag{i}" for i in range(1, level)])}
            GROUP BY {", ".join([f"L{level - 1}_1.tag{i}" for i in range(1, level)])}, PostTags.TagId
            HAVING COUNT(PostTags.PostId) >= 100
            ORDER BY count DESC
        );

        """
    # return generated query
    return query

# Question 4
# generate the final query 
def q4_final_query(level, final_level):
    """
    creates a dynamic query based on the lattice levels.
    
    :param level: The level to keep track of the levels processed.
    :param final_level: The final non-empty level in the lattice.
    :returns query to fetch results with tag names.
    """
    # query for creating joins based on the level
    query = f"""
        SELECT {', '.join([f"Tags{i}.TagName AS tag{i}_name" for i in range(1, level)])}, {final_level}.count
        FROM {final_level}
        {" ".join([f"INNER JOIN Tags AS Tags{i} ON Tags{i}.Id = {final_level}.tag{i}" for i in range(1, level)])}
        ORDER BY {final_level}.count DESC;

        """
    # return generated query
    return query