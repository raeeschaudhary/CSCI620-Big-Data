# insert queries for each table are provided under. 
users_query = """
    INSERT INTO users (Id, AccountId, DisplayName, AboutMe, CreationDate, Reputation) 
    VALUES %s
    """

tags_query = """
    INSERT INTO tags (Id, TagName)
    VALUES %s
    """

dummy_posts_query = """ INSERT INTO dummyposts (Id, UserId)
    VALUES %s 
"""

posts_query = """
    INSERT INTO posts (Id, ParentId, OwnerUserId, AcceptedAnswerId, Title, Body, Score, ViewCount, CreationDate)
    VALUES %s
    """

post_tags_query = """
    INSERT INTO posttags (PostId, TagId)
    VALUES %s
    """

final_query = """
    ALTER TABLE Posts ADD CONSTRAINT fk_ParentId FOREIGN KEY (ParentId) REFERENCES Posts(Id) ON DELETE CASCADE; 
    ALTER TABLE Posts ADD CONSTRAINT fk_AcceptedAnswerId FOREIGN KEY (AcceptedAnswerId) REFERENCES Posts(Id) ON DELETE CASCADE; 
    """

join_tables_q1 = """
    DROP TABLE IF EXISTS joined_posts CASCADE;

    CREATE TABLE joined_posts AS 
    (
        SELECT 
            ROW_NUMBER() OVER () AS JoinId,
            users.Id AS UserId,
            users.DisplayName AS UserDisplayName,
            users.AboutMe AS AboutMe,
            users.CreationDate AS UserCreationDate,
            users.Reputation AS Reputation,
            posts.Id AS PostId,
            posts.Title AS PostTitle,
            posts.Body AS PostBody,
            posts.Score AS PostScore,
            posts.ViewCount AS PostViewCount,
            posts.CreationDate AS PostCreationDate,
            tags.Id AS TagId,
            tags.TagName AS TagName
        FROM 
            users 
        INNER JOIN posts ON users.Id = posts.OwnerUserId 
        INNER JOIN posttags ON posts.Id = posttags.PostId 
        INNER JOIN tags ON posttags.TagId = tags.Id 
        GROUP BY 
            users.Id, users.DisplayName, users.AboutMe, users.CreationDate, users.Reputation, 
            posts.Id, posts.Title, posts.Body, posts.Score, posts.ViewCount, posts.CreationDate, 
            tags.Id, tags.TagName 
        HAVING 
            COUNT(posttags.TagId) = 1
    );
"""

selection_q2 = """
    SELECT * FROM joined_posts;
    """
columns_q2 = """
    SELECT column_name FROM information_schema.columns 
    WHERE table_schema = 'public' AND table_name = 'joined_posts';
    """