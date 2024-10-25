# insert queries for each table are provided under. 
users_query = """
    INSERT INTO users (Id, AccountId, DisplayName, AboutMe, CreationDate, Reputation) 
    VALUES %s
    """

tags_query = """
    INSERT INTO tags (Id, TagName)
    VALUES %s
    """

badges_query = """
    INSERT INTO badges (Id, UserId, Name, Date)
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

comments_query = """
    INSERT INTO comments (Id, PostId, Score, Text, CreationDate, UserId)
    VALUES %s
    """

final_query = """
    ALTER TABLE Posts ADD CONSTRAINT fk_ParentId FOREIGN KEY (ParentId) REFERENCES Posts(Id) ON DELETE CASCADE; 
    ALTER TABLE Posts ADD CONSTRAINT fk_AcceptedAnswerId FOREIGN KEY (AcceptedAnswerId) REFERENCES Posts(Id) ON DELETE CASCADE; 
    """

# selection queries are provided as under;
q2_query1 = """
    SELECT Badges.Name, COUNT(Badges.Id) AS EarnedBadges FROM Badges
    INNER JOIN Users ON Badges.UserId = Users.Id
    WHERE Badges.Date BETWEEN Users.CreationDate AND (Users.CreationDate + INTERVAL '1 year')
    GROUP BY Badges.Name
    ORDER BY EarnedBadges DESC
    LIMIT 10;
"""

q3_query1_explain = """
    EXPLAIN SELECT Badges.Name, COUNT(Badges.Id) AS EarnedBadges FROM Badges
    INNER JOIN Users ON Badges.UserId = Users.Id
    WHERE Badges.Date BETWEEN Users.CreationDate AND (Users.CreationDate + INTERVAL '1 year')
    GROUP BY Badges.Name
    ORDER BY EarnedBadges DESC
    LIMIT 10;
"""

q2_query2 = """
    SELECT DisplayName FROM Users
    LEFT JOIN Posts ON Users.Id = Posts.OwnerUserId
    WHERE Posts.OwnerUserId IS NULL AND Users.Reputation > 1000;
"""

q3_query2_explain = """
    EXPLAIN SELECT DisplayName FROM Users
    LEFT JOIN Posts ON Users.Id = Posts.OwnerUserId
    WHERE Posts.OwnerUserId IS NULL AND Users.Reputation > 1000;
"""

q2_query3 = """
    SELECT Users.DisplayName, Users.Reputation
    FROM Users
    INNER JOIN Posts ON Users.Id = Posts.OwnerUserId
    INNER JOIN PostTags ON Posts.Id = PostTags.PostId
    INNER JOIN Tags ON PostTags.TagId = Tags.Id
    WHERE Posts.ParentId IS NOT NULL AND Tags.TagName = 'postgresql'
    GROUP BY Users.Id
    HAVING COUNT(Posts.Id) > 1;  
"""

q3_query3_explain = """
    EXPLAIN SELECT Users.DisplayName, Users.Reputation
    FROM Users
    INNER JOIN Posts ON Users.Id = Posts.OwnerUserId
    INNER JOIN PostTags ON Posts.Id = PostTags.PostId
    INNER JOIN Tags ON PostTags.TagId = Tags.Id
    WHERE Posts.ParentId IS NOT NULL AND Tags.TagName = 'postgresql'
    GROUP BY Users.Id
    HAVING COUNT(Posts.Id) > 1;
"""

q2_query4 = """
    SELECT DisplayName FROM Users
    INNER JOIN Comments ON Users.Id = Comments.UserId
    WHERE Comments.Score > 10 AND Comments.CreationDate BETWEEN Users.CreationDate AND (Users.CreationDate + INTERVAL '7 days');
"""

q3_query4_explain = """
    EXPLAIN SELECT DisplayName FROM Users
    INNER JOIN Comments ON Users.Id = Comments.UserId
    WHERE Comments.Score > 10 AND Comments.CreationDate BETWEEN Users.CreationDate AND (Users.CreationDate + INTERVAL '7 days');
"""

q2_query5 = """
    SELECT TagName, COUNT(PostTags.PostId) AS TagCount FROM PostTags
    INNER JOIN Tags ON PostTags.TagId = Tags.Id
    INNER JOIN Posts ON PostTags.PostId = Posts.Id
    WHERE 
        Posts.Id IN (
            SELECT p.Id
            FROM Posts AS p
            JOIN PostTags AS pt ON p.Id = pt.PostId
            JOIN Tags t ON pt.TagId = t.Id
            WHERE t.TagName = 'postgresql'
        )
    GROUP BY TagName
    ORDER BY TagCount DESC;
"""

q3_query5_explain = """
    EXPLAIN SELECT TagName, COUNT(PostTags.PostId) AS TagCount FROM PostTags
    INNER JOIN Tags ON PostTags.TagId = Tags.Id
    INNER JOIN Posts ON PostTags.PostId = Posts.Id
    WHERE 
        Posts.Id IN (
            SELECT p.Id
            FROM Posts AS p
            JOIN PostTags AS pt ON p.Id = pt.PostId
            JOIN Tags t ON pt.TagId = t.Id
            WHERE t.TagName = 'postgresql'
        )
    GROUP BY TagName
    ORDER BY TagCount DESC;
"""


indexing_queries = """
    CREATE INDEX idx_users_reputation ON users (Reputation);
    CREATE INDEX idx_posts_owner_user ON posts (OwnerUserId);
    CREATE INDEX idx_posts_parent ON posts (ParentId);
    CREATE INDEX idx_comments_post ON comments (PostId);
    CREATE INDEX idx_comments_user ON comments (UserId);
    CREATE INDEX idx_posttags_post ON posttags (PostId);
    CREATE INDEX idx_posttags_tag ON posttags (TagId);
    CREATE INDEX idx_badges_user ON badges (UserId);
    CREATE INDEX idx_tags_name ON tags USING gin(to_tsvector('english', TagName));
"""