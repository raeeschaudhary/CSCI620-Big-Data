# Question 3
# Users with reputation more than 100 who have commented on at least 10 posts.
# First Query on the Materialized global sources 
q3_query1_materialized = """
    SELECT AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, COUNT(AllCommentsMaterialized.PostId) AS CommentCount, AllUsersMaterialized.Reputation
    FROM AllUsersMaterialized
    INNER JOIN AllCommentsMaterialized ON AllUsersMaterialized.Id = AllCommentsMaterialized.UserId
    WHERE AllUsersMaterialized.Reputation > 100
    GROUP BY AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, AllUsersMaterialized.Reputation
    HAVING COUNT(AllCommentsMaterialized.PostId) >= 10
    ORDER BY AllUsersMaterialized.Reputation DESC;
    """
# First Query on the Non-Materialized 
q3_query1_non_materialized = """
    SELECT AllUsers.Id, AllUsers.DisplayName, COUNT(AllComments.PostId) AS CommentCount, AllUsers.Reputation
    FROM AllUsers 
    INNER JOIN AllComments ON AllUsers.Id = AllComments.UserId
    WHERE AllUsers.Reputation > 100
    GROUP BY AllUsers.Id, AllUsers.DisplayName, AllUsers.Reputation
    HAVING COUNT(AllComments.PostId) >= 10
    ORDER BY AllUsers.Reputation DESC;
    """
# Users whose display name starts with “john-” and who have never commented on any post with the tag “networking”.
# Second Query on the Materialized global views 
q3_query2_materialized = """
    SELECT 
    AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, AllUsersMaterialized.Reputation
    FROM AllUsersMaterialized
    WHERE AllUsersMaterialized.DisplayName LIKE 'john-%'
        AND AllUsersMaterialized.Id NOT IN (
            SELECT AllCommentsMaterialized.UserId
            FROM AllCommentsMaterialized
            INNER JOIN AllPostsMaterialized ON AllCommentsMaterialized.PostId = AllPostsMaterialized.Id
            WHERE AllPostsMaterialized.Tag = 'networking'
        )
	ORDER BY AllUsersMaterialized.Reputation DESC;
    """
# Second Query on the Non-Materialized global views 
q3_query2_non_materialized = """
    SELECT 
    AllUsers.Id, AllUsers.DisplayName, AllUsers.Reputation
    FROM AllUsers
    WHERE AllUsers.DisplayName LIKE 'john-%'
        AND AllUsers.Id NOT IN (
            SELECT AllComments.UserId
            FROM AllComments
            INNER JOIN AllPosts ON AllComments.PostId = AllPosts.Id
            WHERE AllPosts.Tag = 'networking'
        )
	ORDER BY AllUsers.Reputation DESC;
    """


# Question 4
# Users with reputation more than 100 who have commented on at least 10 posts.
# First Query on the Materialized on sources 
q4_query1_materialized = """
    SELECT AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, COUNT(AllCommentsMaterialized.PostId) AS CommentCount, AllUsersMaterialized.Reputation
    FROM (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsersMaterialized
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsersMaterialized
    ) AS AllUsersMaterialized

    INNER JOIN 
        (
            SELECT UserId, PostId
            FROM CommentedOnMaterialized
        ) AS AllCommentsMaterialized ON AllUsersMaterialized.Id = AllCommentsMaterialized.UserId
    
    WHERE AllUsersMaterialized.Reputation > 100
    GROUP BY AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, AllUsersMaterialized.Reputation
    HAVING COUNT(AllCommentsMaterialized.PostId) >= 10
    ORDER BY AllUsersMaterialized.Reputation DESC;
    """

# First Query on the Non-Materialized on sources 
q4_query1_non_materialized = """
    SELECT AllUsers.Id, AllUsers.DisplayName, COUNT(AllComments.PostId) AS CommentCount, AllUsers.Reputation
    FROM (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsers
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsers
    ) AS AllUsers

    INNER JOIN 
        (
            SELECT UserId, PostId
            FROM CommentedOn
        ) AS AllComments ON AllUsers.Id = AllComments.UserId
    
    WHERE AllUsers.Reputation > 100
    GROUP BY AllUsers.Id, AllUsers.DisplayName, AllUsers.Reputation
    HAVING COUNT(AllComments.PostId) >= 10
    ORDER BY AllUsers.Reputation DESC;
    """
# Users whose display name starts with “john-” and who have never commented on any post with the tag “networking”.
# Second Query on the Materialized on sources 
q4_query2_materialized = """
    SELECT AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, AllUsersMaterialized.Reputation
    FROM 
    (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsersMaterialized
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsersMaterialized
    ) AS AllUsersMaterialized

    WHERE AllUsersMaterialized.DisplayName LIKE 'john-%'
        AND AllUsersMaterialized.Id NOT IN (
            SELECT AllCommentsMaterialized.UserId
            FROM 
            (
                SELECT UserId, PostId
                FROM CommentedOnMaterialized
            ) AS AllCommentsMaterialized
            INNER JOIN 
            (
                SELECT Id, Title, Score, 'networking' AS Tag
                FROM NetworkingPostsMaterialized
                
                UNION
                
                SELECT Id, Title, Score, 'other' AS Tag
                FROM NonNetworkingPostsMaterialized
            ) AS AllPostsMaterialized ON AllCommentsMaterialized.PostId = AllPostsMaterialized.Id

            WHERE AllPostsMaterialized.Tag = 'networking'
        )
	ORDER BY AllUsersMaterialized.Reputation DESC;
    """

# Second Query on the Non-Materialized global views 
q4_query2_non_materialized = """
    SELECT 
    AllUsers.Id, AllUsers.DisplayName, AllUsers.Reputation
    FROM 
    (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsers
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsers
    ) AS AllUsers

    WHERE AllUsers.DisplayName LIKE 'john-%'
        AND AllUsers.Id NOT IN (
            SELECT AllComments.UserId
            FROM 
            (
                SELECT UserId, PostId
                FROM CommentedOn
            ) AS AllComments
            INNER JOIN 
            (
                SELECT Id, Title, Score, 'networking' AS Tag
                FROM NetworkingPosts
                
                UNION
                
                SELECT Id, Title, Score, 'other' AS Tag
                FROM NonNetworkingPosts
            ) AS AllPosts ON AllComments.PostId = AllPosts.Id

            WHERE AllPosts.Tag = 'networking'
        )
	ORDER BY AllUsers.Reputation DESC;
    """



# Question 5
# Users with reputation more than 100 who have commented on at least 10 posts.
# First Query on the Materialized - optimization
q5_query1_materialized = """
    SELECT AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, COUNT(CommentedOnMaterialized.PostId) AS CommentCount, AllUsersMaterialized.Reputation
    FROM (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsersMaterialized
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsersMaterialized
    ) AS AllUsersMaterialized

    INNER JOIN CommentedOnMaterialized ON AllUsersMaterialized.Id = CommentedOnMaterialized.UserId
    
    WHERE AllUsersMaterialized.Reputation > 100
    GROUP BY AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, AllUsersMaterialized.Reputation
    HAVING COUNT(CommentedOnMaterialized.PostId) >= 10
    ORDER BY AllUsersMaterialized.Reputation DESC;
    """

# First Query on the Non-Materialized on sources 
q5_query1_non_materialized = """
    SELECT AllUsers.Id, AllUsers.DisplayName, COUNT(CommentedOn.PostId) AS CommentCount, AllUsers.Reputation
    FROM (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsers
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsers
    ) AS AllUsers

    INNER JOIN CommentedOn ON AllUsers.Id = CommentedOn.UserId
    
    WHERE AllUsers.Reputation > 100
    GROUP BY AllUsers.Id, AllUsers.DisplayName, AllUsers.Reputation
    HAVING COUNT(CommentedOn.PostId) >= 10
    ORDER BY AllUsers.Reputation DESC;
    """
# Users whose display name starts with “john-” and who have never commented on any post with the tag “networking”.
# Second Query on the Materialized on sources 
q5_query2_materialized = """
    SELECT AllUsersMaterialized.Id, AllUsersMaterialized.DisplayName, AllUsersMaterialized.Reputation
    FROM 
    (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsersMaterialized
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsersMaterialized
    ) AS AllUsersMaterialized

    WHERE AllUsersMaterialized.DisplayName LIKE 'john-%'
        AND AllUsersMaterialized.Id NOT IN (
            SELECT CommentedOnMaterialized.UserId
            FROM CommentedOnMaterialized
            INNER JOIN 
            (
                SELECT Id, Title, Score, 'networking' AS Tag
                FROM NetworkingPostsMaterialized
                
                UNION
                
                SELECT Id, Title, Score, 'other' AS Tag
                FROM NonNetworkingPostsMaterialized
            ) AS AllPostsMaterialized ON CommentedOnMaterialized.PostId = AllPostsMaterialized.Id

            WHERE AllPostsMaterialized.Tag = 'networking'
        )
	ORDER BY AllUsersMaterialized.Reputation DESC;
    """

# Second Query on the Non-Materialized - optimized
q5_query2_non_materialized = """
    SELECT 
    AllUsers.Id, AllUsers.DisplayName, AllUsers.Reputation
    FROM 
    (
        SELECT Id, DisplayName, Reputation
        FROM NetworkingUsers
        
        UNION
        
        SELECT Id, DisplayName, Reputation
        FROM NonNetworkingUsers
    ) AS AllUsers

    WHERE AllUsers.DisplayName LIKE 'john-%'
        AND AllUsers.Id NOT IN (
            SELECT CommentedOn.UserId
            FROM CommentedOn
            INNER JOIN 
            (
                SELECT Id, Title, Score, 'networking' AS Tag
                FROM NetworkingPosts
                
                UNION
                
                SELECT Id, Title, Score, 'other' AS Tag
                FROM NonNetworkingPosts
            ) AS AllPosts ON CommentedOn.PostId = AllPosts.Id

            WHERE AllPosts.Tag = 'networking'
        )
	ORDER BY AllUsers.Reputation DESC;
    """
