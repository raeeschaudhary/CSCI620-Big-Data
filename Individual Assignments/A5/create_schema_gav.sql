-- -- The code first drops each view to clear up and then creates the view again.

-- -- Create global AllPosts View Non-Materialized 
DROP VIEW IF EXISTS AllPosts;
CREATE VIEW AllPosts AS
(
    SELECT Id, Title, Score, 'networking' AS Tag
    FROM NetworkingPosts
    
    UNION
    
    SELECT Id, Title, Score, 'other' AS Tag
    FROM NonNetworkingPosts
);

-- -- Create global AllPosts View Materialized 
DROP MATERIALIZED VIEW IF EXISTS AllPostsMaterialized;
CREATE MATERIALIZED VIEW AllPostsMaterialized AS
(
    SELECT Id, Title, Score, 'networking' AS Tag
    FROM NetworkingPostsMaterialized
    
    UNION
    
    SELECT Id, Title, Score, 'other' AS Tag
    FROM NonNetworkingPostsMaterialized
);

-- -- Create global AllUsers View Non-Materialized 
DROP VIEW IF EXISTS AllUsers;
CREATE VIEW AllUsers AS
(
    SELECT Id, DisplayName, Reputation
    FROM NetworkingUsers
    
    UNION
    
    SELECT Id, DisplayName, Reputation
    FROM NonNetworkingUsers
);

-- -- Create global AllUsers View Materialized 
DROP MATERIALIZED VIEW IF EXISTS AllUsersMaterialized;
CREATE MATERIALIZED VIEW AllUsersMaterialized AS
(
    SELECT Id, DisplayName, Reputation
    FROM NetworkingUsersMaterialized
    
    UNION
    
    SELECT Id, DisplayName, Reputation
    FROM NonNetworkingUsersMaterialized
);

-- -- Create global AllComments View Non-Materialized 
DROP VIEW IF EXISTS AllComments;
CREATE VIEW AllComments AS
(
    SELECT UserId, PostId
    FROM CommentedOn
);

-- -- Create global AllComments View Materialized 
DROP MATERIALIZED VIEW IF EXISTS AllCommentsMaterialized;
CREATE MATERIALIZED VIEW AllCommentsMaterialized AS
(
    SELECT UserId, PostId
    FROM CommentedOnMaterialized
);


