-- -- The code first drops each view to clear up and then creates the view again.
-- --  CASCADE is used with all views as Q2 depends on these views so droping these after Q2 would cause error without cascade

-- -- Create NetworkingPosts View Non-Materialized

DROP VIEW IF EXISTS NetworkingPosts CASCADE;
CREATE VIEW NetworkingPosts AS
(
    SELECT posts.Id AS Id, posts.Title AS Title, posts.Score AS Score
    FROM posts
    INNER JOIN posttags ON posts.Id = posttags.PostId
    INNER JOIN tags ON posttags.TagId = tags.Id
    WHERE tags.TagName = 'networking'
);

-- -- Create NetworkingPosts View Materialized
DROP MATERIALIZED VIEW IF EXISTS NetworkingPostsMaterialized CASCADE;
CREATE MATERIALIZED VIEW NetworkingPostsMaterialized AS
(
    SELECT posts.Id AS Id, posts.Title AS Title, posts.Score AS Score
    FROM posts
    INNER JOIN posttags ON posts.Id = posttags.PostId
    INNER JOIN tags ON posttags.TagId = tags.Id
    WHERE tags.TagName = 'networking'
);

-- -- Create NonNetworkingPosts View Non-Materialized
DROP VIEW IF EXISTS NonNetworkingPosts CASCADE;
CREATE VIEW NonNetworkingPosts AS
(
    SELECT posts.Id AS Id, posts.Title AS Title, posts.Score AS Score
    FROM posts
    WHERE posts.Id NOT IN (
        SELECT posttags.PostId
        FROM posttags
        INNER JOIN tags ON posttags.TagId = tags.Id
        WHERE tags.TagName = 'networking'
    )
);

-- -- Create NonNetworkingPosts View Materialized
DROP MATERIALIZED VIEW IF EXISTS NonNetworkingPostsMaterialized CASCADE;
CREATE MATERIALIZED VIEW NonNetworkingPostsMaterialized AS
(
    SELECT posts.Id AS Id, posts.Title AS Title, posts.Score AS Score
    FROM posts
    WHERE posts.Id NOT IN (
        SELECT posttags.PostId
        FROM posttags
        INNER JOIN tags ON posttags.TagId = tags.Id
        WHERE tags.TagName = 'networking'
    )
);

-- -- Create NetworkingUser View Non-Materialized
DROP VIEW IF EXISTS NetworkingUsers CASCADE;
CREATE VIEW NetworkingUsers AS
(
    SELECT DISTINCT users.Id AS Id, users.DisplayName AS DisplayName, users.Reputation AS Reputation
    FROM users
    INNER JOIN comments ON users.Id = comments.UserId
    INNER JOIN posts ON comments.PostId = posts.Id
    INNER JOIN posttags ON posts.Id = posttags.PostId
    INNER JOIN tags ON posttags.TagId = tags.Id
    WHERE tags.TagName = 'networking'
);

-- -- Create NetworkingUser View Materialized
DROP MATERIALIZED VIEW IF EXISTS NetworkingUsersMaterialized CASCADE;
CREATE MATERIALIZED VIEW NetworkingUsersMaterialized AS
(
    SELECT DISTINCT users.Id AS Id, users.DisplayName AS DisplayName, users.Reputation AS Reputation
    FROM users
    INNER JOIN comments ON users.Id = comments.UserId
    INNER JOIN posts ON comments.PostId = posts.Id
    INNER JOIN posttags ON posts.Id = posttags.PostId
    INNER JOIN tags ON posttags.TagId = tags.Id
    WHERE tags.TagName = 'networking'
);

-- -- Create NonNetworkingUsers View Non-Materialized
DROP VIEW IF EXISTS NonNetworkingUsers CASCADE;
CREATE VIEW NonNetworkingUsers AS
(
    SELECT DISTINCT users.Id AS Id, users.DisplayName AS DisplayName, users.Reputation AS Reputation
    FROM users
    WHERE users.Id NOT IN (
        SELECT DISTINCT comments.UserId
        FROM comments
        INNER JOIN posts ON comments.PostId = posts.Id
        INNER JOIN posttags ON posts.Id = posttags.PostId
        INNER JOIN tags ON posttags.TagId = tags.Id
        WHERE tags.TagName = 'networking'
    )
);

-- -- Create NetworkingUser View Materialized
DROP MATERIALIZED VIEW IF EXISTS NonNetworkingUsersMaterialized CASCADE;
CREATE MATERIALIZED VIEW NonNetworkingUsersMaterialized AS
(
    SELECT DISTINCT users.Id AS Id, users.DisplayName AS DisplayName, users.Reputation AS Reputation
    FROM users
    WHERE users.Id NOT IN (
        SELECT DISTINCT comments.UserId
        FROM comments
        INNER JOIN posts ON comments.PostId = posts.Id
        INNER JOIN posttags ON posts.Id = posttags.PostId
        INNER JOIN tags ON posttags.TagId = tags.Id
        WHERE tags.TagName = 'networking'
    )
);

-- -- Create CommentedOn View Non-Materialized
-- -- We are certain that records in comments match with users and posts table from assignment 2.
DROP VIEW IF EXISTS CommentedOn CASCADE;
CREATE VIEW CommentedOn AS
(
    SELECT comments.UserId AS UserId, comments.PostId AS PostId
    FROM comments
);

-- -- Create CommentedOn View Materialized
-- -- We are certain that records in comments match with users and posts table from assignment 2.
DROP MATERIALIZED VIEW IF EXISTS CommentedOnMaterialized CASCADE;
CREATE MATERIALIZED VIEW CommentedOnMaterialized AS
(
    SELECT comments.UserId AS UserId, comments.PostId AS PostId
    FROM comments
);