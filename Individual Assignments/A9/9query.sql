-- select count(id) from users
-- Q1
-- SELECT u.id, u.displayname
-- FROM Users u
-- WHERE u.displayname LIKE 'J%' 
-- AND NOT EXISTS (
--     SELECT 1
--     FROM Posts p
--     WHERE p.owneruserid = u.id
--     AND EXTRACT(YEAR FROM p.creationdate) = 2014
-- )

-- Q2
-- WITH tags_2017 AS (
--     SELECT TRIM(BOTH '<>' FROM tag) AS tag
--     FROM Posts
--     JOIN LATERAL unnest(regexp_split_to_array(tags, '<\\|>')) AS tag ON true
--     WHERE EXTRACT(YEAR FROM creationdate) = 2017
--     AND TRIM(BOTH '<>' FROM tag) != ''  -- Exclude empty tags
-- )
-- SELECT tag, COUNT(*) AS tag_count
-- FROM tags_2017
-- GROUP BY tag
-- ORDER BY tag_count DESC;


-- Q3
-- WITH networking_posts AS (
--     SELECT p.id
--     FROM Posts p
--     JOIN LATERAL unnest(regexp_split_to_array(p.tags, '<\\|>')) AS tag ON true
--     WHERE TRIM(BOTH '<>' FROM tag) = 'networking'  -- Filters posts with 'networking' tag
-- ),
-- user_comments AS (
--     SELECT c.userid, COUNT(*) AS comment_count
--     FROM Comments c
--     INNER JOIN networking_posts np ON c.postid = np.id
--     GROUP BY c.userid
-- )
-- SELECT u.displayname, uc.comment_count
-- FROM user_comments uc
-- JOIN Users u ON uc.userid = u.id
-- ORDER BY uc.comment_count DESC;

-- Q4
-- SELECT b.name, COUNT(*) AS badge_count
-- FROM Badges b
-- JOIN Users u ON b.userid = u.id
-- WHERE u.reputation < 100
-- GROUP BY b.name
-- ORDER BY badge_count DESC;
