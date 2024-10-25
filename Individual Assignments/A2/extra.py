
def process_posts_without_parent():
    # Read id and parentId from posts
    all_ids = "SELECT id FROM posts"
    parent_ids = "SELECT parentId FROM posts WHERE parentId IS NOT NULL"
    
    all_ids_data = exec_get_all(all_ids)
    parent_ids_data = exec_get_all(parent_ids)

    # Convert results to sets
    ids = set(id[0] for id in all_ids_data)
    parent_ids = set(parent_id[0] for parent_id in parent_ids_data)

    # Step 3: Find parentIds that do not exist in ids
    missing_parent_ids = parent_ids - ids

    # Print the number of missing parentIds
    print(f"Missing parentIds count: {len(missing_parent_ids)}")
    # print(missing_parent_ids)
    if missing_parent_ids:
        missing_parent_ids_str = ', '.join(['%s'] * len(missing_parent_ids))
        delete_query = f"DELETE FROM posts WHERE parentId IN ({missing_parent_ids_str})"
        exec_commit(delete_query, tuple(missing_parent_ids))
    print("Adding FK parentid to posts")
    parent_fk = """
    ALTER TABLE posts
    ADD CONSTRAINT fk_parent_id FOREIGN KEY (ParentId) REFERENCES posts (Id) ON DELETE CASCADE;    
    """
    exec_commit(parent_fk)

# Main logic to process posts
def process_posts_without_answer():
    # Read id and parentId from posts
    all_ids = "SELECT id FROM posts"
    answer_ids = "SELECT AcceptedAnswerId FROM posts WHERE AcceptedAnswerId IS NOT NULL"
    
    all_ids_data = exec_get_all(all_ids)
    answer_ids_ids_data = exec_get_all(answer_ids)

    # Convert results to sets
    ids = set(id[0] for id in all_ids_data)
    answer_ids = set(answer_id[0] for answer_id in answer_ids_ids_data)

    # Step 3: Find parentIds that do not exist in ids
    missing_answer_ids = answer_ids - ids

    # Print the number of missing parentIds
    print(f"Missing answerIds count: {len(missing_answer_ids)}")
    # print(missing_parent_ids)
    if missing_answer_ids:
        missing_answer_ids_str = ', '.join(['%s'] * len(missing_answer_ids))
        delete_query = f"DELETE FROM posts WHERE AcceptedAnswerId IN ({missing_answer_ids_str})"
        exec_commit(delete_query, tuple(missing_answer_ids))
    print("Adding FK answerid to posts")
    answer_fk = """
    ALTER TABLE posts
    ADD CONSTRAINT fk_accepted_answer_id FOREIGN KEY (AcceptedAnswerId) REFERENCES posts (Id) ON DELETE CASCADE;
    """
    exec_commit(answer_fk)

# DELETE FROM posttags WHERE PostId not valid
def process_posttags_without_fks():
    # Read id and parentId from posts
    all_ids = "SELECT id FROM posts"
    post_ids = "SELECT PostId FROM posttags"
    
    all_ids_data = exec_get_all(all_ids)
    post_ids_ids_data = exec_get_all(post_ids)

    # Convert results to sets
    ids = set(id[0] for id in all_ids_data)
    post_id_ids = set(post_id[0] for post_id in post_ids_ids_data)

    # Step 3: Find parentIds that do not exist in ids
    missing_post_ids = post_id_ids - ids

    # Print the number of missing parentIds
    print(f"Missing postid count: {len(missing_post_ids)}")

    
    # print(missing_parent_ids)
    if missing_post_ids:
        missing_post_ids_str = ', '.join(['%s'] * len(missing_post_ids))
        delete_query = f"DELETE FROM posttags WHERE PostId IN ({missing_post_ids_str})"
        exec_commit(delete_query, tuple(missing_post_ids))
    
    # Read id and parentId from Tags
    all_ids = "SELECT id FROM tags"
    tag_ids = "SELECT TagId FROM posttags"
    
    all_ids_data = exec_get_all(all_ids)
    tag_ids_ids_data = exec_get_all(tag_ids)

    # Convert results to sets
    ids = set(id[0] for id in all_ids_data)
    tag_id_ids = set(tag_id[0] for tag_id in tag_ids_ids_data)

    # Step 3: Find parentIds that do not exist in ids
    missing_tag_ids = tag_id_ids - ids

    # Print the number of missing parentIds
    print(f"Missing postid count: {len(missing_tag_ids)}")
    # print(missing_parent_ids)
    if missing_tag_ids:
        missing_tag_ids_str = ', '.join(['%s'] * len(missing_tag_ids))
        delete_query = f"DELETE FROM posttags WHERE TagId IN ({missing_tag_ids_str})"
        exec_commit(delete_query, tuple(missing_tag_ids))

    print("Adding FK postId to posttags")
    post_tag_fk = """
    ALTER TABLE posttags 
    ADD CONSTRAINT fk_post_id FOREIGN KEY (PostId) REFERENCES posts (Id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_tag_id FOREIGN KEY (TagId) REFERENCES tags (Id) ON DELETE CASCADE;
    """
    exec_commit(post_tag_fk)


def process_comments_without_posts():
    # Read id and parentId from posts
    all_ids = "SELECT id FROM posts"
    post_ids = "SELECT postId FROM comments"
    
    all_ids_data = exec_get_all(all_ids)
    post_ids_data = exec_get_all(post_ids)

    # Convert results to sets
    ids = set(id[0] for id in all_ids_data)
    post_ids = set(post_id[0] for post_id in post_ids_data)

    # Step 3: Find parentIds that do not exist in ids
    missing_post_ids = post_ids - ids

    # Print the number of missing parentIds
    print(f"Missing postIds count: {len(missing_post_ids)}")
    # print(missing_parent_ids)
    if missing_post_ids:
        missing_post_ids_str = ', '.join(['%s'] * len(missing_post_ids))
        delete_query = f"DELETE FROM comments WHERE postId IN ({missing_post_ids_str})"
        exec_commit(delete_query, tuple(missing_post_ids))
    print("Adding FK postid to posts")
    parent_fk = """
    ALTER TABLE comments
    ADD CONSTRAINT fk_comment_post_id FOREIGN KEY (PostId) REFERENCES posts (Id) ON DELETE CASCADE;    
    """
    exec_commit(parent_fk)