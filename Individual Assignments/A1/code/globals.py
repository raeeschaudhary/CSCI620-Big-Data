# Provide the db credentials for your machine
db_config = {
    'host': 'localhost',
    'database': 'ubuntu',
    'user': 'postgres',
    'password': 'root',
    'port': '5432'
}
# provide the dataset folder; make sure to include the last slash(es).
data_directory = 'C:\\Users\\Muhammad Raees\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'
# data_directory = 'C:\\Users\\mr2714\\OneDrive - rit.edu\\Python_Projects\\BigData\\data\\'

# this is static; increasing uses more memory but less connections to db; reducing make more connection to db
chunk_size = 5000

# these are the list of files in the db; I use it check table results; to avoid sql injection
input_files = ["Users", "Badges", "Posts", "PostLinks", "Comments", "PostHistory", "Votes", "Tags"]

# insert queries for each table are provided under. 
users_query = """
    INSERT INTO users (Id, AboutMe, AccountId, CreationDate, DisplayName,
    DownVotes, LastAccessDate, Location, Reputation, UpVotes,
    Views, WebsiteUrl) 
    VALUES %s
    """

badges_query = """
    INSERT INTO badges (Id, Class, Date, Name, TagBased, UserId) 
    VALUES %s
    """

posts_query = """
    INSERT INTO posts (Id, AcceptedAnswerId,AnswerCount,Body,
        ClosedDate,CommentCount,CommunityOwnedDate,ContentLicense,
        CreationDate,FavoriteCount,LastActivityDate,LastEditDate,
        LastEditorDisplayName,LastEditorUserId,OwnerDisplayName,
        OwnerUserId, ParentId,PostTypeId, Score,
        Tags, Title, ViewCount) 
    VALUES %s
    """

post_links_query = """
    INSERT INTO postlinks (Id, CreationDate, LinkTypeId, PostId, RelatedPostId)
    VALUES %s
    """

comments_query = """
    INSERT INTO comments (Id, ContentLicense, CreationDate, PostId, Score,
        Text, UserDisplayName, UserId)
    VALUES %s
    """

post_history_query = """
    INSERT INTO posthistory (Id, Comment, ContentLicense, CreationDate, PostHistoryTypeId,
        PostId, RevisionGUID, Text, UserDisplayName, UserId)
    VALUES %s
    """

votes_query = """
    INSERT INTO votes (Id, BountyAmount, CreationDate, PostId, UserId, VoteTypeId)
    VALUES %s
    """

tags_query = """
    INSERT INTO tags (Id, Count, ExcerptPostId, TagName, WikiPostId)
    VALUES %s
    """