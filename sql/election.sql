/*************************************************************
* WHEN WORKING WITH PSQL!!!
* First create database Election
*  create database Election;
*
* Then connect to the database
* \connect Election
*
* FINALLY run all the create commands below ;-)
* Like this all the tables will be in Election and not in Postgres
**************************************************************/

--all the attributes in tweet are not null as there are no non available values
-- in the data set for those values. For the ones with a lot of NAs, Retweet and
-- Reply table are generated
create table Tweet(
    --id generated for speed up puposes in the query as integer
    --is smaller than datetime AND handle
    id integer Primary Key,
    -- in the dataset from "2016-01-05T03:36:53" till
    -- "2016-09-28T00:22:34" which needs to be transformed
    -- probably to fit the format '2001-02-16 20:38:40'
    created timestamp not null, -- alias for datetime
    favouriteCount integer not null,
    sourceUrl character varying(50) not null,
    quoteStatus boolean not null,
    truncated boolean not null,
    retweetCount integer not null,
    handle character varying(15) not null
    --If you want to set datetime and handle as primary key, incomment
    -- only the one line below
    -- PRIMARY KEY (created, handle)
    -- AND Comment entry in line 5 : id integer Primary Key,
    -- ID was set as a primary key in order to speed up queries
);

create table Content(
  id integer Primary key,
  idTweet integer references Tweet (id)
);

create table Text(
  id integer Primary key,
  idContent integer references Content (id), --when referenced not nullable!!
  plaintext text not null --may be null but would contain useless entries
);

create table Hashtag(
  id integer Primary key,
  idContent integer references Content (id), --when referenced not nullable!!
  hashtag character varying(20) not null
);

create table Link(
  id integer Primary key,
  idContent integer references Content (id), --when referenced not nullable!!
  link character varying(45) not null -- longest link in the data set is below 45
);

create table TargetHandle(
  id integer Primary key,
  idContent integer references Content (id), --when referenced not nullable!!
  targetHandle character varying(30) not null
);

-- in table Retweet we could simply use datetime (date AND time) alone
-- which is already unique enough, as there are no two entries with the
-- same timestamp. Therefore handle may be deleted from the table
create table Retweet(
  id integer Primary key,
  --  there is no unique constraint matching given keys for referenced table "tweet" when using created
  -- created timestamp references Tweet (id), --when referenced not nullable!!
  -- handle character varying(15) references Tweet (handle), --when referenced not nullable!!
  idTweet integer references Tweet (id),
  originalHandle character varying(45) not null -- 268 possible original authors
  -- and the majority of the tweets do not have a value for original author
);

create table Reply(
  -- created datetime references Tweet (created),
  idTweet integer references Tweet (id),
  replyHandle varchar(15) not null -- only 8 available names and the
  -- majority of the tweets do not have a value for in reply blabla
);
