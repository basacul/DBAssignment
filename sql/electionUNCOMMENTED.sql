create table Tweet(
    id integer Primary Key,
    created timestamp not null,
    favouriteCount integer not null,
    sourceUrl character varying(50) not null,
    quoteStatus boolean not null,
    truncated boolean not null,
    retweetCount integer not null,
    handle character varying(15) not null
);

create table Content(
  id integer Primary key,
  idTweet integer references Tweet (id)
);

create table Text(
  id integer Primary key,
  idContent integer references Content (id),
  plaintext text not null
);

create table Hashtag(
  id integer Primary key,
  idContent integer references Content (id),
  hashtag character varying(20) not null
);

create table Link(
  id integer Primary key,
  idContent integer references Content (id),
  link character varying(45) not null
);

create table TargetHandle(
  id integer Primary key,
  idContent integer references Content (id),
  targetHandle character varying(30) not null
);


create table Retweet(
  id integer Primary key,
  idTweet integer references Tweet (id),
  originalHandle character varying(45) not null
);

create table Reply(
  idTweet integer references Tweet (id),
  replyHandle varchar(15) not null
);
