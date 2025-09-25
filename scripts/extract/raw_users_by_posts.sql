CREATE TABLE IF NOT EXISTS raw_users_by_posts (
  user_id integer NULL,
  id integer NOT NULL,
  title text NULL,
  body text NULL,
  PRIMARY KEY (id)
);
