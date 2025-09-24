CREATE TABLE IF NOT EXISTS top_users_by_posts (
  user_id integer NOT NULL,
  posts_cnt integer NULL,
  calculated_at timestamp NULL,
  PRIMARY KEY (user_id)
);
