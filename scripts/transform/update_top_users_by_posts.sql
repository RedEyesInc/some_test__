insert into top_users_by_posts (user_id, posts_cnt, calculated_at)
    select 
        user_id,
        count(id) as posts_cnt,
        NOW() as calculated_at
    from raw_users_by_posts
    group by 
        user_id
