with 

source as (

    select * from {{ source('olist', 'source_system_order_reviews') }}

),

stg_order_reviews as (

    select
        --There could be more than one order_id per review_id
        review_id,
        --Referential integrity with source_system_orders fails because of cut off time of date.
        --Ex: 0764f8b9a45b0305b75e1bbd614ffaaa dated 2017-09-16 
        order_id,
        --Values between 1 and 5
        review_score,
        review_comment_title,
        review_comment_message,
        TIMESTAMP (review_creation_date) AS review_creation_ts,
        TIMESTAMP (review_answer_timestamp) AS review_answer_ts,
        DATE (INGESTION_DATE) as INGESTION_dt,
        TIMESTAMP (UPDATE_TS) as UPDATE_TS

    from source

)

select * from stg_order_reviews
