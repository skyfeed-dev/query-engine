use anyhow::anyhow;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum_macros::debug_handler;
use base64::{engine::general_purpose, Engine as _};
use chrono::prelude::*;
use http::Method;
use mimalloc::MiMalloc;
use rand::{seq::SliceRandom, thread_rng};
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use std::{net::SocketAddr, path::PathBuf};
use tokio::{sync::RwLock, task, time};
use tower_http::cors::{Any, CorsLayer};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// TODO 1 week ago
static TMP_INITIAL_LAST_POST_ID: &str = "post:3kkyt5v2h";

// TODO 24 hours ago (done!)
static TMP_COUNT_QUERIES_ANCHOR: &str = "post:3klhwaxeu";

static HEADER_NAMESPACE: &str = "bsky";
// static HEADER_NAMESPACE: &str = "atproto";

#[derive(Clone)]
struct ServerConfigInstance {
    pub all_posts: HashMap<String, Post>,
    pub all_posts_by_author: HashMap<String, HashSet<String>>,
    pub all_posts_by_tag: HashMap<String, HashSet<String>>,
    pub last_post_id: String,
    pub tag_variations: HashMap<String, HashMap<String, u128>>,

    pub all_profiles: HashMap<String, Profile>,
}

struct ServerConfigWrapper {
    pub instance_a: RwLock<ServerConfigInstance>,
    pub instance_b: RwLock<ServerConfigInstance>,
    pub pointer_is_a: RwLock<bool>,

    pub count_queries_anchor: RwLock<String>,
}

fn get_surreal_api_url() -> String {
    env::var("SURREAL_URL_SQL").unwrap()
}

fn get_surreal_auth_header() -> String {
    let user = env::var("SURREAL_USER").unwrap();
    let pass = env::var("SURREAL_PASS").unwrap();

    format!(
        "Basic {}",
        general_purpose::URL_SAFE.encode(format!("{}:{}", user, pass))
    )
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().unwrap();
    println!(
        "SurrealDB API URL: {}",
        env::var("SURREAL_URL_SQL").unwrap()
    );

    let is_running_in_feed_mode = env::var("MODE").is_err();

    let server_config_instance = ServerConfigInstance {
        all_posts: HashMap::new(),
        all_posts_by_author: HashMap::new(),
        all_posts_by_tag: HashMap::new(),
        last_post_id: TMP_INITIAL_LAST_POST_ID.to_string(),
        tag_variations: HashMap::new(),
        all_profiles: HashMap::new(),
    };

    let server_config_wrapper = ServerConfigWrapper {
        instance_a: RwLock::new(server_config_instance.clone()),
        instance_b: RwLock::new(server_config_instance),
        pointer_is_a: RwLock::new(true),
        count_queries_anchor: RwLock::new(TMP_COUNT_QUERIES_ANCHOR.to_string()),
    };

    let root_arc = Arc::new(server_config_wrapper);

    let arc = Arc::clone(&root_arc);

    println!("init");

    // ! 168h = 1 week

    if is_running_in_feed_mode {
        println!("running in feed mode");
        {
            run_query(&arc.instance_a, Duration::from_secs(60 * 15))
                .await
                .unwrap();

            let res = run_update_counts_query(TMP_INITIAL_LAST_POST_ID, &arc.instance_a).await;
            if res.is_err() {
                println!("ERROR run_update_counts_query {}", res.unwrap_err());
            }

            println!("cloning state");
            {
                let read = arc.instance_a.read().await;
                *arc.instance_b.write().await = read.clone();
            }
        }

        println!("ready!");

        // ! every 100 seconds
        let new_posts_task_arc = Arc::clone(&arc);
        let _new_posts_task = task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(100));
            let mut counter = 0;
            loop {
                interval.tick().await;
                println!("fetching new posts...");
                let new_pointer_is_a = { !*new_posts_task_arc.pointer_is_a.read().await };

                {
                    let res = run_query(
                        {
                            if new_pointer_is_a {
                                &new_posts_task_arc.instance_a
                            } else {
                                &new_posts_task_arc.instance_b
                            }
                        },
                        Duration::from_secs(30),
                    )
                    .await;

                    if res.is_err() {
                        println!("ERROR run_query {}", res.unwrap_err());
                    }
                    counter += 1;
                    if counter % 5 == 0 {
                        println!("fetching new counts...");
                        let count_queries_anchor =
                            { arc.count_queries_anchor.read().await.clone() };
                        let res = run_update_counts_query(&count_queries_anchor, {
                            if new_pointer_is_a {
                                &new_posts_task_arc.instance_a
                            } else {
                                &new_posts_task_arc.instance_b
                            }
                        })
                        .await;

                        if res.is_err() {
                            println!("ERROR run_update_counts_query {}", res.unwrap_err());
                        }
                    }
                }
                *new_posts_task_arc.pointer_is_a.write().await = new_pointer_is_a;

                println!("finished update loop");
            }
        });


        /*     let cleanup_task_arc = Arc::clone(&arc);
        let _cleanup_task = task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60 * 60 * 24));
            let mut counter = 0;
            loop {
                interval.tick().await;
                if counter > 0 {
                    let res = clean_up_old_posts(&cleanup_task_arc).await;

                    if res.is_err() {
                        println!("ERROR clean_up_old_posts {}", res.unwrap_err());
                    }
                }
                counter += 1;
            }
        }); */
        // every 60 mins
        /*     let like_count_task_arc_2 = Arc::clone(&arc);
        let _like_count_task_2 = task::spawn(async move {
            let period = Duration::from_secs(60 * 60);
            time::sleep(period).await;

            let mut interval = time::interval(period);
            loop {
                interval.tick().await;
                run_update_counts_query("72h", &like_count_task_arc_2)
                    .await
                    .unwrap_or(());
            }
        }); */

        // every 5 mins
        // TODO Implement labels
        /* let labels_task_arc = Arc::clone(&arc);
        let _labels_task = task::spawn(async move {
            let period = Duration::from_secs(60 * 5);

            let mut interval = time::interval(period);
            loop {
                interval.tick().await;
                run_update_labels_query(&labels_task_arc)
                    .await
                    .unwrap_or(());
            }
        }); */
    } else {
        println!("running in list mode");
        run_profiles_query(&arc, Duration::from_secs(60 * 10))
            .await
            .unwrap();

        println!("ready!");

        // ! every 30 minutes
        let update_profiles_task_arc = Arc::clone(&arc);
        let _update_profiles_task = task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60 * 30));
            let mut is_first_run = true;
            loop {
                interval.tick().await;
                if is_first_run {
                    is_first_run = false;
                    continue;
                }

                let res =
                    run_profiles_query(&update_profiles_task_arc, Duration::from_secs(60 * 10))
                        .await;
                if res.is_err() {
                    println!("ERROR run_profiles_query {}", res.unwrap_err());
                }
            }
        });
    }

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);

    let app = Router::new()
        .route("/health", get(health_check))
        .route(
            "/xrpc/me.skyfeed.builder.generateFeedSkeleton",
            post(generate_feed_skeleton_route),
        )
        .route(
            "/xrpc/app.skyfeed.graph.generateListSkeleton",
            post(generate_list_skeleton_route),
        )
        .route(
            "/xrpc/app.skyfeed.feed.getTrendingTags",
            get(get_trending_tags),
        )
        .layer(cors)
        .with_state(arc);

    let addr = SocketAddr::from((
        [0, 0, 0, 0],
        if is_running_in_feed_mode { 4444 } else { 4445 },
    ));
    // tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn health_check() -> &'static str {
    ""
}

async fn get_trending_tags(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<ServerConfigWrapper>>,
) -> impl IntoResponse {
    let minutes = if params.contains_key("minutes") {
        params.get("minutes").unwrap().parse::<i64>().unwrap()
    } else {
        600
    };

    let cutoff = Utc::now()
        .checked_sub_signed(chrono::Duration::minutes(minutes))
        .unwrap();

    let mut tags = vec![];

    let sc = {
        if *state.pointer_is_a.read().await {
            state.instance_a.read()
        } else {
            state.instance_b.read()
        }
    }
    .await;

    // sc.tag_variations

    for tag in sc.all_posts_by_tag.keys() {
        let mut count = 0;
        for p in sc.all_posts_by_tag.get(tag).unwrap() {
            let p = sc.all_posts.get(p).unwrap();
            if p.created_at > cutoff {
                count += 1;
                // if !variations.contains_key() {}
            }
        }
        if count > 2 {
            let mut most_popular_variation = "";
            let mut most_popular_variation_count: u128 = 0;
            let variations = sc.tag_variations.get(tag).unwrap();
            for v in variations.keys() {
                let c = variations.get(v).unwrap();
                if c > &most_popular_variation_count {
                    most_popular_variation_count = c.clone();
                    most_popular_variation = v;
                }
            }

            tags.push(TrendingTag {
                tag: tag.clone(),
                name: most_popular_variation.to_string(),
                count: count,
            });
        }
        // .len() as u128
        /* if len > 100 {
            tags.insert(tag.clone(), len);
        } */
    }

    tags.sort_by(|b, a| a.count.cmp(&b.count));

    if tags.len() > 100 {
        tags.drain(100..);
    }

    (StatusCode::OK, Json(TrendingTagsResponse { tags: tags }))
}

#[debug_handler]
async fn generate_feed_skeleton_route(
    State(state): State<Arc<ServerConfigWrapper>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let res = tokio::time::timeout(
        Duration::from_secs(30),
        generate_feed_skeleton(state, payload.clone()),
    )
    .await;
    if res.is_err() {
        return (
            StatusCode::REQUEST_TIMEOUT,
            Json(FeedBuilderResponse {
                debug: FeedBuilderResponseDebug {
                    time: 0,
                    timing: HashMap::new(),
                    counts: HashMap::new(),
                },
                feed: vec![PostReference {
                    post: format!("Feed Builder timed out"),
                }],
            }),
        );
    }
    let response = res
        .unwrap()
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(FeedBuilderResponse {
                    debug: FeedBuilderResponseDebug {
                        time: 0,
                        timing: HashMap::new(),
                        counts: HashMap::new(),
                    },
                    feed: vec![PostReference {
                        post: format!("Error: {}", e),
                    }],
                }),
            )
        })
        .map(|res| (StatusCode::OK, Json(res)));

    response.unwrap_or_else(|(status, body)| (status, body))
}

#[debug_handler]
async fn generate_list_skeleton_route(
    State(state): State<Arc<ServerConfigWrapper>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let res = tokio::time::timeout(
        Duration::from_secs(20),
        generate_list_skeleton(state, payload.clone()),
    )
    .await;
    if res.is_err() {
        return (
            StatusCode::REQUEST_TIMEOUT,
            Json(ListBuilderResponse {
                debug: FeedBuilderResponseDebug {
                    time: 0,
                    timing: HashMap::new(),
                    counts: HashMap::new(),
                },
                items: vec![],
            }),
        );
    }
    let response = res
        .unwrap()
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ListBuilderResponse {
                    debug: FeedBuilderResponseDebug {
                        time: 0,
                        timing: HashMap::new(),
                        counts: HashMap::new(),
                    },
                    items: vec![ListItem {
                        subject: ListSubject {
                            did: format!("Error: {}", e,),
                            name: "".to_string(),
                            handle: "".to_string(),
                        },
                    }],
                }),
            )
        })
        .map(|res| (StatusCode::OK, Json(res)));

    response.unwrap_or_else(|(status, body)| (status, body))
}

static LISTITEM_QUERY: &str =
    "RETURN array::flatten((SELECT ->listitem.out as dids FROM LIST_ID).dids);";

async fn generate_feed_skeleton(
    state: Arc<ServerConfigWrapper>,
    payload: Value,
) -> anyhow::Result<FeedBuilderResponse> {
    let blocks = payload["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block.as_object().unwrap().clone())
        .collect::<Vec<_>>();

    if blocks.len() > 32 {
        // ! Error Message: Your custom feed has too many blocks! If you are doing a lot of "Single User" inputs, use the "List" input type instead. If you have a lot of RegEx blocks, use only one with pipe|symbols for multiple words. Respond to this message if you need help!
        return Ok(FeedBuilderResponse {
            debug: FeedBuilderResponseDebug {
                time: 0,
                timing: HashMap::new(),
                counts: HashMap::new(),
            },
            feed: vec![PostReference {
                post: "at://did:plc:bq2d7fljrtvzvugb2krsnyl6/app.bsky.feed.post/3k5imcxnuay2s"
                    .to_string(),
            }],
        });
    }

    let mut regex_block_count = 0;

    for block in &blocks {
        let b_type = block["type"].as_str().unwrap();

        if b_type == "regex" {
            regex_block_count = regex_block_count + 1;
            if regex_block_count > 6 {
                // ! Error Message: Your custom feed has too many RegEx blocks! Using a single RegEx block and one additional inverted RegEx block should be enough to achieve the same result. Reply to this message if you need help!

                return Ok(FeedBuilderResponse {
                    debug: FeedBuilderResponseDebug {
                        time: 0,
                        timing: HashMap::new(),
                        counts: HashMap::new(),
                    },
                    feed: vec![PostReference {
                        post:
                            "at://did:plc:bq2d7fljrtvzvugb2krsnyl6/app.bsky.feed.post/3k5roduyxlo2e"
                                .to_string(),
                    }],
                });
            }
        }
    }

    // TODO Optimization: pre-fetch external feeds here

    // let mut posts: Vec<Cow<Post>> = vec![];
    // let mut stash: HashMap<&str, Vec<Cow<Post>>> = HashMap::new();

    let mut posts_tmp: HashMap<i32, Vec<Post>> = HashMap::new();

    let mut pre_block_index = 0;

    for block in &blocks {
        let b_type = block["type"].as_str().unwrap();

        if b_type == "input" {
            let input_type = block["inputType"].as_str().unwrap();
            if input_type == "did" {
                let did = did_to_key(block["did"].as_str().unwrap(), true)?;

                let collection = if block.contains_key("collection") {
                    block["collection"].as_str().unwrap_or("post")
                } else {
                    "post"
                };

                let with_counts = if block.contains_key("withCounts") {
                    block["withCounts"].as_bool().unwrap_or(false)
                } else {
                    false
                };

                let mut posts: Vec<Post> = vec![];

                if collection.starts_with("post") {
                    let new_posts = fetch_user_posts(&did, "posts", with_counts).await?;
                    for post in new_posts {
                        posts.push(post);
                    }
                }
                if collection.contains("reply") {
                    let new_posts = fetch_user_posts(&did, "replies", with_counts).await?;
                    for post in new_posts {
                        posts.push(post);
                    }
                }
                if collection.contains("repost") {
                    let new_posts = fetch_user_posts(&did, "repost", with_counts).await?;
                    for post in new_posts {
                        posts.push(post);
                    }
                }
                if collection.ends_with("like") {
                    let new_posts = fetch_user_posts(&did, "like", with_counts).await?;
                    for post in new_posts {
                        posts.push(post);
                    }
                }
                posts_tmp.insert(pre_block_index, posts);
            } else if input_type == "post" {
                let post_id = at_uri_to_post_id(block["postUri"].as_str().unwrap())?;

                // if !sc.all_posts.contains_key(&post_id) {
                let post = fetch_post(&post_id).await?;

                posts_tmp.insert(pre_block_index, vec![post]);
                //posts.insert(0, Cow::Owned(post));
                // } else {
                //  posts.insert(0, Cow::Borrowed(sc.all_posts.get(&post_id).unwrap()));
                // }
            }
        }

        pre_block_index += 1;
    }

    let mut posts: Vec<&Post> = vec![];
    let mut stash: HashMap<&str, Vec<&Post>> = HashMap::new();

    let filter_types = vec!["remove", "regex"];

    let mut debug = FeedBuilderResponseDebug {
        time: 0,
        timing: HashMap::new(),
        counts: HashMap::new(),
    };
    let sc = {
        if *state.pointer_is_a.read().await {
            state.instance_a.read()
        } else {
            state.instance_b.read()
        }
    }
    .await;

    let query_start = Instant::now();

    let mut block_index = 0;

    for block in &blocks {
        let block_start = Instant::now();

        let b_type = block["type"].as_str().unwrap();

        if b_type == "input" {
            let input_type = block["inputType"].as_str().unwrap();
            if input_type == "firehose" {
                let seconds = if block.contains_key("firehoseSeconds") {
                    block["firehoseSeconds"].as_i64().unwrap_or(86400)
                } else {
                    86400
                };

                let cutoff = Utc::now()
                    .checked_sub_signed(chrono::Duration::seconds(seconds))
                    .unwrap();

                posts.extend(sc.all_posts.values().filter(|p| p.created_at > cutoff));
            } else if input_type == "list" {
                let dids = fetch_list(block["listUri"].as_str().unwrap()).await?;

                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };
                let cutoff = Utc::now()
                    .checked_sub_signed(chrono::Duration::seconds(seconds))
                    .unwrap();

                for did in dids {
                    for id in sc.all_posts_by_author.get(&did).unwrap_or(&HashSet::new()) {
                        let post = sc.all_posts.get(id).unwrap();
                        if post.created_at > cutoff {
                            posts.push(post);
                        }
                    }
                }
            } else if input_type == "tags" {
                let array = block["tags"].as_array().unwrap();

                // ---

                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };

                let cutoff = Utc::now()
                    .checked_sub_signed(chrono::Duration::seconds(seconds))
                    .unwrap();

                for tag_value in array {
                    let tag = tag_value.as_str().unwrap().to_lowercase();
                    for id in sc.all_posts_by_tag.get(&tag).unwrap_or(&HashSet::new()) {
                        let post = sc.all_posts.get(id).unwrap();
                        if post.created_at > cutoff {
                            posts.push(post);
                        }
                    }
                }
            } else if input_type == "feed" {
                if !block.contains_key("feedUri") {
                    return Err(anyhow!("No feed selected in input block"));
                }

                let feed_uri = block["feedUri"].as_str().unwrap();

                let client = Client::new();
                let mut headers = reqwest::header::HeaderMap::new();
                headers.insert("accept", "application/json".parse().unwrap());
                // TODO Configurable or public proxy
                let request_builder = client
                    .get(format!(
                        "https://feed-proxy.skyfeed.me/xrpc/app.bsky.feed.getFeedSkeleton?feed={}",
                        &feed_uri
                    ))
                    .headers(headers)
                    .timeout(Duration::from_secs(5));

                let res = request_builder.send().await?;

                if !res.status().is_success() {
                    return Err(anyhow!("HTTP {} {}", res.status(), res.text().await?));
                }

                let res: Value = res.json().await?;
                let list = res["feed"].as_array().unwrap();

                for post in list {
                    let id = at_uri_to_post_id(post["post"].as_str().unwrap())?;
                    if sc.all_posts.contains_key(&id) {
                        posts.push(sc.all_posts.get(&id).unwrap());
                    }
                }
            } else if input_type == "did" {
                for post in posts_tmp.get(&block_index).unwrap() {
                    posts.push(post);
                }
                /*  let did = did_to_key(block["did"].as_str().unwrap(), true)?;

                let collection = if block.contains_key("collection") {
                    block["collection"].as_str().unwrap_or("post")
                } else {
                    "post"
                };

                let with_counts = if block.contains_key("withCounts") {
                    block["withCounts"].as_bool().unwrap_or(false)
                } else {
                    false
                };

                if collection.starts_with("post") {
                    let new_posts = fetch_user_posts(&did, "posts", with_counts).await?;
                    for post in new_posts {
                        posts.push(Cow::Owned(post));
                    }
                }
                if collection.contains("reply") {
                    let new_posts = fetch_user_posts(&did, "replies", with_counts).await?;
                    for post in new_posts {
                        posts.push(Cow::Owned(post));
                    }
                }
                if collection.contains("repost") {
                    let new_posts = fetch_user_posts(&did, "repost", with_counts).await?;
                    for post in new_posts {
                        posts.push(Cow::Owned(post));
                    }
                }
                if collection.ends_with("like") {
                    let new_posts = fetch_user_posts(&did, "like", with_counts).await?;
                    for post in new_posts {
                        posts.push(Cow::Owned(post));
                    }
                } */
            } else if input_type == "post" {
                for post in posts_tmp.get(&block_index).unwrap() {
                    posts.insert(0, post);
                }
                /*  let post_id = at_uri_to_post_id(block["postUri"].as_str().unwrap())?;

                if !sc.all_posts.contains_key(&post_id) {
                    let post = fetch_post(&post_id).await?;
                    posts.insert(0, Cow::Owned(post));
                } else {
                    posts.insert(0, Cow::Borrowed(sc.all_posts.get(&post_id).unwrap()));
                } */
            }
        } else if filter_types.contains(&b_type) {
            let filter = block;
            let filter_type = filter["type"].as_str().unwrap();
            if filter_type == "remove" {
                let subject = filter["subject"].as_str().unwrap();
                if subject == "item" {
                    let value = if filter.contains_key("value") {
                        filter["value"].as_str().unwrap_or("reply")
                    } else {
                        "reply"
                    };

                    if value == "post" {
                        posts.retain(|p| p.is_reply);
                    } else if value == "reply" {
                        posts.retain(|p| !p.is_reply);
                    } else if value == "repost" {
                        // TODO Implement
                    } else if value == "hellthread" {
                        posts.retain(|p| !p.is_hellthread);
                    } else if value == "not_hellthread" {
                        posts.retain(|p| p.is_hellthread);
                    } else if value == "has_labels" {
                        posts.retain(|p| !p.has_labels);
                    } else if value == "has_no_labels" {
                        posts.retain(|p| p.has_labels);
                    }
                } else if subject == "image_count" {
                    let value = if filter.contains_key("value") {
                        filter["value"].as_str().unwrap_or("0")
                    } else {
                        "0"
                    };

                    if value == "0" {
                        posts.retain(|p| p.image_count != 0);
                    } else if value == "1" {
                        posts.retain(|p| p.image_count != 1);
                    } else if value == "2+" {
                        posts.retain(|p| p.image_count < 2);
                    }
                } else if subject == "reply_count" {
                    let value: u32 = filter["value"].as_i64().unwrap().try_into()?;

                    let operator = if filter.contains_key("operator") {
                        filter["operator"].as_str().unwrap_or("<")
                    } else {
                        "<"
                    };

                    if operator == "<" {
                        posts.retain(|p| p.reply_count >= value);
                    } else if operator == ">" {
                        posts.retain(|p| p.reply_count <= value);
                    } else if operator == "==" {
                        posts.retain(|p| p.reply_count != value);
                    } else if operator == "!=" {
                        posts.retain(|p| p.reply_count == value);
                    }
                } else if subject == "repost_count" {
                    let value: u32 = filter["value"].as_i64().unwrap().try_into()?;

                    let operator = if filter.contains_key("operator") {
                        filter["operator"].as_str().unwrap_or("<")
                    } else {
                        "<"
                    };

                    if operator == "<" {
                        posts.retain(|p| p.repost_count >= value);
                    } else if operator == ">" {
                        posts.retain(|p| p.repost_count <= value);
                    } else if operator == "==" {
                        posts.retain(|p| p.repost_count != value);
                    } else if operator == "!=" {
                        posts.retain(|p| p.repost_count == value);
                    }
                } else if subject == "like_count" {
                    let value: u32 = filter["value"].as_i64().unwrap().try_into()?;

                    let operator = if filter.contains_key("operator") {
                        filter["operator"].as_str().unwrap_or("<")
                    } else {
                        "<"
                    };

                    if operator == "<" {
                        posts.retain(|p| p.like_count >= value);
                    } else if operator == ">" {
                        posts.retain(|p| p.like_count <= value);
                    } else if operator == "==" {
                        posts.retain(|p| p.like_count != value);
                    } else if operator == "!=" {
                        posts.retain(|p| p.like_count == value);
                    }
                } else if subject == "language" {
                    let operator = if filter.contains_key("operator") {
                        filter["operator"].as_str().unwrap_or("!=")
                    } else {
                        "!="
                    };

                    let language = if filter.contains_key("language") {
                        filter["language"].as_str().unwrap_or("en")
                    } else {
                        "en"
                    };

                    if operator == "==" {
                        posts.retain(|p| p.lang != language);
                    } else if operator == "!=" {
                        posts.retain(|p| p.lang == language);
                    }
                } else if subject == "list" {
                    let dids_vec = fetch_list(block["listUri"].as_str().unwrap()).await?;
                    let dids: HashSet<String> = HashSet::from_iter(dids_vec);

                    posts.retain(|p| !dids.contains(&p.author));
                } else if subject == "duplicates" {
                    // TODO Make this more efficient
                    let mut seen: HashSet<String> = HashSet::new();
                    posts.retain(|p| seen.insert(p.id.clone()));
                } else if subject == "embed" {
                    let value = if filter.contains_key("value") {
                        filter["value"].as_str().unwrap_or("none")
                    } else {
                        "none"
                    };

                    if value == "none" {
                        posts.retain(|p| !p.record.is_empty());
                    } else if value == "feed" {
                        posts.retain(|p| !p.record.starts_with("feed"));
                    } else if value == "post" {
                        posts.retain(|p| !p.record.starts_with("post"));
                    }
                }
            } else if filter_type == "regex" {
                let value = filter["value"]
                    .as_str()
                    .unwrap()
                    .replace(r"\b", r"(?-u:\b)")
                    .replace(r"\B", r"(?-u:\b)");

                let case_sensitive = if filter.contains_key("caseSensitive") {
                    filter["caseSensitive"].as_bool().unwrap_or(false)
                } else {
                    false
                };

                let target = if filter.contains_key("target") {
                    filter["target"].as_str().unwrap_or("text")
                } else {
                    "text"
                };

                let regex = if !case_sensitive {
                    format!("(?i){}", value)
                } else {
                    value.to_string()
                };
                let re = Regex::new(&regex)?;

                let invert = if filter.contains_key("invert") {
                    filter["invert"].as_bool().unwrap_or(false)
                } else {
                    false
                };
                /*
                148 377µs  2 478ms
                142 480µs  4 223ms
                173 951µs  2 671ms
                199 763µs  4 835ms
                216 334µs 12 515ms
                296 878µs 11 881ms
                304 391µs 11 895ms
                342 631µs  7 124ms
                688 954µs 28 417ms

                 */
                if posts.len() > 100000 && regex.len() > 1000 {
                    let regex_test_start = Instant::now();

                    let mut limit: i64 = ((posts.len() as f32) * 0.01).round() as i64;
                    let mut counter = 0;

                    for p in sc.all_posts.values() {
                        if re.is_match(&p.text) {
                            counter += 1;
                        }
                        limit -= 1;
                        if limit <= 0 {
                            break;
                        }
                    }
                    let elapsed = regex_test_start.elapsed();
                    let micros = elapsed.as_micros();
                    if micros > 500000 {
                        println!("BLOCKED REGEX [{}µs] {:?}", micros, block);
                        // ! Error Message: One of the RegEx blocks in this custom feed is estimated to need more than 30 seconds of 100% CPU power to run. If this is not the first time you are seeing this message, please optimize the RegEx blocks in your custom feed! Reply to this post if you need help.
                        return Ok(FeedBuilderResponse {
                            debug: FeedBuilderResponseDebug {
                                time: 0,
                                timing: HashMap::new(),
                                counts: HashMap::new(),
                            },
                            feed: vec![PostReference {
                post: "at://did:plc:bq2d7fljrtvzvugb2krsnyl6/app.bsky.feed.post/3kcdx57rxlr23"
                    .to_string(),
            }],
                        });
                    }
                    println!("[{}µs] regex {:?}", micros, block);
                    if counter == 0 {
                        println!("[ignore] just some info text");
                    }
                }

                // TODO test performance with 10k posts (text), if above 500ms CANCEL
                if target == "text" {
                    if invert {
                        posts.retain(|p| !re.is_match(&p.text));
                    } else {
                        posts.retain(|p| re.is_match(&p.text));
                    }
                } else if target == "alt_text" {
                    if invert {
                        posts.retain(|p| !re.is_match(&p.alt_text));
                    } else {
                        posts.retain(|p| re.is_match(&p.alt_text));
                    }
                } else if target == "link" {
                    if invert {
                        posts.retain(|p| !re.is_match(&p.link));
                    } else {
                        posts.retain(|p| re.is_match(&p.link));
                    }
                } else if target == "text|alt_text" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.text) || re.is_match(&p.alt_text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.text) || re.is_match(&p.alt_text));
                    }
                } else if target == "alt_text|link" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.link) || re.is_match(&p.alt_text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.link) || re.is_match(&p.alt_text));
                    }
                } else if target == "text|link" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.link) || re.is_match(&p.text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.link) || re.is_match(&p.text));
                    }
                } else if target == "text|alt_text|link" {
                    if invert {
                        posts.retain(|p| {
                            !(re.is_match(&p.link)
                                || re.is_match(&p.text)
                                || re.is_match(&p.alt_text))
                        });
                    } else {
                        posts.retain(|p| {
                            re.is_match(&p.link) || re.is_match(&p.text) || re.is_match(&p.alt_text)
                        });
                    }
                }
            }
        } else if b_type == "sort" {
            let sort_type = block["sortType"].as_str().unwrap();

            let direction = if block.contains_key("sortDirection") {
                block["sortDirection"].as_str().unwrap_or("desc")
            } else {
                "desc"
            };

            if sort_type == "created_at" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                } else {
                    posts.sort_by(|b, a| a.created_at.cmp(&b.created_at));
                }
            } else if sort_type == "hn" {
                let gravity_str = if block.contains_key("gravity") {
                    block["gravity"].as_str().unwrap_or("1.8")
                } else {
                    "1.8"
                };

                let gravity = gravity_str.parse::<f64>()?;

                let mut tuples: Vec<(f64, &Post)> = posts
                    .into_iter()
                    .map(|p| (p.calculate_score(gravity), p))
                    .collect();

                if direction == "asc" {
                    tuples.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                } else {
                    tuples.sort_by(|b, a| a.0.partial_cmp(&b.0).unwrap());
                }
                posts = tuples.into_iter().map(|t| t.1).collect();
            } else if sort_type == "likes" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.like_count.cmp(&b.like_count));
                } else {
                    posts.sort_by(|b, a| a.like_count.cmp(&b.like_count));
                }
            } else if sort_type == "repost_count" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.repost_count.cmp(&b.repost_count));
                } else {
                    posts.sort_by(|b, a| a.repost_count.cmp(&b.repost_count));
                }
            } else if sort_type == "reply_count" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.reply_count.cmp(&b.reply_count));
                } else {
                    posts.sort_by(|b, a| a.reply_count.cmp(&b.reply_count));
                }
            } else if sort_type == "random" {
                // TODO Maybe there's a faster rng
                posts.shuffle(&mut thread_rng());
            }
        } else if b_type == "stash" {
            let action = if block.contains_key("action") {
                block["action"].as_str().unwrap_or("stash")
            } else {
                "stash"
            };
            let key = block["key"].as_str().unwrap();

            if action == "stash" {
                stash.insert(key, posts);
                posts = vec![];
            } else if action == "pop" {
                if !stash.contains_key(key) {
                    return Err(anyhow!(
                        "Stash pop failed because stash with that key does not exist"
                    ));
                }
                // TODO Maybe a clone is not needed?
                posts.extend(stash.get(key).unwrap().clone());
            }
        } else if b_type == "limit" {
            let count = if block.contains_key("count") {
                block["count"].as_i64().unwrap_or(100)
            } else {
                100
            } as usize;

            if posts.len() > count {
                posts.drain((count)..);
            }
        } else if b_type == "replace" {
            let target = if block.contains_key("with") {
                block["with"].as_str().unwrap_or("parent")
            } else {
                "parent"
            };
            let keep_unsuitable_posts = if block.contains_key("keepItemsWithMissingTarget") {
                block["keepItemsWithMissingTarget"]
                    .as_bool()
                    .unwrap_or(true)
            } else {
                true
            };

            let mut new_posts: Vec<&Post> = vec![];

            if target == "parent" {
                for post in posts {
                    if post.parent.is_empty() {
                        if keep_unsuitable_posts {
                            new_posts.push(post);
                        }
                    } else {
                        let replacement = sc.all_posts.get(&post.parent);
                        if replacement.is_some() {
                            new_posts.push(replacement.unwrap());
                        }
                    }
                }
            } else if target == "root" {
                for post in posts {
                    if post.root.is_empty() {
                        if keep_unsuitable_posts {
                            new_posts.push(post);
                        }
                    } else {
                        let replacement = sc.all_posts.get(&post.root);
                        if replacement.is_some() {
                            new_posts.push(replacement.unwrap());
                        }
                    }
                }
            } else if target == "record" {
                for post in posts {
                    if post.record.is_empty() {
                        if keep_unsuitable_posts {
                            new_posts.push(post);
                        }
                    } else {
                        let replacement = sc.all_posts.get(&post.record);
                        if replacement.is_some() {
                            new_posts.push(replacement.unwrap());
                        }
                    }
                }
            }
            posts = new_posts;
        }

        let elapsed = block_start.elapsed();
        let millis = elapsed.as_millis();
        println!("[{}ms] {:?}", millis, block);
        if block.contains_key("id") {
            debug
                .timing
                .insert(block["id"].as_str().unwrap().to_string(), millis);

            debug.counts.insert(
                block["id"].as_str().unwrap().to_string(),
                posts.len() as u128,
            );
        }
        block_index += 1;
    }

    if posts.len() > 1000 {
        posts.drain(1000..posts.len());
    }

    debug.time = query_start.elapsed().as_millis();

    if debug.time > 10000 {
        println!("slow query {}ms {:?}", debug.time, blocks);
    }

    let res = FeedBuilderResponse {
        debug,
        feed: posts
            .iter()
            .map(|p| {
                let post_uri = convert_post_id_to_uri(&p.id);
                PostReference { post: post_uri }
            })
            .collect(),
    };

    Ok(res)
}

async fn generate_list_skeleton(
    state: Arc<ServerConfigWrapper>,
    payload: Value,
) -> anyhow::Result<ListBuilderResponse> {
    let blocks = payload["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block.as_object().unwrap().clone())
        .collect::<Vec<_>>();

    let mut lists_tmp: HashMap<i32, Vec<String>> = HashMap::new();

    let mut pre_block_index = 0;

    for block in &blocks {
        let b_type = block["type"].as_str().unwrap();

        if b_type == "input" {
            let input_type = block["inputType"].as_str().unwrap();
            if input_type == "list" {
                let dids = fetch_list(block["listUri"].as_str().unwrap()).await?;
                lists_tmp.insert(pre_block_index, dids);
            }
        }

        pre_block_index += 1;
    }

    let mut profiles: Vec<&Profile> = vec![];
    // let mut stash: HashMap<&str, Vec<&Profile>> = HashMap::new();
    let filter_types = vec!["remove", "regex"];

    let mut debug = FeedBuilderResponseDebug {
        time: 0,
        timing: HashMap::new(),
        counts: HashMap::new(),
    };
    let sc = {
        if *state.pointer_is_a.read().await {
            state.instance_a.read()
        } else {
            state.instance_b.read()
        }
    }
    .await;

    let query_start = Instant::now();

    let mut block_index = 0;

    for block in &blocks {
        let block_start = Instant::now();

        let b_type = block["type"].as_str().unwrap();

        if b_type == "input" {
            let input_type = block["inputType"].as_str().unwrap();
            if input_type == "network" {
                profiles.extend(sc.all_profiles.values());
            } else if input_type == "list" {
                for did in lists_tmp.get(&block_index).unwrap() {
                    let p = sc.all_profiles.get(did);
                    if p.is_some() {
                        profiles.push(p.unwrap());
                    }
                }
            }
        } else if filter_types.contains(&b_type) {
            let filter = block;
            let filter_type = filter["type"].as_str().unwrap();
            if filter_type == "remove" {
                let subject = filter["subject"].as_str().unwrap();
                if subject == "profile" {
                    let value = if filter.contains_key("value") {
                        filter["value"].as_str().unwrap_or("has_avatar")
                    } else {
                        "has_avatar"
                    };

                    if value == "has_avatar" {
                        profiles.retain(|p| !p.has_avatar);
                    } else if value == "has_no_avatar" {
                        profiles.retain(|p| p.has_avatar);
                    } else if value == "has_banner" {
                        profiles.retain(|p| !p.has_banner);
                    } else if value == "has_no_banner" {
                        profiles.retain(|p| p.has_banner);
                    }
                } else if subject == "list" {
                    let dids_vec = fetch_list(block["listUri"].as_str().unwrap()).await?;
                    let dids: HashSet<String> = HashSet::from_iter(dids_vec);

                    profiles.retain(|p| !dids.contains(&p.id));
                } else if subject == "duplicates" {
                    // TODO Make this more efficient
                    let mut seen: HashSet<String> = HashSet::new();
                    profiles.retain(|p| seen.insert(p.id.clone()));
                }
            } else if filter_type == "regex" {
                let value = filter["value"]
                    .as_str()
                    .unwrap()
                    .replace(r"\b", r"(?-u:\b)")
                    .replace(r"\B", r"(?-u:\b)");

                let case_sensitive = if filter.contains_key("caseSensitive") {
                    filter["caseSensitive"].as_bool().unwrap_or(false)
                } else {
                    false
                };

                let target = if filter.contains_key("target") {
                    filter["target"].as_str().unwrap_or("name")
                } else {
                    "name"
                };

                let regex = if !case_sensitive {
                    format!("(?i){}", value)
                } else {
                    value.to_string()
                };
                let re = Regex::new(&regex)?;

                let invert = if filter.contains_key("invert") {
                    filter["invert"].as_bool().unwrap_or(false)
                } else {
                    false
                };

                if target == "name" {
                    if invert {
                        profiles.retain(|p| !re.is_match(&p.name));
                    } else {
                        profiles.retain(|p| re.is_match(&p.name));
                    }
                } else if target == "handle" {
                    if invert {
                        profiles.retain(|p| !re.is_match(&p.handle));
                    } else {
                        profiles.retain(|p| re.is_match(&p.handle));
                    }
                } else if target == "description" {
                    if invert {
                        profiles.retain(|p| !re.is_match(&p.description));
                    } else {
                        profiles.retain(|p| re.is_match(&p.description));
                    }
                } else if target == "name|handle" {
                    if invert {
                        profiles.retain(|p| !(re.is_match(&p.name) || re.is_match(&p.handle)));
                    } else {
                        profiles.retain(|p| re.is_match(&p.name) || re.is_match(&p.handle));
                    }
                } else if target == "handle|description" {
                    if invert {
                        profiles
                            .retain(|p| !(re.is_match(&p.handle) || re.is_match(&p.description)));
                    } else {
                        profiles.retain(|p| re.is_match(&p.handle) || re.is_match(&p.description));
                    }
                } else if target == "name|description" {
                    if invert {
                        profiles.retain(|p| !(re.is_match(&p.name) || re.is_match(&p.description)));
                    } else {
                        profiles.retain(|p| re.is_match(&p.name) || re.is_match(&p.description));
                    }
                } else if target == "name|handle|description" {
                    if invert {
                        profiles.retain(|p| {
                            !(re.is_match(&p.name)
                                || re.is_match(&p.handle)
                                || re.is_match(&p.description))
                        });
                    } else {
                        profiles.retain(|p| {
                            re.is_match(&p.name)
                                || re.is_match(&p.handle)
                                || re.is_match(&p.description)
                        });
                    }
                }
            }
        } else if b_type == "sort" {
            // TODO Implement
            /*    let sort_type = block["sortType"].as_str().unwrap();

            let direction = if block.contains_key("sortDirection") {
                block["sortDirection"].as_str().unwrap_or("desc")
            } else {
                "desc"
            };

            if sort_type == "created_at" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                } else {
                    posts.sort_by(|b, a| a.created_at.cmp(&b.created_at));
                }
            } else if sort_type == "hn" {
                let gravity_str = if block.contains_key("gravity") {
                    block["gravity"].as_str().unwrap_or("1.8")
                } else {
                    "1.8"
                };

                let gravity = gravity_str.parse::<f64>()?;

                let mut tuples: Vec<(f64, &Post)> = posts
                    .into_iter()
                    .map(|p| (p.calculate_score(gravity), p))
                    .collect();

                if direction == "asc" {
                    tuples.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                } else {
                    tuples.sort_by(|b, a| a.0.partial_cmp(&b.0).unwrap());
                }
                posts = tuples.into_iter().map(|t| t.1).collect();
            } else if sort_type == "likes" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.like_count.cmp(&b.like_count));
                } else {
                    posts.sort_by(|b, a| a.like_count.cmp(&b.like_count));
                }
            } else if sort_type == "repost_count" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.repost_count.cmp(&b.repost_count));
                } else {
                    posts.sort_by(|b, a| a.repost_count.cmp(&b.repost_count));
                }
            } else if sort_type == "reply_count" {
                if direction == "asc" {
                    posts.sort_by(|a, b| a.reply_count.cmp(&b.reply_count));
                } else {
                    posts.sort_by(|b, a| a.reply_count.cmp(&b.reply_count));
                }
            } else if sort_type == "random" {
                // TODO Maybe there's a faster rng
                posts.shuffle(&mut thread_rng());
            } */
            /*       } else if b_type == "stash" {
            let action = if block.contains_key("action") {
                block["action"].as_str().unwrap_or("stash")
            } else {
                "stash"
            };
            let key = block["key"].as_str().unwrap();

            if action == "stash" {
                stash.insert(key, posts);
                posts = vec![];
            } else if action == "pop" {
                if !stash.contains_key(key) {
                    return Err(anyhow!(
                        "Stash pop failed because stash with that key does not exist"
                    ));
                }
                posts.extend(stash.get(key).unwrap().clone());
            } */
        } else if b_type == "limit" {
            let count = if block.contains_key("count") {
                block["count"].as_i64().unwrap_or(100)
            } else {
                100
            } as usize;

            if profiles.len() > count {
                profiles.drain((count)..);
            }
        }

        let elapsed = block_start.elapsed();
        let millis = elapsed.as_millis();
        println!("[{}ms] {:?}", millis, block);
        if block.contains_key("id") {
            debug
                .timing
                .insert(block["id"].as_str().unwrap().to_string(), millis);

            debug.counts.insert(
                block["id"].as_str().unwrap().to_string(),
                profiles.len() as u128,
            );
        }
        block_index += 1;
    }

    debug.time = query_start.elapsed().as_millis();

    let res = ListBuilderResponse {
        debug,
        items: profiles
            .iter()
            .map(|p| ListItem {
                subject: ListSubject {
                    did: unsafe_key_to_did(&p.id).unwrap(),
                    name: p.name.clone(),
                    handle: p.handle.clone(),
                },
            })
            .collect(),
    };

    Ok(res)
}

#[derive(Serialize)]
struct TrendingTagsResponse {
    tags: Vec<TrendingTag>,
}
#[derive(Serialize)]
struct TrendingTag {
    tag: String,
    name: String,
    count: u128,
}

#[derive(Serialize)]
struct FeedBuilderResponse {
    debug: FeedBuilderResponseDebug,
    feed: Vec<PostReference>,
}

#[derive(Serialize)]
struct PostReference {
    post: String,
}

#[derive(Serialize)]
struct FeedBuilderResponseDebug {
    time: u128,
    timing: HashMap<String, u128>,
    counts: HashMap<String, u128>,
}

#[derive(Serialize)]
struct ListBuilderResponse {
    debug: FeedBuilderResponseDebug,
    items: Vec<ListItem>,
    // TODO dids?
}

#[derive(Serialize)]
struct ListItem {
    subject: ListSubject,
}

#[derive(Serialize)]
struct ListSubject {
    did: String,
    name: String,
    handle: String,
}

static FOLLOWING_QUERY: &str =
    "RETURN array::flatten((SELECT ->follow.out AS dids FROM USER_DID).dids);";
static FOLLOWING_FOLLOWING_QUERY: &str =
    "RETURN array::flatten((SELECT ->follow.out->follow.out AS dids FROM USER_DID).dids);";
static FOLLOWERS_QUERY: &str =
    "RETURN array::flatten((SELECT <-follow.in AS dids FROM USER_DID).dids);";
static MUTUALS_QUERY: &str = "RETURN array::intersect(array::flatten((SELECT ->follow.out AS dids FROM USER_DID).dids), array::flatten((SELECT <-follow.in AS dids FROM USER_DID).dids));";
// static MUTUALS_QUERY: &str = "LET $res = (select ->follow.out as following, <-follow.in as follows from USER_DID);RETURN array::intersect($res.follows, $res.following);";

async fn fetch_list(list_uri: &str) -> anyhow::Result<Vec<String>> {
    let query = if list_uri.starts_with("list://") {
        let u: Vec<&str> = list_uri.split('/').collect();
        let u_hostname = u.get(2).unwrap().to_string();
        let did = did_to_key(&u_hostname, true).unwrap();
        let u_type = u.get(3).unwrap().to_string();

        if u_type == "following" {
            Ok(FOLLOWING_QUERY.replace("USER_DID", &did))
        } else if u_type == "following_following" {
            Ok(FOLLOWING_FOLLOWING_QUERY.replace("USER_DID", &did))
        } else if u_type == "mutuals" {
            Ok(MUTUALS_QUERY.replace("USER_DID", &did))
        } else if u_type == "followers" {
            Ok(FOLLOWERS_QUERY.replace("USER_DID", &did))
        } else {
            Err(anyhow!("Invalid list URI {}", list_uri))
        }
    } else {
        let list_id = at_uri_to_post_id(list_uri)?;
        Ok(LISTITEM_QUERY.replace("LIST_ID", &list_id))
    }?;

    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse()?);
    headers.insert("NS", HEADER_NAMESPACE.parse()?);
    headers.insert("DB", "bsky".parse()?);
    headers.insert("Authorization", get_surreal_auth_header().parse()?);

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(query)
        .timeout(Duration::from_secs(5));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    // println!("LISTRES {:?}", list);

    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    Ok(list2
        .iter()
        .map(|did| did.as_str().unwrap().to_string())
        .collect::<Vec<String>>())
}

async fn run_query(
    sc_mutex: &RwLock<ServerConfigInstance>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let last_post_id = { sc_mutex.read().await.last_post_id.clone() };
    println!("run_query {}", last_post_id);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(format!(
            "SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent FROM {}..;",
            last_post_id,
        ))
        .timeout(timeout);

    let res = request_builder.send().await?;

    println!("run_query_2 {}", last_post_id);

    let list: Vec<Value> = res.json().await?;

    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    {
        println!("run_query_2.5 {}", last_post_id);
        let mut sc = sc_mutex.write().await;
        println!("run_query_3 {}", last_post_id);

        if list2.len() > 100 {
            let id = list2.get(list2.len() - 100).unwrap().as_object().unwrap()["id"]
                .as_str()
                .unwrap();
            sc.last_post_id = id.to_string();
        }

        println!("run_query fetched {} new posts", list2.len());

        for p in list2 {
            let post = p.as_object().unwrap();
            let id = post["id"].as_str().unwrap().to_string();
            if sc.all_posts.contains_key(&id) {
                continue;
            }
            let new_post_res = process_post(post);
            if new_post_res.is_err() {
                println!("ERROR could not process post {}", id);
                continue;
            }
            sc.all_posts.insert(id.clone(), new_post_res?);

            if post.contains_key("tags") && !post["tags"].is_null() {
                let array = post["tags"].as_array().unwrap();

                for tag_value in array {
                    // TODO remove spaces/underscores/hyphens
                    let tag_raw = tag_value.as_str().unwrap().to_string();
                    let tag = tag_raw.to_lowercase();
                    if !sc.tag_variations.contains_key(&tag) {
                        sc.tag_variations.insert(tag.clone(), HashMap::new());
                    }

                    let map = sc.tag_variations.get_mut(&tag).unwrap();

                    if let Some(count) = map.get(&tag_raw) {
                        map.insert(tag_raw, count + 1);
                    } else {
                        map.insert(tag_raw, 1);
                    };
                    /*
                    if map.contains_key(&tag_raw){
                       map.get_mut(&tag_raw).unwrap();
                       m = *m + 1;
                    }else{
                       map.insert(tag_raw.clone(), 1);
                    } */

                    if let Some(tag_set) = sc.all_posts_by_tag.get_mut(&tag) {
                        tag_set.insert(id.clone());
                    } else {
                        let mut tag_set = HashSet::new();
                        tag_set.insert(id.clone());
                        sc.all_posts_by_tag.insert(tag.clone(), tag_set);
                    }
                }
            }

            let author = post["author"].as_str().unwrap().to_string();
            if let Some(author_set) = sc.all_posts_by_author.get_mut(&author) {
                author_set.insert(id);
            } else {
                let mut author_set = HashSet::new();
                author_set.insert(id);
                sc.all_posts_by_author.insert(author.clone(), author_set);
            }
        }

        println!("run_query done {}", last_post_id);
        Ok(())
    }
}

async fn run_profiles_query(
    sc_mutex: &RwLock<ServerConfigInstance>,
    timeout: Duration,
) -> anyhow::Result<()> {
    println!("run_profiles_query");
    let client = Client::new();

    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body("SELECT * FROM did;")
        .timeout(timeout);

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    println!("run_profiles_query_2");

    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    {
        println!("run_profiles_query_2.5 {}", list2.len());
        let mut sc = sc_mutex.write().await;
        println!("run_profiles_query_3");

        for p in list2 {
            let profile = p.as_object().unwrap();
            let id = profile["id"].as_str().unwrap().to_string();

            let new_profile_res = process_profile(profile);
            if new_profile_res.is_err() {
                println!("ERROR could not process profile {}", id);
                continue;
            }
            sc.all_profiles.insert(id.clone(), new_profile_res?);
        }

        println!("run_profiles_query done");
        Ok(())
    }
}

fn process_post(post: &serde_json::Map<String, Value>) -> anyhow::Result<Post> {
    let id_res = post["id"].as_str();
    if id_res.is_none() {
        return Err(anyhow!("Failed to process_post"));
    }
    let id = id_res.unwrap().to_string();

    let author = post["author"].as_str().unwrap().to_string();
    /*   let langs: Vec<String> = if post.contains_key("langs") && !post["langs"].is_null() {
        post["langs"]
            .as_array()
            .unwrap()
            .into_iter()
            .map(|l| l.as_str().unwrap().to_string())
            .collect()
    } else {
        vec![]
    }; */
    let lang: String = if post.contains_key("langs") && !post["langs"].is_null() {
        let array = post["langs"].as_array().unwrap().first();

        if array.is_none() {
            "en".to_string()
        } else {
            array.unwrap().as_str().unwrap().to_string()
        }
    } else {
        "en".to_string()
    };

    let image_count: u32;

    let alt_text: String = if post.contains_key("images") && !post["images"].is_null() {
        let images = post["images"].as_array().unwrap();
        image_count = images.len() as u32;
        images
            .iter()
            .map(|i| i["alt"].as_str().unwrap())
            .collect::<Vec<&str>>()
            .join("|||")
    } else {
        image_count = 0;
        "".to_string()
    };

    let link: String = if post.contains_key("links") && !post["links"].is_null() {
        let links = post["links"].as_array().unwrap();

        links
            .iter()
            .map(|i| i.as_str().unwrap())
            .collect::<Vec<&str>>()
            .join("|||")
    } else {
        "".to_string()
    };
    let record: String = if post.contains_key("record") && !post["record"].is_null() {
        post["record"].as_str().unwrap().to_string()
    } else {
        "".to_string()
    };
    let root: String = if post.contains_key("root") && !post["root"].is_null() {
        post["root"].as_str().unwrap().to_string()
    } else {
        "".to_string()
    };
    let parent: String = if post.contains_key("parent") && !post["parent"].is_null() {
        post["parent"].as_str().unwrap().to_string()
    } else {
        "".to_string()
    };

    let new_post = Post {
        id: id.clone(),
        text: post["text"].as_str().unwrap().to_string(),
        record,
        alt_text,
        link,
        author: author.clone(),
        lang,
        created_at: DateTime::parse_from_rfc3339(post["createdAt"].as_str().unwrap())
            .unwrap_or(DateTime::default())
            .into(),
        image_count,
        is_reply: !post["root"].is_null(),
        is_hellthread: !post["root"].is_null()
            && post["root"].as_str().unwrap() == "post:3juzlwllznd24_plc_wgaezxqi2spqm3mhrb5xvkzi",
        reply_count: if post.contains_key("replyCount") {
            post["replyCount"].as_i64().unwrap_or(0).try_into()?
        } else {
            0
        },
        repost_count: if post.contains_key("repostCount") {
            post["repostCount"].as_i64().unwrap_or(0).try_into()?
        } else {
            0
        },
        like_count: if post.contains_key("likeCount") {
            post["likeCount"].as_i64().unwrap_or(0).try_into()?
        } else {
            0
        },
        has_labels: !post["labels"].is_null(),
        root,
        parent,
    };

    Ok(new_post)
}

fn process_profile(profile: &serde_json::Map<String, Value>) -> anyhow::Result<Profile> {
    let id = profile["id"].as_str().unwrap().to_string();

    let name: String = if profile.contains_key("displayName") && !profile["displayName"].is_null() {
        profile["displayName"].as_str().unwrap().to_string()
    } else {
        "".to_string()
    };

    let handle: String = if profile.contains_key("handle") && !profile["handle"].is_null() {
        profile["handle"].as_str().unwrap().to_string()
    } else {
        "".to_string()
    };

    let description: String =
        if profile.contains_key("description") && !profile["description"].is_null() {
            profile["description"].as_str().unwrap().to_string()
        } else {
            "".to_string()
        };

    let new_profile = Profile {
        id,
        name,
        handle,
        description,
        has_avatar: profile.contains_key("avatar"),
        has_banner: profile.contains_key("banner"),
    };

    Ok(new_profile)
}

// static SINGLE_POST_QUERY: &str = "SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,count(<-like) as likeCount,count(<-repost) as repostCount,count(<-replyto) as replyCount FROM POSTID;";

static SINGLE_POST_QUERY: &str =
    "SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent FROM POSTID;";

async fn fetch_post(id: &str) -> anyhow::Result<Post> {
    println!("fetch_post {}", id);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(SINGLE_POST_QUERY.replace("POSTID", id))
        .timeout(Duration::from_secs(3));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;
    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    for p in list2 {
        return Ok(process_post(p.as_object().unwrap())?);
    }
    Err(anyhow!("Could not fetch post {}", id))
}

// TODO Maybe remove counts

// static USER_POSTS_QUERY_TEMPLATE: &str ="LET $posts = (SELECT ->COLLECTION_TYPE.out as ids FROM USER_ID).ids; SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,count(<-like) as likeCount,count(<-repost) as repostCount,count(<-replyto) as replyCount FROM $posts;";
// static USER_LIKED_POSTS_QUERY_TEMPLATE: &str ="LET $posts = (SELECT ->like.out as ids FROM USER_ID).ids; SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,count(<-like) as likeCount,count(<-repost) as repostCount,count(<-replyto) as replyCount FROM $posts WHERE meta::tb(id) == 'post';";

static USER_POSTS_QUERY_TEMPLATE_NO_COUNTS: &str ="LET $posts = array::flatten((SELECT ->COLLECTION_TYPE.out as ids FROM USER_ID).ids); SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent FROM $posts;";
static USER_POSTS_QUERY_TEMPLATE_WITH_COUNTS: &str ="LET $posts = array::flatten((SELECT ->COLLECTION_TYPE.out as ids FROM USER_ID).ids); SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,array::first((SELECT c FROM type::thing('like_count_view', [$parent.id])).c) as likeCount,array::first((SELECT c FROM type::thing('reply_count_view', [$parent.id])).c) as replyCount,array::first((SELECT c FROM type::thing('repost_count_view', [$parent.id])).c) as repostCount FROM $posts;";
static USER_LIKED_POSTS_QUERY_TEMPLATE: &str ="LET $posts = array::flatten((SELECT ->like.out as ids FROM USER_ID).ids); SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent FROM $posts WHERE meta::tb(id) == 'post';";

async fn fetch_user_posts(
    did: &str,
    collection: &str,
    with_counts: bool,
) -> anyhow::Result<Vec<Post>> {
    println!("fetch_user_posts {} {}", did, collection);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(if collection == "like" {
            USER_LIKED_POSTS_QUERY_TEMPLATE.replace("USER_ID", did)
        } else {
            if with_counts {
                USER_POSTS_QUERY_TEMPLATE_WITH_COUNTS
                    .replace("USER_ID", did)
                    .replace("COLLECTION_TYPE", collection)
            } else {
                USER_POSTS_QUERY_TEMPLATE_NO_COUNTS
                    .replace("USER_ID", did)
                    .replace("COLLECTION_TYPE", collection)
            }
        })
        .timeout(Duration::from_secs(20));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;
    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    let mut posts: Vec<Post> = vec![];

    for p in list2 {
        posts.push(process_post(p.as_object().unwrap())?);
    }
    Ok(posts)
}

/* static REPLY_COUNT_QUERY: &str = "SELECT replyCount as c,subject as i from reply_count_view WHERE subject.createdAt > (time::now() - VAR_DURATION);";
static REPOST_COUNT_QUERY: &str = "SELECT repostCount as c,subject as i from repost_count_view WHERE subject.createdAt > (time::now() - VAR_DURATION);";
static LIKE_COUNT_QUERY: &str = "SELECT likeCount as c,subject as i from like_count_view WHERE subject.createdAt > (time::now() - VAR_DURATION);"; */

static REPLY_COUNT_QUERY: &str = "SELECT * from reply_count_view:[VAR_ANCHOR]..;";
static REPOST_COUNT_QUERY: &str = "SELECT * from repost_count_view:[VAR_ANCHOR]..;";
static LIKE_COUNT_QUERY: &str = "SELECT * from like_count_view:[VAR_ANCHOR]..;";

async fn run_update_counts_query(
    anchor: &str,
    sc_mutex: &RwLock<ServerConfigInstance>,
) -> anyhow::Result<()> {
    println!("run_update_counts_query");

    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(
            vec![REPLY_COUNT_QUERY, REPOST_COUNT_QUERY, LIKE_COUNT_QUERY]
                .join("\n")
                .replace("VAR_ANCHOR", anchor),
        )
        .timeout(Duration::from_secs(60 * 10));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    let reply_counts = list.get(0).unwrap()["result"].as_array().unwrap();
    let repost_counts = list.get(1).unwrap()["result"].as_array().unwrap();
    let like_counts = list.get(2).unwrap()["result"].as_array().unwrap();

    println!("run_update_counts_query2");

    let mut sc = sc_mutex.write().await;
    println!("run_update_counts_query3");

    for post in reply_counts {
        let id = extract_count_id(post);
        let reply_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().reply_count = reply_count.try_into()?;
        }
    }

    for post in repost_counts {
        let id = extract_count_id(post);
        let repost_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().repost_count = repost_count.try_into()?;
        }
    }

    for post in like_counts {
        let id = extract_count_id(post);
        let like_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().like_count = like_count.try_into()?;
        } else {
            // TODO println!("MISSING POST {}", id);
        }
    }
    println!("run_update_counts_query done");
    Ok(())
}
/*
// TODO Test if this works locally
async fn clean_up_old_posts(sc_mutex: &RwLock<ServerConfig>) -> anyhow::Result<()> {
    println!("clean_up_old_posts1");
    let mut sc = sc_mutex.write().await;
    println!("clean_up_old_posts2");

    let start = Instant::now();

    let cutoff = Utc::now()
        .checked_sub_signed(chrono::Duration::days(7))
        .unwrap();

    println!("clean_up_old_posts3");

    let ids: Vec<String> = sc
        .all_posts
        .values()
        .filter(|p| p.created_at < cutoff)
        .map(|p| p.id.clone())
        .collect();

    println!("clean_up_old_posts4");

    for id in ids.clone() {
        for set in sc.all_posts_by_author.values_mut() {
            set.remove(&id);
        }
    }
    println!("clean_up_old_posts5");
    for id in ids.clone() {
        for set in sc.all_posts_by_tag.values_mut() {
            set.remove(&id);
        }
    }

    println!("clean_up_old_posts6");

    for id in ids {
        sc.all_posts.remove(&id);
    }

    println!("clean_up_old_posts_done {}ms", start.elapsed().as_millis());

    // posts.extend(sc.all_posts.values().filter(|p| p.created_at > cutoff));

    Ok(())
} */

fn extract_count_id(val: &Value) -> &str {
    return val["id"]
        .as_str()
        .unwrap()
        .split(":[")
        .last()
        .unwrap()
        .strip_suffix("]")
        .unwrap();
}

/* static LABELS_QUERY: &str = "SELECT in FROM post_labels;";

async fn run_update_labels_query(mutex: &RwLock<ServerConfig>) -> anyhow::Result<()> {
    println!("run_update_labels_query");
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(LABELS_QUERY)
        .timeout(Duration::from_secs(60 * 1));

    let res = request_builder.send().await?;
    let list: Vec<Value> = res.json().await?;
    let posts = list.get(0).unwrap()["result"].as_array().unwrap();

    let mut sc = mutex.write().await;

    for post in posts {
        let id = post["in"].as_str().unwrap();

        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().has_labels = true;
        }
    }

    println!("run_update_labels_query done");
    Ok(())
} */

lazy_static::lazy_static! {
    static ref VALID_DID_KEY_REGEX: Regex = Regex::new(r"^(plc|web)_[a-z0-9_]+$").unwrap();
}

fn convert_post_id_to_uri(id: &str) -> String {
    let last = id.split(':').last().unwrap();

    let parts: Vec<&str> = last.split('_').collect();

    format!(
        "at://did:{}:{}/app.bsky.feed.post/{}",
        parts[1], parts[2], parts[0]
    )
}

fn at_uri_to_post_id(uri: &str) -> anyhow::Result<String> {
    let u: Vec<&str> = uri.split('/').collect();
    let u_hostname = u.get(2).unwrap().to_string();
    let u_collection = u.get(3).unwrap().to_string();
    let u_rkey = u.get(4).unwrap().to_string();

    let collection: &str;
    if u_collection == "app.bsky.feed.post" {
        collection = "post";
    } else if u_collection == "app.bsky.graph.list" {
        collection = "list";
    } else {
        return Err(anyhow!("Unsupported URI {}", uri));
    }

    let mut did = did_to_key(&u_hostname, false)?;
    if did.starts_with("plc_did:plc:") {
        did = format!("plc_{}", &did[12..]);
    }
    ensure_valid_rkey(&u_rkey).unwrap();

    Ok(format!("{}:{}_{}", collection, u_rkey, did))
}

fn did_to_key(did: &str, full: bool) -> anyhow::Result<String> {
    let val: String;
    if did.starts_with("did:plc:") {
        val = format!("plc_{}", &did[8..]);
    } else if did.starts_with("did:web:") {
        val = format!("web_{}", &did[8..].replace('.', "_").replace('-', "__"));
    } else {
        return Err(anyhow!("Invalid DID {}", did));
    }

    if !VALID_DID_KEY_REGEX.is_match(&val) {
        return Err(anyhow!("Found invalid DID: {} {} {}", did, full, val));
    }

    if full {
        Ok(format!("did:{}", val))
    } else {
        Ok(val)
    }
}

fn unsafe_key_to_did(key: &str) -> anyhow::Result<String> {
    Ok(key.replace("_", ":"))
}

lazy_static::lazy_static! {
    static ref RKEY_REGEX: Regex = Regex::new(r"^[a-z0-9\-]+$").unwrap();
}

fn ensure_valid_rkey(rkey: &str) -> anyhow::Result<()> {
    if !RKEY_REGEX.is_match(rkey) {
        return Err(anyhow!("Invalid rkey {}", rkey));
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone)]
struct Post {
    id: String,
    text: String,
    alt_text: String,
    link: String,
    author: String,
    record: String,
    created_at: DateTime<Utc>,
    image_count: u32,
    is_reply: bool,
    is_hellthread: bool,
    lang: String,

    parent: String,
    root: String,

    reply_count: u32,
    repost_count: u32,
    like_count: u32,

    has_labels: bool,
    // langs: Vec<String>,
}

#[derive(Clone)]
struct Profile {
    id: String,
    name: String,
    handle: String,
    description: String,

    has_avatar: bool,
    has_banner: bool,
    // TODO follower count
    // TODO following count

    // TODO Use SkyFeed input for list?
    // TODO languages (posts)
    // TODO hashtags (posts)

    // TODO Post count
    // TODO Repost count
    // TODO Reply count
}

impl Post {
    /*  fn new(
        id: String,
        text: String,
        alt_text: String,
        author: String,
        lang: String,
        created_at: DateTime<Utc>,
        image_count: u32,
        is_reply: bool,
        is_hellthread: bool,

        reply_count: u32,
        repost_count: u32,
        like_count: u32,
    ) -> Post {
        Post {
            id,
            text,
            alt_text,
            author,
            lang,
            created_at,
            image_count,
            is_reply,
            is_hellthread,
            reply_count,
            repost_count,
            like_count,
        }
    } */

    fn calculate_score(&self, gravity: f64) -> f64 {
        let diff_hours = Utc::now()
            .signed_duration_since(self.created_at)
            .num_minutes()
            .abs() as f64
            / 60.0
            + 2.0;

        self.like_count as f64 / diff_hours.powf(gravity)
    }
}
