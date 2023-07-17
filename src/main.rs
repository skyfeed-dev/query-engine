use anyhow::anyhow;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum_macros::debug_handler;
use base64::{engine::general_purpose, Engine as _};
use chrono::prelude::*;
use http::Method;
use rand::{seq::SliceRandom, thread_rng};
use regex::Regex;
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;
use std::net::SocketAddr;
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::RwLock, task, time};
use tower_http::cors::{Any, CorsLayer};

struct ServerConfig {
    pub all_posts: HashMap<String, Post>,
    pub all_posts_by_author: HashMap<String, HashSet<String>>,
}

fn get_surreal_api_url() -> String {
    env::var("SURREAL_URL_SQL").unwrap()
}

fn get_surreal_auth_header() -> String {
    let user = env::var("SURREAL_USER").unwrap();
    let pass = env::var("SURREAL_PASS").unwrap();

    format!(
        "Basic {}",
        general_purpose::URL_SAFE_NO_PAD.encode(format!("{}:{}", user, pass))
    )
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().unwrap();
    println!("SurrealDB API URL: {}", env::var("SURREAL_URL_SQL").unwrap());

    let server_config = ServerConfig {
        all_posts: HashMap::new(),
        all_posts_by_author: HashMap::new(),
    };

    let arc = Arc::new(RwLock::new(server_config));

    println!("init");

    // ! 168h = 1 week
    run_query(
        "SELECT id,text,author,langs,createdAt,images,links,root,count(<-like) as likeCount,count(<-repost) as repostCount,count(<-replyto) as replyCount FROM post WHERE createdAt > (time::now() - 168h);", 
        &arc,
        Duration::from_secs(60 * 30)
    ).await.unwrap();

    println!("ready!");

    let new_posts_task_arc = Arc::clone(&arc);
    let _new_posts_task = task::spawn(async move {
        let query = "SELECT id,text,author,langs,createdAt,images,links,root,count(<-like) as likeCount,count(<-repost) as repostCount,count(<-replyto) as replyCount FROM post WHERE createdAt > (time::now() - 3000s);";
        let res = run_query(query, &new_posts_task_arc, Duration::from_secs(100)).await;
        if res.is_err() {
            println!("ERROR run_query {}", res.unwrap_err());
        }

        let mut interval = time::interval(Duration::from_secs(100));
        loop {
            interval.tick().await;
            let query = "SELECT id,text,author,langs,createdAt,images,links,root,count(<-like) as likeCount,count(<-repost) as repostCount,count(<-replyto) as replyCount FROM post WHERE createdAt > (time::now() - 300s);";
            let res = run_query(query, &new_posts_task_arc, Duration::from_secs(100)).await;
            if res.is_err() {
                println!("ERROR run_query {}", res.unwrap_err());
            }
        }
    });

    // every 5 mins
    let like_count_task_arc = Arc::clone(&arc);
    let _like_count_task = task::spawn(async move {
        time::sleep(Duration::from_secs(30)).await;

        let mut interval = time::interval(Duration::from_secs(60 * 5));
        loop {
            interval.tick().await;
            run_update_counts_query("12h", &like_count_task_arc)
                .await
                .unwrap_or(());
        }
    });
    // every 60 mins
    let like_count_task_arc_2 = Arc::clone(&arc);
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
    });

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);

    let app = Router::new()
        .route("/health", get(health_check))
        .route(
            "/xrpc/me.skyfeed.builder.generateFeedSkeleton",
            post(generate_feed_skeleton_route),
        )
        .layer(cors)
        .with_state(arc);

    let addr = SocketAddr::from(([0, 0, 0, 0], 4444));
    // tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn health_check() -> &'static str {
    ""
}

#[debug_handler]
async fn generate_feed_skeleton_route(
    State(state): State<Arc<RwLock<ServerConfig>>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let response = generate_feed_skeleton(state, payload.clone())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FeedBuilderResponse {
                    debug: FeedBuilderResponseDebug {
                        time: 0,
                        timing: HashMap::new(),
                        counts: HashMap::new(),
                    },
                    feed: vec![PostReference {
                        post: format!("Error: {}", e,),
                    }],
                }),
            )
        })
        .map(|res| (StatusCode::OK, Json(res)));

    response.unwrap_or_else(|(status, body)| (status, body))
}

static LISTITEM_QUERY: &str = "RETURN (SELECT ->listitem.out as dids FROM LIST_ID).dids;";

async fn generate_feed_skeleton(
    state: Arc<RwLock<ServerConfig>>,
    payload: Value,
) -> anyhow::Result<FeedBuilderResponse> {
    let blocks = payload["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block.as_object().unwrap().clone())
        .collect::<Vec<_>>();

    let mut posts: Vec<&Post> = vec![];

    let mut stash: HashMap<&str, Vec<&Post>> = HashMap::new();

    let filter_types = vec!["remove", "regex"];

    let mut debug = FeedBuilderResponseDebug {
        time: 0,
        timing: HashMap::new(),
        counts: HashMap::new(),
    };

    let sc = state.read().await;

    let query_start = Instant::now();

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
                        "http://10.0.0.5:4444/xrpc/app.bsky.feed.getFeedSkeleton?feed={}",
                        &feed_uri
                    ))
                    .headers(headers);

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
                let did = did_to_key(block["did"].as_str().unwrap(), true)?;

                for id in sc.all_posts_by_author.get(&did).unwrap_or(&HashSet::new()) {
                    posts.push(sc.all_posts.get(id).unwrap());
                }
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
                }
            } else if filter_type == "regex" {
                let value = filter["value"]
                    .as_str()
                    .unwrap()
                    .replace(r"\b", r"(?-u:\b)");

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
                posts.extend(stash.get(key).unwrap());
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
                posts.len() as u128,
            );
        }
    }

    if posts.len() > 1024 {
        posts.drain(1024..posts.len());
    }

    debug.time = query_start.elapsed().as_millis();

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

async fn fetch_list(list_uri: &str) -> anyhow::Result<Vec<String>> {
    let list_id = at_uri_to_post_id(list_uri)?;

    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse()?);
    headers.insert("NS", "atproto".parse()?);
    headers.insert("DB", "bsky".parse()?);
    headers.insert("Authorization", get_surreal_auth_header().parse()?);

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(LISTITEM_QUERY.replace("LIST_ID", &list_id));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    Ok(list2
        .iter()
        .map(|did| did.as_str().unwrap().to_string())
        .collect::<Vec<String>>())
}

async fn run_query(
    query: &str,
    mutex: &RwLock<ServerConfig>,
    timeout: Duration,
) -> Result<(), reqwest::Error> {
    println!("run_query {}", query);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", "atproto".parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(query.to_string())
        .timeout(timeout);

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;
    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    let mut sc = mutex.write().await;

    for p in list2 {
        let post = p.as_object().unwrap();

        let id = post["id"].as_str().unwrap().to_string();
        if sc.all_posts.contains_key(&id) {
            continue;
        }

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
                .map(|i| {
                    i.as_str()
                        .unwrap()
                        .strip_prefix("link:⟨")
                        .unwrap()
                        .strip_suffix("⟩")
                        .unwrap()
                })
                .collect::<Vec<&str>>()
                .join("|||")
        } else {
            "".to_string()
        };

        let new_post = Post {
            id: id.clone(),
            text: post["text"].as_str().unwrap().to_string(),
            alt_text: alt_text,
            link: link,
            author: author.clone(),
            lang: lang,
            created_at: DateTime::parse_from_rfc3339(post["createdAt"].as_str().unwrap())
                .unwrap()
                .into(),
            image_count,
            is_reply: !post["root"].is_null(),
            is_hellthread: !post["root"].is_null()
                && post["root"].as_str().unwrap()
                    == "post:plc_wgaezxqi2spqm3mhrb5xvkzi_3juzlwllznd24",
            reply_count: post["replyCount"].as_i64().unwrap().try_into().unwrap(),
            repost_count: post["repostCount"].as_i64().unwrap().try_into().unwrap(),
            like_count: post["likeCount"].as_i64().unwrap().try_into().unwrap(),
        };

        sc.all_posts.insert(id.clone(), new_post);

        if let Some(author_set) = sc.all_posts_by_author.get_mut(&author) {
            author_set.insert(id);
        } else {
            let mut author_set = HashSet::new();
            author_set.insert(id);
            sc.all_posts_by_author.insert(author.clone(), author_set);
        }
    }
    println!("run_query done {}", query);
    Ok(())
}

static REPLY_COUNT_QUERY: &str = "SELECT replyCount as c,subject as i from reply_count_view WHERE subject.createdAt > (time::now() - VAR_DURATION);";
static REPOST_COUNT_QUERY: &str = "SELECT repostCount as c,subject as i from repost_count_view WHERE subject.createdAt > (time::now() - VAR_DURATION);";
static LIKE_COUNT_QUERY: &str = "SELECT likeCount as c,subject as i from like_count_view WHERE subject.createdAt > (time::now() - VAR_DURATION);";

async fn run_update_counts_query(
    duration: &str,
    mutex: &RwLock<ServerConfig>,
) -> Result<(), reqwest::Error> {
    println!("run_update_counts_query {}", duration);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", "atproto".parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(
            vec![REPLY_COUNT_QUERY, REPOST_COUNT_QUERY, LIKE_COUNT_QUERY]
                .join("\n")
                .replace("VAR_DURATION", duration),
        )
        .timeout(Duration::from_secs(60 * 10));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    let reply_counts = list.get(0).unwrap()["result"].as_array().unwrap();
    let repost_counts = list.get(1).unwrap()["result"].as_array().unwrap();
    let like_counts = list.get(2).unwrap()["result"].as_array().unwrap();

    let mut sc = mutex.write().await;

    for post in reply_counts {
        let id = post["i"].as_str().unwrap();
        let reply_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().reply_count = reply_count.try_into().unwrap();
        }
    }

    for post in repost_counts {
        let id = post["i"].as_str().unwrap();
        let repost_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().repost_count = repost_count.try_into().unwrap();
        }
    }

    for post in like_counts {
        let id = post["i"].as_str().unwrap();
        let like_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().like_count = like_count.try_into().unwrap();
        }
    }
    println!("run_update_counts_query done {}", duration);
    Ok(())
}

lazy_static::lazy_static! {
    static ref VALID_DID_KEY_REGEX: Regex = Regex::new(r"^(plc|web)_[a-z0-9_]+$").unwrap();
}

fn convert_post_id_to_uri(id: &str) -> String {
    let last = id.split(':').last().unwrap();

    let parts: Vec<&str> = last.split('_').collect();

    format!(
        "at://did:{}:{}/app.bsky.feed.post/{}",
        parts[0], parts[1], parts[2]
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

    let mut did = did_to_key(&u_hostname, false).unwrap();
    if did.starts_with("plc_did:plc:") {
        did = format!("plc_{}", &did[12..]);
    }
    ensure_valid_rkey(&u_rkey).unwrap();

    Ok(format!("{}:{}_{}", collection, did, u_rkey))
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

lazy_static::lazy_static! {
    static ref RKEY_REGEX: Regex = Regex::new(r"^[a-z0-9\-]+$").unwrap();
}

fn ensure_valid_rkey(rkey: &str) -> anyhow::Result<()> {
    if !RKEY_REGEX.is_match(rkey) {
        return Err(anyhow!("Invalid rkey {}", rkey));
    }
    Ok(())
}

struct Post {
    id: String,
    text: String,
    alt_text: String,
    link: String,
    author: String,
    created_at: DateTime<Utc>,
    image_count: u32,
    is_reply: bool,
    is_hellthread: bool,
    lang: String,

    reply_count: u32,
    repost_count: u32,
    like_count: u32,
    // langs: Vec<String>,
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
