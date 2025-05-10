use anyhow::anyhow;
use arc_swap::ArcSwap;
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
use elsa::FrozenVec;
use http::Method;
use lasso::{Key, Spur, ThreadedRodeo};
use mimalloc::MiMalloc;
use rand::{seq::SliceRandom, thread_rng};
use regex::Regex;
use reqwest::Client;
use rkyv::{collections::btree_map::ArchivedBTreeMap, Archive};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::prelude::*,
    sync::OnceLock,
};
use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
    time::{Duration, Instant},
};
use std::{net::SocketAddr, path::PathBuf};
use tokio::{
    sync::{RwLock, RwLockWriteGuard},
    task, time,
};
use tower_http::cors::{Any, CorsLayer};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static HEADER_NAMESPACE: &str = "bsky";

static LINK_DETECT_ENABLED: bool = true;

#[derive(Clone)]
struct ServerConfigInstance {
    pub all_posts_by_key: BTreeMap<u32, Arc<PostV1>>,
    pub all_posts_by_id: BTreeMap<String, Arc<PostV1>>,
    pub label_to_posts: HashMap<String, RoaringBitmap>,
    pub all_posts_by_author: BTreeMap<u32, RoaringBitmap>,
    pub all_posts_by_tag: HashMap<String, RoaringBitmap>,

    pub last_post_id: String,
    pub tag_variations: HashMap<String, HashMap<String, u128>>,

    pub last_like_id: String,
    pub likes_post_to_users: BTreeMap<u32, RoaringBitmap>,
    pub likes_user_to_posts: BTreeMap<u32, RoaringBitmap>,
    pub all_profiles: HashMap<String, Profile>,
}

static POST_ID_MAP: OnceLock<ThreadedRodeo<lasso::Spur>> = OnceLock::new();
static USER_ID_MAP: OnceLock<ThreadedRodeo<lasso::Spur>> = OnceLock::new();

struct ServerConfigWrapper {
    pub data: ArcSwap<ServerConfigInstance>,
    pub post_links: RwLock<LinkedPostsState>,
    pub skygraph: RwLock<SkyGraphState>,
    pub regex_result_cache: RwLock<BTreeMap<String, Arc<RwLock<RegExResultCache>>>>,
}

struct RegExResultCache {
    pub matches: RoaringBitmap,
    pub last_processed_id: String,
}

struct SkyGraphState {
    community_name_to_id: HashMap<String, u32>,
    community_id_to_name: HashMap<u32, String>,

    user_to_communities: HashMap<u32, HashMap<u8, u32>>,
    community_to_users: HashMap<u32, HashSet<u32>>,

    uid_to_did: HashMap<u32, String>,
    did_to_uid: HashMap<String, u32>,

    moots: HashMap<u32, HashMap<u32, f64>>,
}

#[repr(u8)]
enum CommunityLayer {
    Gigacluster = 1,
    Supercluster = 2,
    Cluster = 3,
    Galaxy = 4,
    Nebula = 5,
    Constellation = 6,
}

#[derive(Serialize, Deserialize)]
struct LinkedPostsState {
    pub post_to_feeds: BTreeMap<String, HashSet<String>>,
    pub feed_to_posts: HashMap<String, BTreeSet<String>>,
}

#[derive(Deserialize)]
struct SkyGraphFileCommunityRelations {
    pub communities: Vec<SkyGraphFileCommunityRelationsItem>,
}
#[derive(Deserialize)]
struct SkyGraphFileCommunityRelationsItem {
    pub community: String,
    pub dids: Vec<String>,
}
#[derive(Deserialize)]
struct SkyGraphFileCommunities {
    pub nodes: Vec<SkyGraphFileCommunitiesItem>,
}
#[derive(Deserialize)]
struct SkyGraphFileCommunitiesItem {
    pub name: String,
    pub community: String,
}

#[derive(Deserialize)]
struct SkyGraphFileMoots {
    #[serde(rename = "topMoots")]
    pub top_moots: Vec<SkyGraphFileMootsUser>,
}
#[derive(Deserialize)]
struct SkyGraphFileMootsUser {
    pub source: String,
    #[serde(rename = "topMoots")]
    pub top_moots: Vec<SkyGraphFileMootsUserMoot>,
}
#[derive(Deserialize)]
struct SkyGraphFileMootsUserMoot {
    #[serde(rename = "toDid")]
    pub to_did: String,
    pub weight: f64,
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

async fn clean_post_links(post_links: &RwLock<LinkedPostsState>) -> anyhow::Result<()> {
    let mut w = post_links.write().await;

    let cutoff_tid = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::days(2))
            .unwrap(),
    );

    w.post_to_feeds.retain(|k, _| k > &cutoff_tid);

    for s in &mut w.feed_to_posts {
        s.1.retain(|p| p > &cutoff_tid);
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().unwrap();
    println!(
        "SurrealDB API URL: {}",
        env::var("SURREAL_URL_SQL").unwrap()
    );
    if false {
        let mut sc = ServerConfigInstance {
            all_posts_by_id: BTreeMap::new(),
            all_posts_by_key: BTreeMap::new(),
            label_to_posts: HashMap::new(),
            all_posts_by_author: BTreeMap::new(),
            all_posts_by_tag: HashMap::new(),
            last_post_id: "post:3l4g2".to_string(),
            tag_variations: HashMap::new(),
            all_profiles: HashMap::new(),

            last_like_id: format!("like:{}", "1"),
            likes_post_to_users: BTreeMap::new(),
            likes_user_to_posts: BTreeMap::new(),
        };
        generate_daily_blobs(&mut sc, true).await.unwrap();
        return;
    }

    POST_ID_MAP.get_or_init(|| ThreadedRodeo::new());
    USER_ID_MAP.get_or_init(|| ThreadedRodeo::new());

    let is_running_in_feed_mode = env::var("MODE").is_err();

    let include_likes = !env::var("INCLUDE_LIKES").is_err();
    let load_skygraph = !env::var("LOAD_SKYGRAPH").is_err();

    let is_test_mode = !env::var("TEST_MODE").is_err();

    // TODO archive mode
    let archive_mode_active = false;

    let tid_archive_end = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::days(if is_test_mode { 1 } else { 7 })) // TODO use 7
            .unwrap(),
    );

    let tid_one_hour_ago_perf_check = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::hours(1))
            .unwrap(),
    );

    let tid_2days_ago_likes = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::days(1))
            .unwrap(),
    );

    let tid_7days_ago_labels = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::days(7))
            .unwrap(),
    );

    let server_config_instance = ServerConfigInstance {
        all_posts_by_id: BTreeMap::new(),
        all_posts_by_key: BTreeMap::new(),
        label_to_posts: HashMap::new(),
        all_posts_by_author: BTreeMap::new(),
        all_posts_by_tag: HashMap::new(),
        last_post_id: format!("post:{}", tid_archive_end),
        tag_variations: HashMap::new(),
        all_profiles: HashMap::new(),

        last_like_id: format!("like:{}", tid_2days_ago_likes),
        likes_post_to_users: BTreeMap::new(),
        likes_user_to_posts: BTreeMap::new(),
    };

    let server_config_wrapper = ServerConfigWrapper {
        data: ArcSwap::from(Arc::new(server_config_instance)),
        post_links: RwLock::new(LinkedPostsState {
            post_to_feeds: BTreeMap::new(),
            feed_to_posts: HashMap::new(),
        }),
        skygraph: RwLock::new(SkyGraphState {
            community_id_to_name: HashMap::new(),
            community_name_to_id: HashMap::new(),
            community_to_users: HashMap::new(),
            user_to_communities: HashMap::new(),
            did_to_uid: HashMap::new(),
            uid_to_did: HashMap::new(),
            moots: HashMap::new(),
        }),
        regex_result_cache: RwLock::new(BTreeMap::new()),
    };
    println!("clean_post_links start");

    clean_post_links(&server_config_wrapper.post_links)
        .await
        .expect("failed to clean_post_links");

    let root_arc = Arc::new(server_config_wrapper);

    let arc = Arc::clone(&root_arc);

    println!("init");

    // ! 168h = 1 week

    if is_running_in_feed_mode {
        println!("running in feed mode");
        {
            let mut sc = ServerConfigInstance::clone(&arc.data.load_full());

            run_posts_query(&mut sc, Duration::from_secs(60 * 15), false)
                .await
                .unwrap();
            println!("check {} {}", sc.last_post_id, tid_one_hour_ago_perf_check);

            while sc.last_post_id < format!("post:{}", tid_one_hour_ago_perf_check) {
                run_posts_query(&mut sc, Duration::from_secs(60 * 15), false)
                    .await
                    .unwrap();
            }

            let res = run_update_counts_query(&tid_archive_end, &mut sc, false).await;
            if res.is_err() {
                println!("ERROR run_update_counts_query {}", res.unwrap_err());
            }

            let res = run_update_labels_query(&tid_7days_ago_labels, &mut sc).await;
            if res.is_err() {
                println!("ERROR run_update_labels_query {}", res.unwrap_err());
            }

            if include_likes {
                while sc.last_like_id < format!("like:{}", tid_one_hour_ago_perf_check) {
                    run_likes_query(&mut sc, Duration::from_secs(60 * 15))
                        .await
                        .unwrap();
                }
            }

            println!("swapping state");
            arc.data.swap(Arc::new(sc));
        }

        println!("ready!");

        let _new_posts_task = task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            let mut counter = 0;
            loop {
                interval.tick().await;
                println!("fetching new posts...");

                {
                    let mut sc = ServerConfigInstance::clone(&arc.data.load_full());
                    let res = run_posts_query(&mut sc, Duration::from_secs(30), false).await;

                    if res.is_err() {
                        println!("ERROR run_query {}", res.unwrap_err());
                    }

                    if include_likes {
                        let res = run_likes_query(&mut sc, Duration::from_secs(60 * 15)).await;
                        if res.is_err() {
                            println!("ERROR run_likes_query {}", res.unwrap_err());
                        }
                    }

                    let count_queries_anchor =
                        sc.all_posts_by_id.first_key_value().unwrap().0.clone();

                    counter += 1;
                    if counter % 5000 == 0 {
                        println!("fetching new counts...");

                        let res =
                            run_update_counts_query(&count_queries_anchor, &mut sc, false).await;

                        if res.is_err() {
                            println!("ERROR run_update_counts_query {}", res.unwrap_err());
                        }

                        let labels_query_anchor = datetime_to_tid(
                            Utc::now()
                                .checked_sub_signed(chrono::Duration::hours(50))
                                .unwrap(),
                        );
                        let res = run_update_labels_query(&labels_query_anchor, &mut sc).await;
                        if res.is_err() {
                            println!("ERROR run_update_labels_query {}", res.unwrap_err());
                        }
                        if archive_mode_active {
                            let res = generate_daily_blobs(&mut sc, false).await;
                            if res.is_err() {
                                println!("ERROR generate_daily_blobs {}", res.unwrap_err());
                            }
                        }
                    } else if counter % 100 == 0 {
                        println!("fetching new counts (10x)...");
                        let res = run_update_counts_query(
                            &datetime_to_tid(
                                Utc::now()
                                    .checked_sub_signed(chrono::Duration::minutes(300))
                                    .unwrap(),
                            ),
                            &mut sc,
                            false,
                        )
                        .await;
                        if res.is_err() {
                            println!("ERROR run_update_counts_query {}", res.unwrap_err());
                        }
                        let res = run_update_labels_query(
                            &datetime_to_tid(
                                Utc::now()
                                    .checked_sub_signed(chrono::Duration::minutes(300))
                                    .unwrap(),
                            ),
                            &mut sc,
                        )
                        .await;
                        if res.is_err() {
                            println!("ERROR run_update_labels_query {}", res.unwrap_err());
                        }
                    } else if counter % 10 == 0 {
                        println!("fetching new counts (10x)...");
                        let res = run_update_counts_query(
                            &datetime_to_tid(
                                Utc::now()
                                    .checked_sub_signed(chrono::Duration::minutes(50))
                                    .unwrap(),
                            ),
                            &mut sc,
                            false,
                        )
                        .await;
                        if res.is_err() {
                            println!("ERROR run_update_counts_query {}", res.unwrap_err());
                        }
                        let res = run_update_labels_query(
                            &datetime_to_tid(
                                Utc::now()
                                    .checked_sub_signed(chrono::Duration::minutes(50))
                                    .unwrap(),
                            ),
                            &mut sc,
                        )
                        .await;
                        if res.is_err() {
                            println!("ERROR run_update_labels_query {}", res.unwrap_err());
                        }
                    } else {
                        let labels_query_anchor = datetime_to_tid(
                            Utc::now()
                                .checked_sub_signed(chrono::Duration::minutes(20))
                                .unwrap(),
                        );
                        let res = run_update_labels_query(&labels_query_anchor, &mut sc).await;
                        if res.is_err() {
                            println!("ERROR run_update_labels_query {}", res.unwrap_err());
                        }
                    }
                    if counter % 240 == 0 {
                        let res = clean_memory(&mut sc).await;
                        if res.is_err() {
                            println!("ERROR clean_memory {}", res.unwrap_err());
                        }
                        let res = clean_post_links(&arc.post_links).await;
                        if res.is_err() {
                            println!("ERROR clean_post_links {}", res.unwrap_err());
                        }

                        let res = dump_links(&arc.post_links).await;
                        if res.is_err() {
                            println!("ERROR dump_links {}", res.unwrap_err());
                        }
                    }
                    arc.data.swap(Arc::new(sc));
                }

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
        {
            let mut sc = ServerConfigInstance::clone(&arc.data.load_full());

            println!("running in list mode");
            run_profiles_query(&mut sc, Duration::from_secs(60 * 10))
                .await
                .unwrap();
            arc.data.swap(Arc::new(sc));
        }

        println!("ready!");

        // ! every 30 minutes
        let _update_profiles_task = task::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(60 * 30));
            let mut is_first_run = true;
            loop {
                interval.tick().await;
                if is_first_run {
                    is_first_run = false;
                    continue;
                }
                let mut sc = ServerConfigInstance::clone(&arc.data.load_full());

                let res = run_profiles_query(&mut sc, Duration::from_secs(60 * 10)).await;
                if res.is_err() {
                    println!("ERROR run_profiles_query {}", res.unwrap_err());
                }
                arc.data.swap(Arc::new(sc));
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
        // TODO re-implement list generator
        /*    .route(
            "/xrpc/app.skyfeed.graph.generateListSkeleton",
            post(generate_list_skeleton_route),
        ) */
        .route(
            "/xrpc/app.skyfeed.feed.getTrendingTags",
            get(get_trending_tags),
        )
        .route("/api/admin/stats", get(admin_get_stats))
        .layer(cors)
        .with_state(root_arc);

    let addr = SocketAddr::from((
        [0, 0, 0, 0],
        if is_running_in_feed_mode { 4040 } else { 4445 },
    ));
    // tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

fn get_user_uid(did: &String, g: &mut RwLockWriteGuard<'_, SkyGraphState>) -> anyhow::Result<u32> {
    if !g.did_to_uid.contains_key(did) {
        let new_uid = g.did_to_uid.len() as u32;
        g.did_to_uid.insert(did.clone(), new_uid);
        g.uid_to_did.insert(new_uid, did.clone());
    }
    Ok(*g.did_to_uid.get(did).unwrap())
}

async fn dump_links(links: &RwLock<LinkedPostsState>) -> anyhow::Result<()> {
    let links = links.read().await;

    let mut path_buf = PathBuf::new();
    path_buf.push("post_feed_links");
    path_buf.push("feed_to_posts.json");
    if let Some(parent_dir) = path_buf.parent() {
        tokio::fs::create_dir_all(parent_dir).await?;
    }
    let serialized = simd_json::serde::to_string(&links.feed_to_posts)?;
    let mut file = std::fs::File::create(path_buf)?;
    file.write_all(serialized.as_bytes())?;

    let mut path_buf = PathBuf::new();
    path_buf.push("post_feed_links");
    path_buf.push("post_to_feeds.json");
    if let Some(parent_dir) = path_buf.parent() {
        tokio::fs::create_dir_all(parent_dir).await?;
    }
    let serialized = simd_json::serde::to_string(&links.post_to_feeds)?;
    let mut file = std::fs::File::create(path_buf)?;
    file.write_all(serialized.as_bytes())?;

    Ok(())
}

async fn health_check() -> &'static str {
    ""
}
#[derive(Serialize)]
struct SkyGraphCommunityNameResponse {
    words: Vec<SkyGraphCommunityNameResponseWord>,
}
#[derive(Serialize)]
struct SkyGraphCommunityNameResponseWord {
    word: String,
    count: u128,
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

    let cutoff_tid = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::minutes(minutes))
            .unwrap(),
    );

    let mut tags = vec![];

    let sc = state.data.load();

    // sc.tag_variations

    for tag in sc.all_posts_by_tag.keys() {
        let mut count = 0;
        for p in sc.all_posts_by_tag.get(tag).unwrap() {
            let p = sc.all_posts_by_key.get(&p).unwrap();
            if p.id > cutoff_tid {
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

async fn admin_get_stats(State(state): State<Arc<ServerConfigWrapper>>) -> impl IntoResponse {
    let sc = state.data.load();

    (
        StatusCode::OK,
        Json(AdminStatsResponse {
            all_posts_by_author_length: sc.all_posts_by_author.len() as u128,
            all_posts_by_tag_length: sc.all_posts_by_tag.len() as u128,
            all_posts_length: sc.all_posts_by_key.len() as u128,
            count_queries_anchor: "DEPRECATED".to_string(),
            last_post_id: sc.last_post_id.clone(),
            pointer_is_a: true,
        }),
    )
}

fn input_archived(
    blocks: &Vec<Value>,
    limit: u16, // ! u16 is perfect
    newest_first: bool,
) -> anyhow::Result<Vec<PostV1>> {
    let mut path_buf = PathBuf::new();
    path_buf.push("cache");
    path_buf.push("input_archived_posts");
    path_buf.push(if newest_first { "newest" } else { "oldest" });
    path_buf.push(format!("limit_{}", limit));

    std::fs::create_dir_all(&path_buf);

    path_buf.push(
        blake3::hash(serde_json::to_string(&blocks).unwrap().as_bytes())
            .to_hex()
            .as_str(),
    );

    let mut archive_query_cache: PostV1ArchiveQueryCache = if path_buf.exists() {
        let mut input = std::fs::OpenOptions::new()
            .read(true)
            .open(&path_buf)
            .unwrap();
        println!("loading archive_query_cache {:?}", &path_buf);

        let mut buffer = Vec::new();
        input.read_to_end(&mut buffer)?;

        let archived =
            rkyv::access::<ArchivedPostV1ArchiveQueryCache, rkyv::rancor::Error>(&buffer)?;
        rkyv::deserialize::<PostV1ArchiveQueryCache, rkyv::rancor::Error>(archived).unwrap()
    } else {
        PostV1ArchiveQueryCache {
            anchor: "".to_string(),
            posts: vec![],
        }
    };

    let mut list: Vec<std::fs::DirEntry> = std::fs::read_dir("archive/post_v1")
        .unwrap()
        .map(|f| f.unwrap())
        .collect();

    let anchor = archive_query_cache.anchor.as_str();
    let mut insert_at_index = 0u32;

    if !anchor.is_empty() {
        list.retain(|f| f.file_name().to_str().unwrap() > anchor);
    }

    if newest_first {
        list.sort_by(|b, a| a.path().cmp(&b.path()));
    } else {
        list.sort_by(|a, b| a.path().cmp(&b.path()));
    }

    let mut regex_cache: HashMap<String, Regex> = HashMap::new();

    for file in &list {
        let input = std::fs::OpenOptions::new()
            .read(true)
            .open(file.path())
            .unwrap();

        println!("input_archived mmap {:?}", &file.path());
        let mmap: memmap2::Mmap = unsafe { memmap2::Mmap::map(&input).unwrap() };

        let pinned_bytes = Box::new(unsafe { std::pin::Pin::new_unchecked(mmap.as_ref()) });

        let rkyv_map = unsafe {
            rkyv::access_unchecked::<ArchivedBTreeMap<rkyv::string::ArchivedString, ArchivedPostV1>>(
                &pinned_bytes,
            )
        };

        let mut posts: Vec<&ArchivedPostV1> = rkyv_map.values().collect();

        for b in blocks {
            let block = b.as_object().unwrap();

            let b_type = block["type"].as_str().unwrap();
            if b_type == "regex" {
                let value = block["value"]
                    .as_str()
                    .unwrap()
                    .replace(r"\b", r"(?-u:\b)")
                    .replace(r"\B", r"(?-u:\b)");

                let case_sensitive = if block.contains_key("caseSensitive") {
                    block["caseSensitive"].as_bool().unwrap_or(false)
                } else {
                    false
                };
                let cache_key = format!("{}/{}", case_sensitive, value);

                let re = if regex_cache.contains_key(&cache_key) {
                    regex_cache.get(&cache_key).unwrap()
                } else {
                    let regex = if !case_sensitive {
                        format!("(?i){}", value)
                    } else {
                        value.to_string()
                    };
                    let re = Regex::new(&regex)?;
                    regex_cache.insert(cache_key.clone(), re);
                    regex_cache.get(&cache_key).unwrap()
                };

                let target = if block.contains_key("regexType") {
                    block["regexType"].as_str().unwrap_or("text")
                } else {
                    "text"
                };

                let invert = if block.contains_key("invert") {
                    block["invert"].as_bool().unwrap_or(false)
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
                        posts.retain(|p| !re.is_match(&p.links.concat()));
                    } else {
                        posts.retain(|p| re.is_match(&p.links.concat()));
                    }
                } else if target == "text|alt_text" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.text) || re.is_match(&p.alt_text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.text) || re.is_match(&p.alt_text));
                    }
                } else if target == "alt_text|link" {
                    if invert {
                        posts.retain(|p| {
                            !(re.is_match(&p.links.concat()) || re.is_match(&p.alt_text))
                        });
                    } else {
                        posts
                            .retain(|p| re.is_match(&p.links.concat()) || re.is_match(&p.alt_text));
                    }
                } else if target == "text|link" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.links.concat()) || re.is_match(&p.text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.links.concat()) || re.is_match(&p.text));
                    }
                } else if target == "text|alt_text|link" {
                    if invert {
                        posts.retain(|p| {
                            !(re.is_match(&p.links.concat())
                                || re.is_match(&p.text)
                                || re.is_match(&p.alt_text))
                        });
                    } else {
                        posts.retain(|p| {
                            re.is_match(&p.links.concat())
                                || re.is_match(&p.text)
                                || re.is_match(&p.alt_text)
                        });
                    }
                }
            } else if b_type == "keep" || b_type == "remove" {
                let keep_type = block["subject"].as_str().unwrap();
                if keep_type == "tags" {
                    let mut tags: HashSet<String> = HashSet::new();
                    for tag in block["value"].as_array().unwrap() {
                        tags.insert(tag.as_str().unwrap().to_lowercase());
                    }
                } else if keep_type == "like_count" {
                    if block.contains_key("min") {
                        let min: u32 = block["min"].as_i64().unwrap().try_into()?;
                        posts.retain(|p| p.like_count >= min);
                    }
                } else if keep_type == "videos" {
                    if b_type == "keep" {
                        posts.retain(|p| !p.video.is_none());
                    } else {
                        posts.retain(|p| p.video.is_none());
                    }
                }
            }
        }

        if newest_first {
            if anchor.is_empty() {
                for p in posts.iter().rev() {
                    archive_query_cache
                        .posts
                        .push(clone_archived_post_to_post(p));
                }
            } else {
                for p in posts.iter().rev() {
                    archive_query_cache
                        .posts
                        .insert(insert_at_index as usize, clone_archived_post_to_post(p));
                    insert_at_index += 1;
                }
            }
        } else {
            archive_query_cache
                .posts
                .extend(posts.iter().map(|p| clone_archived_post_to_post(p)));
        }

        if anchor.is_empty() {
            if archive_query_cache.posts.len() >= limit.into() {
                break;
            }
        }
    }

    if archive_query_cache.posts.len() > limit.into() {
        archive_query_cache.posts.drain((limit as usize)..);
    }

    if list.is_empty() {
    } else if newest_first {
        archive_query_cache.anchor = list
            .first()
            .clone()
            .unwrap()
            .file_name()
            .to_str()
            .unwrap()
            .to_string();
    } else {
        archive_query_cache.anchor = list
            .last()
            .unwrap()
            .file_name()
            .to_str()
            .unwrap()
            .to_string();
    }

    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path_buf)
        .unwrap();
    output
        .write_all(&rkyv::to_bytes::<rkyv::rancor::Error>(&archive_query_cache).unwrap())
        .unwrap();
    drop(output);

    let post_id_map = POST_ID_MAP.get().unwrap();
    let user_id_map = USER_ID_MAP.get().unwrap();

    archive_query_cache.posts.iter_mut().for_each(|p| {
        p.key = post_id_map.get_or_intern(&p.id);
        p.author_key = user_id_map.get_or_intern(&p.author);
    });

    Ok(archive_query_cache.posts)
}

fn get_newest_post_archive_tid() -> String {
    std::fs::create_dir_all("archive/post_v1").unwrap();

    let mut list: Vec<std::fs::DirEntry> = std::fs::read_dir("archive/post_v1")
        .unwrap()
        .map(|f| f.unwrap())
        .collect();

    if list.is_empty() {
        return "000000000".to_string();
    }

    list.sort_by(|b, a| a.path().cmp(&b.path()));

    let date = list
        .first()
        .unwrap()
        .path()
        .clone()
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
        .clone();
    date
}

async fn generate_daily_blobs(
    sc: &mut ServerConfigInstance,
    is_running_in_background: bool,
) -> anyhow::Result<()> {
    let start = Instant::now();

    let tid_in_10mins = datetime_to_tid(
        Utc::now()
            .checked_add_signed(chrono::Duration::minutes(10))
            .unwrap(),
    );

    std::fs::create_dir_all("archive/post_v1").unwrap();

    let mut already_saved_days: Vec<String> = vec![];

    loop {
        if is_running_in_background {
            run_posts_query(sc, Duration::from_secs(60 * 15), true)
                .await
                .unwrap();
        }

        let mut first_occurence_map: Vec<String> = vec![];

        for post in &sc.all_posts_by_key {
            let date = post.1.id[0..5].to_string();
            if !first_occurence_map.contains(&date) {
                first_occurence_map.push(date);
            }
        }

        first_occurence_map.sort_by(|a: &String, b| a.cmp(&b));
        first_occurence_map.retain(|c| c < &tid_in_10mins);

        println!("dates {:?}", first_occurence_map);

        let mut ids_to_delete: Vec<u32> = vec![];

        // TODO Maybe use lower than 30 cutoff
        if first_occurence_map.len() > 8 && sc.all_posts_by_key.len() > 100000 {
            for date in &first_occurence_map[0..(first_occurence_map.len() - 8)] {
                if already_saved_days.contains(date) {
                    println!("abort write due to duplicate date {}", date);
                    continue;
                }
                let mut post_map: BTreeMap<String, PostV1> = BTreeMap::new();

                for (id, post) in &sc.all_posts_by_key {
                    // let post = sc.all_posts.get(id).unwrap();
                    let post_date = post.id[0..5].to_string();
                    if &post_date == date {
                        post_map.insert(post.id.clone(), PostV1::clone(post));
                        ids_to_delete.push(post.key.into_inner().into());
                    }
                }

                println!("write {}", date);
                already_saved_days.push(date.clone());
                let mut output = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(format!("archive/post_v1/{}.rkyv", date))
                    .unwrap();
                output
                    .write_all(&rkyv::to_bytes::<rkyv::rancor::Error>(&post_map).unwrap())
                    .unwrap();
                drop(output);
            }
        }
        for key in ids_to_delete {
            if let Some(p) = sc.all_posts_by_key.get(&key) {
                sc.all_posts_by_id.remove(&p.id);
            }
            sc.all_posts_by_key.remove(&key);
        }
        if !is_running_in_background {
            break;
        }
    }

    println!("admin_save_blob {}ms", start.elapsed().as_millis());
    Ok(())
}

#[derive(Serialize)]
struct AdminStatsResponse {
    pub all_posts_length: u128,
    pub all_posts_by_author_length: u128,
    pub all_posts_by_tag_length: u128,

    pub last_post_id: String,
    pub count_queries_anchor: String,

    pub pointer_is_a: bool,
}

async fn clean_memory(sc: &mut ServerConfigInstance) -> anyhow::Result<()> {
    println!("clean_up_old_posts2");

    let start = Instant::now();

    let cutoff_tid = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::days(7))
            .unwrap(),
    );

    println!("clean_up_old_posts3");

    let mut ids_to_remove: HashSet<u32> = HashSet::new();

    for p in sc.all_posts_by_key.values() {
        if p.id < cutoff_tid {
            ids_to_remove.insert(p.key.into_inner().into());
        }
    }

    println!("clean_up_old_posts4");

    for set in sc.all_posts_by_author.values_mut() {
        for key in &ids_to_remove {
            set.remove(*key);
        }
        // set.retain(|x| !ids_to_remove.contains(x));
    }
    println!("clean_up_old_posts5");
    for set in sc.all_posts_by_tag.values_mut() {
        for key in &ids_to_remove {
            set.remove(*key);
        }
        //  set.retain(|x| !ids_to_remove.contains(x));
    }

    println!("clean_up_old_posts6");
    for set in sc.label_to_posts.values_mut() {
        for key in &ids_to_remove {
            set.remove(*key);
        }
        // set.retain(|x| !ids_to_remove.contains(x));
    }

    println!("clean_up_old_posts7");

    for key in ids_to_remove {
        if let Some(p) = sc.all_posts_by_key.get(&key) {
            sc.all_posts_by_id.remove(&p.id);
        }
        sc.all_posts_by_key.remove(&key);
    }

    let likes_cutoff_tid = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::days(2))
            .unwrap(),
    );

    // TODO Implement this again
    /* for liked_posts in sc.likes_user_to_posts.values_mut() {
        liked_posts.retain(|x| x > &likes_cutoff_tid);
    }
    sc.likes_post_to_users.retain(|k, _| k > &likes_cutoff_tid); */

    /* for id in ids {
        sc.all_posts.remove(&id);
    } */

    // state.data.swap(Arc::new(sc));

    println!("clean_up_old_posts_done {}ms", start.elapsed().as_millis());
    Ok(())
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
                    scores: BTreeMap::new(),
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
                        scores: BTreeMap::new(),
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
                scores: BTreeMap::new(),
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
            if regex_block_count > 10 {
                // ! Error Message: Your custom feed has too many RegEx blocks! Using a single RegEx block and one additional inverted RegEx block should be enough to achieve the same result. Reply to this message if you need help!

                return Ok(FeedBuilderResponse {
                    debug: FeedBuilderResponseDebug {
                        time: 0,
                        timing: HashMap::new(),
                        counts: HashMap::new(),
                        scores: BTreeMap::new(),
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

    let mut posts_tmp: HashMap<u16, Vec<PostV1>> = HashMap::new();
    let post_cache: FrozenVec<Vec<PostV1>> = FrozenVec::new();

    let mut pre_block_index: u16 = 0;
    let mut pre_block_timings: HashMap<u16, u128> = HashMap::new();

    let query_start = Instant::now();

    for block in &blocks {
        let pre_block_start = Instant::now();
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

                let mut posts: Vec<PostV1> = vec![];

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
                // TODO First check if there are enough likes in memory (likely not)
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
                //  posts.insert(0, Cow::Borrowed(sc.all_posts.get(&post_id).unwrap()));
                // }
            } else if input_type == "archived" {
                let children = block["children"].as_array().unwrap();

                let mut limit: u16 = if block.contains_key("limit") {
                    block["limit"].as_i64().unwrap_or(100) as u16
                } else {
                    100
                };

                let oldest_first = if block.contains_key("oldestFirst") {
                    block["oldestFirst"].as_bool().unwrap_or(false)
                } else {
                    false
                };

                if limit > 1000 {
                    limit = 1000;
                }

                let new_posts = input_archived(children, limit, !oldest_first)?;

                posts_tmp.insert(pre_block_index, new_posts);
            }
        }
        let elapsed = pre_block_start.elapsed();
        let millis = elapsed.as_millis();

        pre_block_timings.insert(pre_block_index, millis);

        pre_block_index += 1;
    }

    let mut posts: Vec<&PostV1> = vec![];
    let mut stash: HashMap<&str, Vec<&PostV1>> = HashMap::new();

    let filter_types = vec!["remove", "keep", "regex"];

    let mut debug = FeedBuilderResponseDebug {
        time: 0,
        timing: HashMap::new(),
        counts: HashMap::new(),
        scores: BTreeMap::new(),
    };
    let sc = state.data.load();

    let mut post_score_global: BTreeMap<u32, f64> = BTreeMap::new();

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

                let cutoff_tid = datetime_to_tid(
                    Utc::now()
                        .checked_sub_signed(chrono::Duration::seconds(seconds))
                        .unwrap(),
                );
                let cutoff_front = "3s".to_string();

                for (_, value) in sc.all_posts_by_id.range(cutoff_tid..cutoff_front) {
                    posts.push(value);
                }
            } else if input_type == "list" {
                let user_keys = fetch_list(block, &state).await?;

                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };
                let cutoff_tid = datetime_to_tid(
                    Utc::now()
                        .checked_sub_signed(chrono::Duration::seconds(seconds))
                        .unwrap(),
                );

                for did in user_keys {
                    for id in sc
                        .all_posts_by_author
                        .get(&did)
                        .unwrap_or(&RoaringBitmap::new())
                    {
                        let post = sc.all_posts_by_key.get(&id).unwrap();
                        if post.id > cutoff_tid {
                            posts.push(post);
                        }
                    }
                }
            } else if input_type == "custom_likedweighted" {
                // ? Fetches a list of users and then inputs posts liked by them. Users who like a lot are worth less
                // ? param: historySeconds (for posts)
                // ? param: baseLikeCount, default: 0
                // ? score += 1 / USER_LIKES_COUNT
                let dids = fetch_list(block, &state).await?;

                let base_like_count = if block.contains_key("baseLikeCount") {
                    block["baseLikeCount"].as_i64().unwrap_or(0)
                } else {
                    0
                };

                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };
                let cutoff_tid = datetime_to_tid(
                    Utc::now()
                        .checked_sub_signed(chrono::Duration::seconds(seconds))
                        .unwrap(),
                );

                let mut post_score: BTreeMap<u32, f64> = BTreeMap::new();
                for user_key in dids {
                    let like_count = sc
                        .likes_user_to_posts
                        .get(&user_key)
                        .unwrap_or(&RoaringBitmap::new())
                        .len() as i64;

                    for key in sc
                        .likes_user_to_posts
                        .get(&user_key)
                        .unwrap_or(&RoaringBitmap::new())
                    {
                        // TODO Implement
                        // if id > &cutoff_tid {
                        // TODO Maybe add custom score function support
                        let user_score = 1f64 / ((base_like_count + like_count) as f64);
                        if let Some(score) = post_score.get(&key) {
                            post_score.insert(key, score + user_score);
                        } else {
                            post_score.insert(key, user_score);
                        };
                    }
                }

                let mut post_scores: Vec<_> = post_score.iter().collect();
                post_scores.sort_by(|b, a| a.1.partial_cmp(b.1).unwrap());
                posts.clear();
                for p in post_scores {
                    if let Some(post) = sc.all_posts_by_key.get(p.0) {
                        posts.push(post);
                    }
                }
                if seconds < 604800 {
                    posts.retain(|p| p.id > cutoff_tid);
                }

                add_post_score_to_global_normalized(&post_score, &mut post_score_global, &block)?;
            } else if input_type == "custom_likedbylikers" {
                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };

                let score_exponent_str = if block.contains_key("scoreExponent") {
                    block["scoreExponent"].as_str().unwrap_or("1.3")
                } else {
                    "1.3"
                };

                let score_exponent = score_exponent_str.parse::<f64>()?;

                let user_score_function = if block.contains_key("userScoreFunction") {
                    block["userScoreFunction"].as_str().unwrap_or("f1")
                } else {
                    "f1"
                };

                let cutoff_tid = datetime_to_tid(
                    Utc::now()
                        .checked_sub_signed(chrono::Duration::seconds(seconds))
                        .unwrap(),
                );

                let mut curators: HashMap<u32, f64> = HashMap::new();

                for post in &posts {
                    if let Some(likers) = sc.likes_post_to_users.get(&post.key.into_inner().into())
                    {
                        for liker in likers {
                            let s = match user_score_function {
                                "f0" => 1f64 / (likers.len() as f64).powf(score_exponent),
                                "f1" => 1f64 / (likers.len() as f64),
                                "f2" => 100f64 - (likers.len() as f64).sqrt(),
                                "f3" => 10f64 - (likers.len() as f64).ln(),
                                "f4" => 1f64 / (likers.len() as f64).sqrt(),
                                "f5" => 1f64 / (likers.len() as f64).ln(),
                                _ => 1f64,
                            };

                            if s > 0f64 {
                                if let Some(score) = curators.get(&liker) {
                                    curators.insert(liker, score + s);
                                } else {
                                    curators.insert(liker, s);
                                };
                            }
                        }
                    }
                }
                let user_scores: Vec<_> = curators.iter().collect();

                let mut post_score: BTreeMap<u32, f64> = BTreeMap::new();

                for user in user_scores {
                    let ps: &f64 = user.1;
                    for id in sc
                        .likes_user_to_posts
                        .get(user.0)
                        .unwrap_or(&RoaringBitmap::new())
                    {
                        // TODO Implement cutoff
                        if let Some(score) = post_score.get(&id) {
                            post_score.insert(id.clone(), score + ps);
                        } else {
                            post_score.insert(id.clone(), *ps);
                        };
                        /*  if id > &cutoff_tid {
                            if let Some(score) = post_score.get(id) {
                                post_score.insert(id.clone(), score + ps);
                            } else {
                                post_score.insert(id.clone(), *ps);
                            };
                        } */
                    }
                }
                let mut post_scores: Vec<_> = post_score.iter().collect();
                post_scores.sort_by(|b, a| a.1.partial_cmp(b.1).unwrap());
                posts.clear();
                for p in post_scores {
                    if let Some(post) = sc.all_posts_by_key.get(p.0) {
                        posts.push(post);
                    }
                }
                if seconds < 604800 {
                    posts.retain(|p| p.id > cutoff_tid);
                }
                add_post_score_to_global_normalized(&post_score, &mut post_score_global, &block)?;
            } else if input_type == "tags" {
                let array = block["tags"].as_array().unwrap();

                // ---

                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };

                let duration = chrono::Duration::seconds(seconds);

                let cutoff_tid = datetime_to_tid(Utc::now().checked_sub_signed(duration).unwrap());

                let is_cutoff_beyond_7_days = seconds >= 604800;

                let mut i: u32 = 0;

                for tag_value in array {
                    let tag = tag_value.as_str().unwrap().to_lowercase();
                    for key in sc
                        .all_posts_by_tag
                        .get(&tag)
                        .unwrap_or(&RoaringBitmap::new())
                    {
                        let post = sc.all_posts_by_key.get(&key).unwrap();
                        if is_cutoff_beyond_7_days {
                            posts.push(post);
                            i = i + 1;
                        } else {
                            if post.id > cutoff_tid {
                                posts.push(post);
                            }
                        }
                    }
                }
                // TODO Adjust constant
                if is_cutoff_beyond_7_days && i < 10000 {
                    // let children = block["children"].as_array().unwrap();
                    let limit_str = if block.contains_key("limit") {
                        block["limit"].as_str().unwrap_or("1000")
                    } else {
                        "1000"
                    };
                    // TODO Maybe make this a u8
                    let limit = limit_str.parse::<u32>()?;

                    if i < limit {
                        let children: Vec<Value> = vec![serde_json::json!(
                            {
                                "type": "keep",
                                "keepType": "tags",
                                "value": array,
                            }
                        )];
                        if false {
                            let new_posts = input_archived(&children, (limit - i) as u16, true)?;

                            posts.extend(post_cache.push_get(new_posts));
                        }
                    }
                }
            } else if input_type == "labels" {
                let array = block["labels"].as_array().unwrap();

                let seconds = if block.contains_key("historySeconds") {
                    block["historySeconds"].as_i64().unwrap_or(604800)
                } else {
                    604800
                };

                let duration = chrono::Duration::seconds(seconds);

                let cutoff_tid = datetime_to_tid(Utc::now().checked_sub_signed(duration).unwrap());

                let is_cutoff_beyond_7_days = seconds >= 604800;

                for label_value in array {
                    let label = label_value.as_str().unwrap().to_lowercase();
                    for id in sc
                        .label_to_posts
                        .get(&label)
                        .unwrap_or(&RoaringBitmap::new())
                    {
                        let post = sc.all_posts_by_key.get(&id);
                        if post.is_some() {
                            if is_cutoff_beyond_7_days {
                                posts.push(post.unwrap());
                            } else {
                                let p = post.unwrap();
                                if p.id > cutoff_tid {
                                    posts.push(p);
                                }
                            }
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
                    let id = at_uri_to_post_id(post["post"].as_str().unwrap())?[5..].to_string();
                    if sc.all_posts_by_id.contains_key(&id) {
                        posts.push(sc.all_posts_by_id.get(&id).unwrap());
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
            } else if input_type == "archived" {
                posts.extend(posts_tmp.get(&block_index).unwrap());
            }
        } else if filter_types.contains(&b_type) {
            let filter = block;
            let filter_type = filter["type"].as_str().unwrap();
            if filter_type == "remove" || filter_type == "keep" {
                let subject = filter["subject"].as_str().unwrap();
                if subject == "item" {
                    let value = if filter.contains_key("value") {
                        filter["value"].as_str().unwrap_or("reply")
                    } else {
                        "reply"
                    };

                    if value == "post" {
                        posts.retain(|p| p.is_reply());
                    } else if value == "reply" {
                        posts.retain(|p| !p.is_reply());
                    } else if value == "repost" {
                    } else if value == "has_labels" {
                        posts.retain(|p| p.labels.is_empty());
                    } else if value == "has_no_labels" {
                        posts.retain(|p| !p.labels.is_empty());
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
                        posts.retain(|p| !p.langs.contains(language));
                    } else if operator == "!=" {
                        posts.retain(|p| p.langs.contains(language));
                    }
                } else if subject == "list" {
                    let dids_vec = fetch_list(block, &state).await?;
                    let mut dids = RoaringBitmap::new();

                    for did in dids_vec {
                        dids.insert(did);
                    }

                    if filter_type == "keep" {
                        posts.retain(|p| dids.contains(p.author_key.into_inner().into()));
                    } else {
                        posts.retain(|p| !dids.contains(p.author_key.into_inner().into()));
                    }
                } else if subject == "duplicates" {
                    let mut seen = RoaringBitmap::new();
                    posts.retain(|p| seen.insert(p.key.into_inner().into()));
                } else if subject == "non_duplicates" {
                    let mut seen = RoaringBitmap::new();
                    posts.retain(|p| !seen.insert(p.key.into_inner().into()));
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
                } else if subject == "labels" {
                    let array = block["labels"].as_array().unwrap();

                    let mut to_remove = RoaringBitmap::new();

                    for label_value in array {
                        let label = label_value.as_str().unwrap().to_lowercase();
                        for id in sc
                            .label_to_posts
                            .get(&label)
                            .unwrap_or(&RoaringBitmap::new())
                        {
                            to_remove.insert(id);
                        }
                    }
                    posts.retain(|p| !to_remove.contains(p.key.into_inner().into()));
                } else if subject == "where" {
                    let script = block["value"].as_str().unwrap();
                    let engine = rhai::Engine::new();
                    let ast = engine.compile(script)?;

                    let invert = filter_type == "remove";

                    posts.retain(|p| {
                        let mut scope = create_rhai_scope_for_post(p);
                        let score = *post_score_global
                            .get(&p.key.into_inner().into())
                            .unwrap_or(&0f64);
                        scope.push_constant("score", score);
                        let result: bool = engine.eval_ast_with_scope(&mut scope, &ast).unwrap();
                        if invert {
                            !result
                        } else {
                            result
                        }
                    });
                } else if subject == "videos" {
                    // ? keep videos
                    // ? remove videos
                    if filter_type == "keep" {
                        posts.retain(|p| p.video.is_some());
                    } else {
                        posts.retain(|p| p.video.is_none());
                    }
                    // TODO target alt video and so on with regex
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
                // ! regex cache impl start
                let payload_map = payload.as_object().unwrap();
                let namespace = if payload_map.contains_key("namespace") {
                    payload_map["namespace"].as_str().unwrap_or("public")
                } else {
                    "public"
                };
                let regex_cache_key = if payload_map.contains_key("regexCacheKey") {
                    payload_map["regexCacheKey"].as_str().unwrap_or("default")
                } else {
                    "default"
                };

                let regex_hash = blake3::hash(
                    format!(
                        "{}/{}/{}/{}/{}",
                        namespace, regex_cache_key, target, invert, regex
                    )
                    .as_bytes(),
                )
                .to_hex()
                .to_string();

                let rc_arc = {
                    let mut map = state.regex_result_cache.write().await;
                    if let Some(x) = map.get(&regex_hash) {
                        Arc::clone(x)
                    } else {
                        let arc = Arc::new(RwLock::new(RegExResultCache {
                            last_processed_id: "0000".to_string(),
                            matches: RoaringBitmap::new(),
                        }));
                        map.insert(regex_hash.clone(), Arc::clone(&arc));
                        arc
                    }
                };

                let mut rc = rc_arc.write().await;

                let mut existing_posts: Vec<&PostV1> = vec![];

                let mut highest_id = &rc.last_processed_id.clone();
                /*   */

                if !rc.matches.is_empty() {
                    for p in &posts {
                        if rc.matches.contains(p.key.into_inner().into()) {
                            existing_posts.push(p);
                        }
                    }

                    posts.retain(|p| &rc.last_processed_id < &p.id);
                }
                for p in &posts {
                    if highest_id < &p.id {
                        highest_id = &p.id;
                    }
                }
                // ! regex cache impl end
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
                        // TODO Match links more efficently
                        posts.retain(|p| !re.is_match(&p.links.concat()));
                        // posts.retain(|p| !re.is_match(&p.links.concat()));
                    } else {
                        posts.retain(|p| re.is_match(&p.links.concat()));
                    }
                } else if target == "text|alt_text" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.text) || re.is_match(&p.alt_text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.text) || re.is_match(&p.alt_text));
                    }
                } else if target == "alt_text|link" {
                    if invert {
                        posts.retain(|p| {
                            !(re.is_match(&p.links.concat()) || re.is_match(&p.alt_text))
                        });
                    } else {
                        posts
                            .retain(|p| re.is_match(&p.links.concat()) || re.is_match(&p.alt_text));
                    }
                } else if target == "text|link" {
                    if invert {
                        posts.retain(|p| !(re.is_match(&p.links.concat()) || re.is_match(&p.text)));
                    } else {
                        posts.retain(|p| re.is_match(&p.links.concat()) || re.is_match(&p.text));
                    }
                } else if target == "text|alt_text|link" {
                    if invert {
                        posts.retain(|p| {
                            !(re.is_match(&p.links.concat())
                                || re.is_match(&p.text)
                                || re.is_match(&p.alt_text))
                        });
                    } else {
                        posts.retain(|p| {
                            re.is_match(&p.links.concat())
                                || re.is_match(&p.text)
                                || re.is_match(&p.alt_text)
                        });
                    }
                }
                // ! regex cache impl start

                for p in &posts {
                    rc.matches.insert(p.key.into_inner().into());
                }

                rc.last_processed_id = highest_id.clone();
                posts.extend(existing_posts);
                // ! regex cache impl end
            }
        } else if b_type == "score" {
            let score_type = block["scoreType"].as_str().unwrap();
            if score_type == "add" {
                let mut post_score: BTreeMap<u32, f64> = BTreeMap::new();

                let normalize = if block.contains_key("normalize") {
                    block["normalize"].as_bool().unwrap_or(true)
                } else {
                    true
                };

                if block.contains_key("value") {
                    let script = block["value"].as_str().unwrap();
                    let engine = rhai::Engine::new();
                    let ast = engine.compile(script)?;

                    for p in &posts {
                        let mut scope = create_rhai_scope_for_post(&p);
                        let score = *post_score_global
                            .get(&p.key.into_inner().into())
                            .unwrap_or(&0f64);
                        scope.push_constant("score", score);
                        let result: f64 = engine.eval_ast_with_scope(&mut scope, &ast).unwrap();
                        post_score.insert(p.ukey(), result);
                    }
                } else {
                    let from = block["from"].as_str().unwrap();
                    if from == "like_count" {
                        for p in &posts {
                            post_score.insert(p.ukey(), p.like_count as f64);
                        }
                    } else if from == "repost_count" {
                        for p in &posts {
                            post_score.insert(p.ukey(), p.repost_count as f64);
                        }
                    } else if from == "reply_count" {
                        for p in &posts {
                            post_score.insert(p.ukey(), p.reply_count as f64);
                        }
                    } else if from == "hn" {
                        let gravity_str = if block.contains_key("gravity") {
                            block["gravity"].as_str().unwrap_or("1.8")
                        } else {
                            "1.8"
                        };

                        let gravity = gravity_str.parse::<f64>()?;
                        for p in &posts {
                            post_score.insert(p.ukey(), p.calculate_score(gravity));
                        }
                    } else if from == "created_at" {
                        for p in &posts {
                            post_score.insert(
                                p.ukey(),
                                (p.created_dt().timestamp_millis() / 1000i64) as f64,
                            );
                        }
                    }
                }

                if normalize {
                    add_post_score_to_global_normalized(
                        &post_score,
                        &mut post_score_global,
                        &block,
                    )?;
                } else {
                    for score in post_score {
                        let id = score.0;
                        let s = score.1;
                        if let Some(score) = post_score_global.get(&id) {
                            post_score_global.insert(id, score + s);
                        } else {
                            post_score_global.insert(id, s);
                        };
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
                // TODO Maybe sort by TID instead (perf)
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

                let mut tuples: Vec<(f64, &PostV1)> = posts
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
            } else if sort_type == "score" {
                // ! this also removes duplicates
                // TODO Make rev() configurable
                let mut post_scores: Vec<_> = post_score_global.iter().rev().collect();
                if direction == "asc" {
                    post_scores.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap());
                } else {
                    post_scores.sort_by(|b, a| a.1.partial_cmp(b.1).unwrap());
                }
                let mut active_posts: BTreeMap<u32, &PostV1> = BTreeMap::new();
                for p in &posts {
                    active_posts.insert(p.ukey(), p);
                }

                posts = vec![];

                for p in post_scores {
                    if let Some(post) = active_posts.get(p.0) {
                        posts.push(post);
                    }
                }
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
            } else if action == "subtract" {
                if !stash.contains_key(key) {
                    return Err(anyhow!(
                        "Stash pop failed because stash with that key does not exist"
                    ));
                }
                let posts_in_stash = stash.get(key).unwrap();

                let mut post_keys_in_stash = RoaringBitmap::new();
                for p in posts_in_stash {
                    post_keys_in_stash.insert(p.ukey());
                }

                posts.retain(|p| !post_keys_in_stash.contains(p.ukey()));
            }
        } else if b_type == "limit" {
            let limit_type = if block.contains_key("limitType") {
                block["limitType"].as_str().unwrap_or("default")
            } else {
                "default"
            };

            if limit_type == "posts_per_user" {
                let count = if block.contains_key("count") {
                    block["count"].as_u64().unwrap_or(3)
                } else {
                    5
                } as u32;

                let mut seen_users: BTreeMap<u32, u32> = BTreeMap::new();
                posts.retain(|p| {
                    if seen_users
                        .get(&p.author_key.into_inner().into())
                        .unwrap_or(&0)
                        >= &count
                    {
                        return false;
                    }
                    seen_users.insert(
                        p.author_key.into_inner().into(),
                        seen_users
                            .get(&p.author_key.into_inner().into())
                            .unwrap_or(&0)
                            + 1,
                    );
                    return true;
                });
            } else {
                let count = if block.contains_key("count") {
                    block["count"].as_i64().unwrap_or(100)
                } else {
                    100
                } as usize;

                if posts.len() > count {
                    posts.drain((count)..);
                }
            }
        } else if b_type == "remember_posts" {
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

            let mut new_posts: Vec<&PostV1> = vec![];

            if target == "parent" {
                for post in posts {
                    if post.parent.is_empty() {
                        if keep_unsuitable_posts {
                            new_posts.push(post);
                        }
                    } else {
                        let replacement = sc.all_posts_by_id.get(&post.parent);
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
                        let replacement = sc.all_posts_by_id.get(&post.root);
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
                        let replacement = sc.all_posts_by_id.get(&post.record[5..]);
                        if replacement.is_some() {
                            new_posts.push(replacement.unwrap());
                        }
                    }
                }
            } else if target == "direct_replies"
                || target == "all_replies"
                || target == "quote_posts"
            {
                let mut targets = RoaringBitmap::new();
                for post in posts {
                    targets.insert(post.ukey());
                    /*  if post.record.is_empty() {
                        if keep_unsuitable_posts {
                            new_posts.push(post);
                        }
                    } else {
                        let replacement = sc.all_posts.get(&post.record[5..]);
                        if replacement.is_some() {
                            new_posts.push(replacement.unwrap());
                        }
                    } */
                }
                posts = vec![];
                let post_id_map = POST_ID_MAP.get().unwrap();

                if target == "direct_replies" {
                    for p in sc.all_posts_by_key.values() {
                        if targets
                            .contains(post_id_map.get_or_intern(&p.parent).into_inner().into())
                        {
                            posts.push(p);
                        }
                    }
                } else if target == "all_replies" {
                    for p in sc.all_posts_by_key.values() {
                        if targets.contains(post_id_map.get_or_intern(&p.root).into_inner().into())
                        {
                            posts.push(p);
                        }
                    }
                } else if target == "quote_posts" {
                    for p in sc.all_posts_by_key.values() {
                        if targets.contains(
                            post_id_map
                                .get_or_intern(&p.record[5..])
                                .into_inner()
                                .into(),
                        ) {
                            posts.push(p);
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
            debug.timing.insert(
                block["id"].as_str().unwrap().to_string(),
                millis + pre_block_timings.get(&block_index).unwrap_or(&0u128),
            );

            debug.counts.insert(
                block["id"].as_str().unwrap().to_string(),
                posts.len() as u128,
            );
        }
        block_index += 1;
    }

    if posts.len() > 500 {
        posts.drain(500..posts.len());
    }

    debug.time = query_start.elapsed().as_millis();

    if debug.time > 10000 {
        println!("slow query {}ms {:?}", debug.time, blocks);
    }

    let payload_map = payload.as_object().unwrap();

    let debug_include_scores = if payload_map.contains_key("debugIncludeScores") {
        payload_map["debugIncludeScores"].as_bool().unwrap_or(false)
    } else {
        false
    };

    if debug_include_scores {
        let mut post_ids = RoaringBitmap::new();
        for p in &posts {
            post_ids.insert(p.ukey());
        }
        let post_id_map = POST_ID_MAP.get().unwrap();
        for e in post_score_global {
            if post_ids.contains(e.0) {
                debug.scores.insert(
                    post_id_map
                        .resolve(&Spur::try_from_usize((e.0 - 1) as usize).unwrap())
                        .to_string(),
                    e.1,
                );
            }
        }
    }

    if LINK_DETECT_ENABLED {
        let qstart = Instant::now();
        let feedkey = if payload_map.contains_key("feedkey") {
            payload_map["feedkey"].as_str().unwrap_or("default")
        } else {
            "default"
        };
        if feedkey != "default" {
            let mut links = state.post_links.write().await;
            if let Some(posts_set) = links.feed_to_posts.get_mut(feedkey) {
                for p in &posts {
                    posts_set.insert(p.id.clone());
                }
            } else {
                let mut posts_set = BTreeSet::new();
                for p in &posts {
                    posts_set.insert(p.id.clone());
                }
                links.feed_to_posts.insert(feedkey.to_string(), posts_set);
            }

            for p in &posts {
                if let Some(feeds) = links.post_to_feeds.get_mut(&p.id) {
                    feeds.insert(feedkey.to_string());
                } else {
                    let mut feeds = HashSet::new();
                    feeds.insert(feedkey.to_string());
                    links.post_to_feeds.insert(p.id.clone(), feeds);
                }
            }
            if qstart.elapsed().as_millis() > 0 {
                println!("link_detect_update {}ms", qstart.elapsed().as_millis());
            }
        }
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

fn create_rhai_scope_for_post(p: &PostV1) -> rhai::Scope {
    // TODO Optimize by only including relevant fields? or using post. getters
    // TODO Use rhai::CustomType
    // scope.push_constant("post", p);

    let mut scope = rhai::Scope::new();
    scope.push_constant("likeCount", p.like_count as i64);
    scope.push_constant("replyCount", p.reply_count as i64);
    scope.push_constant("repostCount", p.repost_count as i64);
    scope.push_constant("quoteCount", p.quote_count as i64);

    // TODO Check perf

    // TODO Languages
    // scope.push_constant("languages", rhai::Array::from(p.langs));
    scope.push_constant("imageCount", p.image_count as i64);
    let lang = p.langs.iter().next().unwrap_or(&"".to_string()).clone();

    scope.push_constant("lang", lang);

    // TODO Maybe don't keep
    scope.push_constant("tsMillis", p.created_dt().timestamp_millis());

    let age = Utc::now().signed_duration_since(p.created_dt());

    scope.push_constant("ageSeconds", age.num_seconds() as i64);

    scope
}

fn add_post_score_to_global_normalized(
    post_score: &BTreeMap<u32, f64>,
    post_score_global: &mut BTreeMap<u32, f64>,
    block: &serde_json::Map<String, Value>,
) -> anyhow::Result<()> {
    let score_normalization_multiplier_str = if block.contains_key("scoreNormalizationMultiplier") {
        block["scoreNormalizationMultiplier"]
            .as_str()
            .unwrap_or("1.0")
    } else {
        "1.0"
    };
    let score_normalization_multiplier = score_normalization_multiplier_str.parse::<f64>()?;

    let mut max_score: f64 = 0f64;
    for score in post_score {
        if score.1 > &max_score {
            max_score = *score.1;
        }
    }
    for score in post_score {
        let id = score.0;
        let s = (score.1 / max_score) * score_normalization_multiplier;
        if let Some(score) = post_score_global.get(id) {
            post_score_global.insert(*id, score + s);
        } else {
            post_score_global.insert(*id, s);
        };
    }

    Ok(())
}

fn extract_cutoff_tid_from_block(
    block: &serde_json::Map<String, Value>,
    default_seconds: i64,
) -> anyhow::Result<String> {
    let history_seconds = if block.contains_key("historySeconds") {
        block["historySeconds"].as_i64().unwrap_or(default_seconds)
    } else {
        default_seconds
    };
    let cutoff_tid = datetime_to_tid(
        Utc::now()
            .checked_sub_signed(chrono::Duration::seconds(history_seconds))
            .unwrap(),
    );
    Ok(cutoff_tid)
}

// TODO Re-implement this
/*
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
                let dids = fetch_list(block, &state).await?;
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
        scores: BTreeMap::new(),
    };
    let sc = state.data.load();
    /*  let sc = {
        if *state.pointer_is_a.read().await {
            state.instance_a.read()
        } else {
            state.instance_b.read()
        }
    }
    .await; */

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
                    let dids_vec = fetch_list(block, &state).await?;
                    let dids: HashSet<String> = HashSet::from_iter(dids_vec);

                    profiles.retain(|p| !dids.contains(&p.id));
                } else if subject == "duplicates" {
                    // TODO Make this more efficient
                    let mut seen: HashSet<String> = HashSetd::new();
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
} */

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
    scores: BTreeMap<String, f64>,
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

async fn fetch_list(
    list_block: &serde_json::Map<String, Value>,
    global_state: &ServerConfigWrapper,
) -> anyhow::Result<Vec<u32>> {
    let list_uri: &str = list_block["listUri"].as_str().unwrap();

    let user_id_map = USER_ID_MAP.get().unwrap();

    if list_uri.starts_with("skygraph://") {
        let u: Vec<&str> = list_uri.split('/').collect();
        let u_hostname = u.get(2).unwrap().to_string();

        let skygraph = global_state.skygraph.read().await;
        if u_hostname == "community" {
            let community_name = u.get(3).unwrap().to_string();
            let community_id = skygraph.community_name_to_id.get(&community_name).unwrap();

            return Ok(skygraph
                .community_to_users
                .get(community_id)
                .unwrap()
                .iter()
                .map(|uid| skygraph.uid_to_did.get(uid).unwrap().clone())
                .map(|did| did_to_key(&did, false).unwrap())
                .map(|did| user_id_map.get_or_intern(did).into_inner().into())
                .collect::<Vec<u32>>());
        } else {
            // let did_key = did_to_key(&u_hostname, true).unwrap();
            let u_type = u.get(3).unwrap().to_string();

            let uid = skygraph.did_to_uid.get(&u_hostname).unwrap();
            if u_type == "topMoots" {
                return Ok(skygraph
                    .moots
                    .get(uid)
                    .unwrap()
                    .iter()
                    .map(|moot| skygraph.uid_to_did.get(moot.0).unwrap().clone())
                    .map(|did| did_to_key(&did, false).unwrap())
                    .map(|did| user_id_map.get_or_intern(did).into_inner().into())
                    .collect::<Vec<u32>>());
            }
        }
    }
    // TODO List of users whose posts I liked

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
        .map(|did| did.as_str().unwrap()[4..].to_string())
        .map(|did| user_id_map.get_or_intern(did).into_inner().into())
        .collect::<Vec<u32>>())
}


async fn run_posts_query(
    sc: &mut ServerConfigInstance,
    timeout: Duration,
    with_counts: bool,
) -> anyhow::Result<()> {
    let last_post_id = sc.last_post_id.clone();
    println!("run_query {}", last_post_id);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let max_allowed_post_id = datetime_to_tid(
        Utc::now()
            .checked_add_signed(chrono::Duration::minutes(10))
            .unwrap(),
    );

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(if with_counts {format!(
            "SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,mentions,video,bridgyOriginalUrl,via,array::first((SELECT c FROM type::thing('like_count_view', [$parent.id])).c) as likeCount,array::first((SELECT c FROM type::thing('reply_count_view', [$parent.id])).c) as replyCount,array::first((SELECT c FROM type::thing('repost_count_view', [$parent.id])).c) as repostCount FROM {}..{} LIMIT 1000000;",
            last_post_id,max_allowed_post_id
        )}else {format!(
            "SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,mentions,video,bridgyOriginalUrl,via FROM {}..{} LIMIT 1000000;",
            last_post_id, max_allowed_post_id
        )})
        .timeout(timeout);

    let res = request_builder.send().await?;

    println!("run_query_2 {}", last_post_id);

    // TODO use slice directly with simd-json
    // let mut res_bytes = &mut res.bytes().await?;
    // let list: Vec<Value> = simd_json::serde::from_slice(res_bytes)?;
    // !works let list: Vec<Value> = simd_json::serde::from_reader(&res.bytes().await?[..])?;
    let list: Vec<Value> = res.json().await?;
    /* let list: Vec<Value> = {
        let mut b = (res.bytes().await?).as_mut();
        simd_json::serde::from_slice(b)?
    }; */

    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    {
        println!("run_query_2.5 {}", last_post_id);
        println!("run_query_3 {}", last_post_id);

        if list2.len() > 1000 {
            let id = list2.get(list2.len() - 1000).unwrap().as_object().unwrap()["id"]
                .as_str()
                .unwrap();
            sc.last_post_id = id.to_string();
        }

        println!("run_query fetched {} new posts", list2.len());

        for p in list2 {
            let post = p.as_object().unwrap();
            let id = post["id"].as_str().unwrap()[5..].to_string();
            if sc.all_posts_by_id.contains_key(&id) {
                continue;
            }
            let new_post_res = process_post(post);
            if new_post_res.is_err() {
                println!("PPERR could not process post {}", id);
                continue;
            }
            let arc = Arc::new(new_post_res?);
            let key = arc.ukey();
            let author_key: u32 = arc.author_key.into_inner().into();
            sc.all_posts_by_id.insert(id.clone(), arc.clone());
            sc.all_posts_by_key.insert(key, arc);

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
                        tag_set.insert(key);
                    } else {
                        let mut tag_set = RoaringBitmap::new();
                        tag_set.insert(key);
                        sc.all_posts_by_tag.insert(tag.clone(), tag_set);
                    }
                }
            }

            if post.contains_key("labels") && !post["labels"].is_null() {
                let array = post["labels"].as_array().unwrap();

                for label_value in array {
                    let label = format!("self/{}", label_value.as_str().unwrap());

                    if let Some(label_set) = sc.label_to_posts.get_mut(&label) {
                        label_set.insert(key);
                    } else {
                        let mut label_set = RoaringBitmap::new();
                        label_set.insert(key);
                        sc.label_to_posts.insert(label.clone(), label_set);
                    }
                }
            }

            if let Some(author_set) = sc.all_posts_by_author.get_mut(&author_key) {
                author_set.insert(key);
            } else {
                let mut author_set = RoaringBitmap::new();
                author_set.insert(key);
                sc.all_posts_by_author.insert(author_key, author_set);
            }
        }

        println!("run_query done {}", last_post_id);
        Ok(())
    }
}

async fn run_likes_query(sc: &mut ServerConfigInstance, timeout: Duration) -> anyhow::Result<()> {
    let last_like_id = sc.last_like_id.clone();
    println!("run_likes_query {}", last_like_id);
    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let max_allowed_like_id = datetime_to_tid(
        Utc::now()
            .checked_add_signed(chrono::Duration::minutes(10))
            .unwrap(),
    );

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(format!(
            "SELECT id,in,out FROM {}..{} LIMIT 10000000;",
            last_like_id, max_allowed_like_id,
        ))
        .timeout(timeout);

    let res = request_builder.send().await?;

    println!("run_likes_query_2 {}", last_like_id);

    let list: Vec<Value> = res.json().await?;

    let list2 = list.last().unwrap()["result"].as_array().unwrap();

    {
        if list2.len() > 1000 {
            let id = list2.get(list2.len() - 1000).unwrap().as_object().unwrap()["id"]
                .as_str()
                .unwrap();
            sc.last_like_id = id.to_string();
        }

        println!("run_likes_query fetched {} new likes", list2.len());
        let post_id_map = POST_ID_MAP.get().unwrap();
        let user_id_map = USER_ID_MAP.get().unwrap();
        // let key = post_id_map.get_or_intern(&id);

        // let key = user_id_map.get_or_intern(&id);

        for p in list2 {
            let like = p.as_object().unwrap();
            // let in_did = like["in"].as_str().unwrap()[4..].to_string();
            let in_did_str = &like["in"].as_str().unwrap()[4..];
            let out_post_id_str = &like["out"].as_str().unwrap()[5..];

            let in_user_key: u32 = user_id_map.get_or_intern(in_did_str).into_inner().into();
            let out_post_key: u32 = post_id_map
                .get_or_intern(out_post_id_str)
                .into_inner()
                .into();

            if let Some(post_set) = sc.likes_user_to_posts.get_mut(&in_user_key) {
                post_set.insert(out_post_key);
            } else {
                let mut post_set = RoaringBitmap::new();
                post_set.insert(out_post_key);
                sc.likes_user_to_posts.insert(in_user_key, post_set);
            }

            if let Some(user_set) = sc.likes_post_to_users.get_mut(&out_post_key) {
                user_set.insert(in_user_key);
            } else {
                let mut user_set = RoaringBitmap::new();
                user_set.insert(in_user_key);
                sc.likes_post_to_users.insert(out_post_key, user_set);
            }
        }

        println!("run_likes_query done {}", last_like_id);
        Ok(())
    }
}

async fn run_profiles_query(
    sc: &mut ServerConfigInstance,
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
        .body(
            "
SELECT * FROM did;
SELECT c,array::first(meta::id(id)) as id FROM follower_count_view;
SELECT c,array::first(meta::id(id)) as id FROM following_count_view;
    ",
        )
        .timeout(timeout);

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    println!("run_profiles_query_2");

    let list2 = list.get(0).unwrap()["result"].as_array().unwrap();

    let follower_counts = list.get(1).unwrap()["result"].as_array().unwrap();
    let following_counts = list.get(2).unwrap()["result"].as_array().unwrap();
    /*     let repost_counts = list.get(1).unwrap()["result"].as_array().unwrap();
    let like_counts = list.get(2).unwrap()["result"].as_array().unwrap(); */

    {
        println!("run_profiles_query_2.5 {}", list2.len());
        // let mut sc = sc_mutex.write().await;
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
        println!("run_profiles_query_3");

        for user in follower_counts {
            let id = user["id"].as_str().unwrap();
            let follower_count = user["c"].as_i64().unwrap();
            if sc.all_profiles.contains_key(id) {
                sc.all_profiles.get_mut(id).unwrap().follower_count = follower_count.try_into()?;
            }
        }
        println!("run_profiles_query_4");
        for user in following_counts {
            let id = user["id"].as_str().unwrap();
            let following_count = user["c"].as_i64().unwrap();
            if sc.all_profiles.contains_key(id) {
                sc.all_profiles.get_mut(id).unwrap().following_count =
                    following_count.try_into()?;
            }
        }

        println!("run_profiles_query done");
        Ok(())
    }
}

fn process_post(post: &serde_json::Map<String, Value>) -> anyhow::Result<PostV1> {
    let id_res = post["id"].as_str();
    if id_res.is_none() {
        return Err(anyhow!("Failed to process_post"));
    }
    let id = id_res.unwrap()[5..].to_string();
    //println!("{} id", id);

    let author = post["author"].as_str().unwrap()[4..].to_string();
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
    /*  let lang: HashSet<String> = if post.contains_key("langs") && !post["langs"].is_null() {
        let array = post["langs"].as_array().unwrap();

        if array.is_none() {
            HashSet::from(vec!["en".to_string()])
        } else {
            array.unwrap().as_str().unwrap().to_string()
        }
    } else {
        "en".to_string()
    }; */
    let langs: Vec<String> = if post.contains_key("langs") && !post["langs"].is_null() {
        post["langs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap().to_string())
            .collect::<Vec<String>>()
    } else {
        vec![]
    };

    let tags: Vec<String> = if post.contains_key("tags") && !post["tags"].is_null() {
        post["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap().to_string())
            .collect::<Vec<String>>()
    } else {
        vec![]
    };

    let mentions: Vec<String> = if post.contains_key("mentions") && !post["mentions"].is_null() {
        post["mentions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap().to_string())
            .collect::<Vec<String>>()
    } else {
        vec![]
    };

    let labels: Vec<String> = if post.contains_key("labels") && !post["labels"].is_null() {
        post["labels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap().to_string())
            .collect::<Vec<String>>()
    } else {
        vec![]
    };

    let links: Vec<String> = if post.contains_key("links") && !post["links"].is_null() {
        post["links"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tag| tag.as_str().unwrap().to_string())
            .collect::<Vec<String>>()
    } else {
        vec![]
    };

    let image_count: u8;

    let alt_text: String = if post.contains_key("images") && !post["images"].is_null() {
        let images = post["images"].as_array().unwrap();
        image_count = images.len() as u8;
        images
            .iter()
            .map(|i| i["alt"].as_str().unwrap())
            .collect::<Vec<&str>>()
            .join("\n\n<[{next-image}]>\n\n")
    } else {
        image_count = 0;
        "".to_string()
    };

    /*     let link: String = if post.contains_key("links") && !post["links"].is_null() {
        let links = post["links"].as_array().unwrap();

        links
            .iter()
            .map(|i| i.as_str().unwrap())
            .collect::<Vec<&str>>()
            .join("\n")
    } else {
        "".to_string()
    }; */
    let record: String = if post.contains_key("record") && !post["record"].is_null() {
        post["record"].as_str().unwrap().to_string()
    } else {
        "".to_string()
    };
    let root: String = if post.contains_key("root") && !post["root"].is_null() {
        post["root"].as_str().unwrap()[5..].to_string()
    } else {
        "".to_string()
    };
    let parent: String = if post.contains_key("parent") && !post["parent"].is_null() {
        post["parent"].as_str().unwrap()[5..].to_string()
    } else {
        "".to_string()
    };

    let via: Option<String> = if post.contains_key("via") && !post["via"].is_null() {
        Some(post["via"].as_str().unwrap().to_string())
    } else {
        None
    };

    let bridgy_original_url: Option<String> =
        if post.contains_key("bridgyOriginalUrl") && !post["bridgyOriginalUrl"].is_null() {
            Some(post["bridgyOriginalUrl"].as_str().unwrap().to_string())
        } else {
            None
        };

    let video = if post.contains_key("video") && !post["video"].is_null() {
        let vo = post["video"].as_object().unwrap();
        let mut captions_text = "".to_string();

        if vo.contains_key("captions") && !vo["captions"].is_null() {
            for value in vo["captions"].as_array().unwrap() {
                let caption = value.as_object().unwrap();
                let text = if caption.contains_key("text") && !caption["text"].is_null() {
                    caption["text"].as_str().unwrap()
                } else {
                    ""
                };

                captions_text = format!(
                    "{}CAPTION_FILE_TEXT_CONTENT_SKYFEED_QUERY_ENGINE\n{}\n",
                    captions_text, text
                );
            }
        };

        let v = PostV1Video {
            height: vo["aspectRatio"]["height"]
                .as_i64()
                .unwrap_or(0)
                .try_into()?,
            width: vo["aspectRatio"]["width"]
                .as_i64()
                .unwrap_or(0)
                .try_into()?,
            media_type: vo["blob"]["mediaType"].as_str().unwrap_or("").to_string(),
            size: vo["blob"]["size"].as_i64().unwrap_or(0).try_into()?,
            alt: if vo.contains_key("alt") && !vo["alt"].is_null() {
                vo["alt"].as_str().unwrap().to_string()
            } else {
                "".to_string()
            },
            captions_text,
        };

        Some(v)
    } else {
        None
    };

    let post_id_map = POST_ID_MAP.get().unwrap();
    let key = post_id_map.get_or_intern(&id);

    let user_id_map = USER_ID_MAP.get().unwrap();
    let user_key = user_id_map.get_or_intern(&author);

    let new_post = PostV1 {
        id: id.clone(),
        key: key,
        text: post["text"].as_str().unwrap().to_string(),
        record,
        alt_text,
        links: links,
        via: via,
        video: video,
        author: author.clone(),
        author_key: user_key,
        mentions: HashSet::from_iter(mentions),
        langs: HashSet::from_iter(langs),
        tags: HashSet::from_iter(tags),
        labels: HashSet::from_iter(labels),
        quote_count: 0, // TODO Get proper quote count
        bridgy_original_url: bridgy_original_url,
        created_at: post["createdAt"].as_str().unwrap().to_string(),
        /*  created_at: DateTime::parse_from_rfc3339(post["createdAt"].as_str().unwrap())
        .unwrap_or(DateTime::default())
        .into(), */
        image_count,
        /* is_hellthread: !post["root"].is_null()
        && post["root"].as_str().unwrap() == "post:3juzlwllznd24_plc_wgaezxqi2spqm3mhrb5xvkzi", */
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
        // has_labels: !post["labels"].is_null(),
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
        follower_count: 0,
        following_count: 0,
    };

    Ok(new_profile)
}


static SINGLE_POST_QUERY: &str =
    "SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,mentions,video,bridgyOriginalUrl,via FROM POSTID;";

async fn fetch_post(id: &str) -> anyhow::Result<PostV1> {
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
        let res = process_post(p.as_object().unwrap());
        if res.is_err() {
            println!("PPERR id3 {}", id);
        }
        return Ok(res?);
    }
    Err(anyhow!("Could not fetch post {}", id))
}


static USER_POSTS_QUERY_TEMPLATE_NO_COUNTS: &str ="LET $posts = array::flatten((SELECT ->COLLECTION_TYPE.out as ids FROM USER_ID).ids); SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,mentions,video,bridgyOriginalUrl,via FROM $posts;";
static USER_POSTS_QUERY_TEMPLATE_WITH_COUNTS: &str ="LET $posts = array::flatten((SELECT ->COLLECTION_TYPE.out as ids FROM USER_ID).ids); SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,mentions,video,bridgyOriginalUrl,via,array::first((SELECT c FROM type::thing('like_count_view', [$parent.id])).c) as likeCount,array::first((SELECT c FROM type::thing('reply_count_view', [$parent.id])).c) as replyCount,array::first((SELECT c FROM type::thing('repost_count_view', [$parent.id])).c) as repostCount FROM $posts;";
static USER_LIKED_POSTS_QUERY_TEMPLATE: &str ="LET $posts = array::flatten((SELECT ->like.out as ids FROM USER_ID).ids); SELECT id,text,author,langs,tags,record,labels,createdAt,images,links,root,parent,mentions,video,bridgyOriginalUrl,via FROM $posts WHERE meta::tb(id) == 'post';";

async fn fetch_user_posts(
    did: &str,
    collection: &str,
    with_counts: bool,
) -> anyhow::Result<Vec<PostV1>> {
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

    let mut posts: Vec<PostV1> = vec![];

    for p in list2 {
        let res = process_post(p.as_object().unwrap());
        if res.is_err() {
            println!("PPERR {} {} {}", did, collection, with_counts);
        }
        posts.push(res.unwrap());
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
    anchor_tid: &str,
    sc: &mut ServerConfigInstance,
    limit: bool,
) -> anyhow::Result<()> {
    println!("run_update_counts_query");

    let anchor = format!("post:{}", anchor_tid);

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
                .replace(
                    "[VAR_ANCHOR]..",
                    if limit {
                        "[VAR_ANCHOR].. LIMIT 100000"
                    } else {
                        "[VAR_ANCHOR].."
                    },
                )
                .replace("VAR_ANCHOR", &anchor),
        )
        .timeout(Duration::from_secs(60 * 10));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    let reply_counts = list.get(0).unwrap()["result"].as_array().unwrap();
    let repost_counts = list.get(1).unwrap()["result"].as_array().unwrap();
    let like_counts = list.get(2).unwrap()["result"].as_array().unwrap();

    println!("run_update_counts_query2");


    println!("run_update_counts_query3");

    let mut reply_count_map: BTreeMap<u32, u32> = BTreeMap::new();
    let mut repost_count_map: BTreeMap<u32, u32> = BTreeMap::new();
    let mut like_count_map: BTreeMap<u32, u32> = BTreeMap::new();

    let post_id_map = POST_ID_MAP.get().unwrap();

    for post in reply_counts {
        let id = extract_count_post_id(post);
        let reply_count = post["c"].as_i64().unwrap();

        reply_count_map.insert(
            post_id_map.get_or_intern(id).into_inner().into(),
            reply_count.try_into()?,
        );
    }

    for post in repost_counts {
        let id = extract_count_post_id(post);
        let repost_count = post["c"].as_i64().unwrap();
        repost_count_map.insert(
            post_id_map.get_or_intern(id).into_inner().into(),
            repost_count.try_into()?,
        );
    }

    for post in like_counts {
        let id = extract_count_post_id(post);
        let like_count = post["c"].as_i64().unwrap();
        like_count_map.insert(
            post_id_map.get_or_intern(id).into_inner().into(),
            like_count.try_into()?,
        );
    }
    let mut all_keys: BTreeSet<u32> = BTreeSet::new();
    all_keys.extend(reply_count_map.keys());
    all_keys.extend(repost_count_map.keys());
    all_keys.extend(like_count_map.keys());

    for key in all_keys {
        if sc.all_posts_by_key.contains_key(&key) {
            let mut post = PostV1::clone(sc.all_posts_by_key.get(&key).unwrap());

            if let Some(count) = like_count_map.get(&key) {
                post.like_count = *count;
            }

            if let Some(count) = reply_count_map.get(&key) {
                post.reply_count = *count;
            }

            if let Some(count) = repost_count_map.get(&key) {
                post.repost_count = *count;
            }

            let arc = Arc::new(post);
            sc.all_posts_by_id.insert(arc.id.clone(), arc.clone());
            sc.all_posts_by_key.insert(key, arc);
        }
    }
    println!("run_update_counts_query done");
    Ok(())
}

static LABELS_QUERY: &str = "SELECT * from VAR_ANCHOR..pp;";

async fn run_update_labels_query(
    anchor_tid: &str,
    sc: &mut ServerConfigInstance,
) -> anyhow::Result<()> {
    let anchor = format!("label:post_{}", anchor_tid);
    println!("run_update_labels_query {}", anchor);

    let client = Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("accept", "application/json".parse().unwrap());
    headers.insert("NS", HEADER_NAMESPACE.parse().unwrap());
    headers.insert("DB", "bsky".parse().unwrap());
    headers.insert("Authorization", get_surreal_auth_header().parse().unwrap());

    let request_builder = client
        .post(get_surreal_api_url())
        .headers(headers)
        .body(LABELS_QUERY.replace("VAR_ANCHOR", &anchor))
        .timeout(Duration::from_secs(60 * 1));

    let res = request_builder.send().await?;

    let list: Vec<Value> = res.json().await?;

    let labels = list.get(0).unwrap()["result"].as_array().unwrap();

    println!("run_update_labels_query count {}", labels.len());

    let post_id_map = POST_ID_MAP.get().unwrap();

    for label_val in labels {
        let label = label_val.as_object().unwrap();
        let neg = if label.contains_key("neg") {
            label["neg"].as_bool().unwrap_or(false)
        } else {
            false
        };
        let label_key = format!(
            "did:{}/{}",
            label["id"]
                .as_str()
                .unwrap()
                .split("__")
                .last()
                .unwrap()
                .replace("_", ":"),
            label["val"].as_str().unwrap(),
        );

        // let label =
        let post_id = label["uri"].as_str().unwrap()[5..].to_string();
        let post_key = post_id_map.get_or_intern(post_id).into_inner().into();

        if neg {
            if sc.label_to_posts.contains_key(&label_key) {
                sc.label_to_posts
                    .get_mut(&label_key)
                    .unwrap()
                    .remove(post_key);
            }
        } else {
            if let Some(label_set) = sc.label_to_posts.get_mut(&label_key) {
                label_set.insert(post_key);
            } else {
                let mut label_set = RoaringBitmap::new();
                label_set.insert(post_key);
                sc.label_to_posts.insert(label_key, label_set);
            }
        }

        /* let id = extract_count_post_id(post);
        let reply_count = post["c"].as_i64().unwrap();
        if sc.all_posts.contains_key(id) {
            sc.all_posts.get_mut(id).unwrap().reply_count = reply_count.try_into()?;
        } */
    }

    Ok(())
}
/*
// TODO Test if this works locally
async fn clean_up_old_posts(sc_mutex: &ServerConfigWrapper) -> anyhow::Result<()> {
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

fn extract_count_post_id(val: &Value) -> &str {
    return val["id"]
        .as_str()
        .unwrap()
        .split(":[post:")
        .last()
        .unwrap()
        .strip_suffix("]")
        .unwrap();
}

/* static LABELS_QUERY: &str = "SELECT in FROM post_labels;";

async fn run_update_labels_query(mutex: &ServerConfigWrapper) -> anyhow::Result<()> {
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

    let did_method = parts[1];

    if did_method == "web" {
        format!(
            "at://did:{}:{}/app.bsky.feed.post/{}",
            did_method,
            parts[2].replace("__", "-").replace("_", "."),
            parts[0]
        )
    } else {
        format!(
            "at://did:{}:{}/app.bsky.feed.post/{}",
            did_method, parts[2], parts[0]
        )
    }
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
lazy_static::lazy_static! {
    static ref BASE_32_SORT: data_encoding::Encoding = {
        let mut spec = data_encoding::Specification::new();
        spec.symbols.push_str("234567abcdefghijklmnopqrstuvwxyz");
        spec.encoding().unwrap()
    };
}

fn datetime_to_tid(dt: DateTime<Utc>) -> String {
    let ts_microseconds = dt.timestamp_micros() as u64;
    let moved_some_bits = ts_microseconds << 9;
    BASE_32_SORT.encode(&moved_some_bits.to_be_bytes())
}

fn clone_archived_post_to_post(a: &ArchivedPostV1) -> PostV1 {
    rkyv::deserialize::<PostV1, rkyv::rancor::Error>(a).unwrap()
}

#[derive(Clone, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
struct PostV1ArchiveQueryCache {
    anchor: String,
    posts: Vec<PostV1>,
}

// Serialize, Deserialize,
// TODO add threadgate and postgate fields (or store extra)
#[derive(Clone, rkyv::Archive, rkyv::Deserialize, rkyv::Serialize)]
// #[archive(check_bytes)]
struct PostV1 {
    // TODO I *really* want these fields out of here!
    reply_count: u32,
    repost_count: u32,
    like_count: u32,
    quote_count: u32,

    image_count: u8,
    // This must stay a string, even if the from->to becomes a u32
    id: String,

    #[rkyv(with = rkyv::with::Skip)]
    key: Spur,
    #[rkyv(with = rkyv::with::Skip)]
    author_key: Spur,

    // TODO Make sure leading did: is no longer in use
    author: String, // plc_example123123123
    text: String,
    alt_text: String,
    tags: HashSet<String>,

    // link: String,
    links: Vec<String>,

    langs: HashSet<String>,

    parent: String,
    root: String,

    record: String,

    labels: HashSet<String>,
    mentions: HashSet<String>,

    created_at: String,
    video: Option<PostV1Video>,

    via: Option<String>,
    bridgy_original_url: Option<String>,
    // created_at_dt: DateTime<Utc>,
}

#[derive(Clone, Archive, rkyv::Deserialize, rkyv::Serialize)]
struct PostV1Video {
    height: u16,
    width: u16,
    size: u64,

    alt: String,
    media_type: String,
    captions_text: String,
    // cid: String,
}

#[derive(Clone)]
struct Profile {
    id: String,
    name: String,
    handle: String,
    description: String,

    has_avatar: bool,
    has_banner: bool,

    follower_count: u32,
    following_count: u32,
    // TODO Use SkyFeed input for list?
    // TODO languages (posts)
    // TODO hashtags (posts)

    // TODO Post count
    // TODO Repost count
    // TODO Reply count
    // TODO Like count
}

impl PostV1 {
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

    fn is_reply(&self) -> bool {
        !&self.parent.is_empty()
    }

    fn ukey(&self) -> u32 {
        self.key.into_inner().into()
    }

    fn created_dt(&self) -> DateTime<Utc> {
        return DateTime::parse_from_rfc3339(&self.created_at)
            .unwrap_or(DateTime::default())
            .into();
    }

    fn calculate_score(&self, gravity: f64) -> f64 {
        let diff_hours = Utc::now()
            .signed_duration_since(self.created_dt())
            .num_minutes()
            .abs() as f64
            / 60.0
            + 2.0;

        self.like_count as f64 / diff_hours.powf(gravity)
    }
}
