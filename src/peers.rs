use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::sync::RwLock;

use url::Url;

// https://github.com/euro-ix/json-schemas/wiki/Schema-Field-Entries-Members#schema-field-entries---members

const MAX_AGE: Duration = Duration::hours(1);

#[derive(Deserialize, Clone)]
enum EuroIXMemberType {
  #[serde(rename = "peering")]
  Peering,
  #[serde(rename = "ixp")]
  IXP,
  #[serde(rename = "other")]
  Other,
}

#[derive(Deserialize, Serialize, Clone)]
enum PeeringPolicy {
  #[serde(rename = "open")]
  Open,
  #[serde(rename = "selective")]
  Selective,
  #[serde(rename = "case-by-case")]
  CaseByCase,
  #[serde(rename = "mandatory")]
  Mandatory,
  #[serde(rename = "not-a-peer")]
  NotPeering,
}

#[derive(Deserialize, Clone)]
struct EuroIXIfList {
  if_speed: u64,
}

#[derive(Deserialize, Clone)]
struct EuroIXConnection {
  if_list: Vec<EuroIXIfList>,
}

#[derive(Deserialize, Clone)]
struct EuroIXMemberScheme {
  asnum: i32,
  member_type: EuroIXMemberType,
  name: String,
  url: Url,
  peering_policy: PeeringPolicy,
  connection_list: Vec<EuroIXConnection>,
}

#[derive(Deserialize, Clone)]
struct EuroIXApiScheme {
  member_list: Vec<EuroIXMemberScheme>,
}

#[derive(Deserialize, Clone)]
struct Supporter {
  name: String,
  url: Url,
}

#[derive(Deserialize, Clone)]
struct StaticSupporterInformation {
  supporters: Vec<Supporter>,
  supporting_peers: Vec<i32>,
}

#[derive(Serialize, Clone)]
pub(crate) struct FoundationEntity {
  is_peer: bool,
  is_supporter: bool,
  asn: Option<i32>,
  name: String,
  url: Url,
  peering_policy: PeeringPolicy,
  speed: Vec<(u64, u64)>,
}

#[derive(Clone)]
pub(crate) struct NetworkService {
  ixp_manager_url: Arc<Url>,
  client: Client,
  updating: Arc<AtomicBool>,
  yaml_file: Arc<StaticSupporterInformation>,
  cached: Arc<RwLock<(OffsetDateTime, Vec<FoundationEntity>)>>,
}
impl NetworkService {
  pub(crate) async fn new(base_path: &PathBuf, ixp_manager_url: Url) -> anyhow::Result<Self> {
    let serialized_supporter = tokio::fs::read_to_string(base_path.join("supporter.yaml")).await?;
    let yaml_file = serde_yaml::from_str(&serialized_supporter)?;

    Ok(Self {
      ixp_manager_url: Arc::new(ixp_manager_url),
      client: Client::new(),
      updating: Arc::new(AtomicBool::new(false)),
      yaml_file: Arc::new(yaml_file),
      cached: Arc::new(RwLock::new((
        OffsetDateTime::now_utc() - MAX_AGE,
        Vec::new(),
      ))),
    })
  }
  pub(crate) async fn get_stats(&self) -> anyhow::Result<Vec<FoundationEntity>> {
    let now = OffsetDateTime::now_utc();

    while self.updating.load(Ordering::Relaxed) {
      tokio::time::sleep(std::time::Duration::from_millis(500)).await
    }

    let lock = self.cached.read().await;
    if let (next_update, stats) = lock.deref() {
      if next_update < &now {
        self.updating.store(true, Ordering::Relaxed);
      } else {
        return Ok(stats.clone());
      }
    }

    let new_values = self.fetch_values().await?;

    *self.cached.write().await = (now + MAX_AGE, new_values.clone());
    self.updating.store(false, Ordering::Relaxed);

    Ok(new_values)
  }

  async fn fetch_values(&self) -> anyhow::Result<Vec<FoundationEntity>> {
    let api_result: EuroIXApiScheme = self
      .client
      .get(self.ixp_manager_url.join("/api/v4/member-export/ixf/0.6")?)
      //.query(&query)
      .send()
      .await?
      .error_for_status()?
      .json::<crate::peers::EuroIXApiScheme>()
      .await?;

    let mut peers: Vec<FoundationEntity> = api_result
      .member_list
      .into_iter()
      .map(|value| {
        let is_supporter = self.yaml_file.supporting_peers.contains(&value.asnum);

        let mut speeds: HashMap<u64, u64> = HashMap::new();

        for connection_list in value.connection_list {
          for if_list in connection_list.if_list {
            speeds
              .entry(if_list.if_speed)
              .and_modify(|count| *count += 1)
              .or_insert(1);
          }
        }

        let speed_list: Vec<(u64, u64)> = speeds
          .into_iter()
          .filter_map(|(key, value)| Some((key, value)))
          .collect();

        FoundationEntity {
          is_peer: true,
          is_supporter,
          asn: Some(value.asnum),
          name: value.name,
          url: value.url,
          peering_policy: PeeringPolicy::Open,
          speed: speed_list,
        }
      })
      .collect();

    let mut supporters: Vec<FoundationEntity> = self
      .yaml_file
      .supporters
      .iter()
      .map(|value| FoundationEntity {
        is_peer: false,
        is_supporter: true,
        asn: None,
        name: value.name.clone(),
        url: value.url.clone(),
        peering_policy: PeeringPolicy::NotPeering,
        speed: Vec::new(),
      })
      .collect();

    peers.append(&mut supporters);

    peers.sort_by_key(|x| x.name.clone());

    Ok(peers)
  }
}
