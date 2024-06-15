use std::{collections::HashMap, str::FromStr};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use url::Url;

use crate::cache::Updater;

use super::{Series, TimeSelection};

#[derive(Serialize)]
struct PrometheusQuery {
  query: String,
  #[serde(with = "time::serde::rfc3339")]
  start: OffsetDateTime,
  #[serde(with = "time::serde::rfc3339")]
  end: OffsetDateTime,
  step: f64,
}

#[derive(Deserialize)]
struct PrometheusResponse {
  data: PrometheusData,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrometheusData {
  result: Vec<PrometheusMetrics>,
}

#[derive(Deserialize)]
struct PrometheusMetrics {
  metric: PrometheusMetric,
  values: Vec<(f64, String)>,
}

#[derive(Deserialize)]
struct PrometheusMetric {
  r#type: String,
}

pub(super) struct As112Updater {
  client: Client,
  prometheus_url: Url,
  selection: Duration,
}

#[async_trait::async_trait]
impl Updater for As112Updater {
  type Output = Series<HashMap<String, Vec<(f64, f64)>>>;
  type Error = anyhow::Error;

  async fn update(&self) -> Result<Self::Output, Self::Error> {
    let now = OffsetDateTime::now_utc();

    let data = self
      .query_stats(OffsetDateTime::now_utc() - self.selection, now, 255.0)
      .await?;

    Ok(data)
  }
}
impl As112Updater {
  pub(super) fn new(client: Client, prometheus_url: Url, selection: TimeSelection) -> Self {
    Self {
      client,
      prometheus_url,
      selection: selection.into(),
    }
  }

  async fn query_stats(
    &self,
    start: OffsetDateTime,
    end: OffsetDateTime,
    points: f64,
  ) -> anyhow::Result<Series<HashMap<String, Vec<(f64, f64)>>>> {
    let query = PrometheusQuery {
      query: "sum by (type) (rate(knot_query_type_total[5m])) >= 0.01".to_string(),
      start,
      end,
      step: ((end - start) / points).as_seconds_f64(),
    };

    Ok(Series {
      start,
      end,
      data: self
        .client
        .get(self.prometheus_url.join("/api/v1/query_range")?)
        .query(&query)
        .send()
        .await?
        .error_for_status()?
        .json::<PrometheusResponse>()
        .await?
        .data
        .result
        .into_iter()
        .map(|series| {
          (
            series.metric.r#type,
            series
              .values
              .into_iter()
              .map(|(time, value)| (time, f64::from_str(&value).unwrap()))
              .collect(),
          )
        })
        .collect(),
    })
  }
}
