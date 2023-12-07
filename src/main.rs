extern crate logwatcher;

use dotenv::dotenv;
use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;
use logwatcher::{LogWatcher, LogWatcherAction};
use reqwest::{self};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;
use std::error::Error;
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc;
use std::{
    env,
    time::{self, SystemTime, UNIX_EPOCH},
};
use sysinfo::{CpuExt, CpuRefreshKind, NetworkExt, RefreshKind, System, SystemExt};
use tokio;

#[derive(Debug, Deserialize, Clone)]
struct LocalConfig {
    influxdb_url: String,
    influxdb_token: String,
    influxdb_org: String,
    influxdb_bucket: String,
    hostname: String,
}

impl LocalConfig {
  fn from_env() -> Result<Self, env::VarError> {
      Ok(Self {
          influxdb_url: env::var("INFLUXDB_URL")?,
          influxdb_token: env::var("INFLUXDB_TOKEN")?,
          influxdb_org: env::var("INFLUXDB_ORG")?,
          influxdb_bucket: env::var("INFLUXDB_BUCKET")?,
          hostname: env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string()),
      })
  }
}

async fn send_data_to_influxdb(client: &Client, config: &LocalConfig, data: String) -> Result<(), Box<dyn Error + Send + Sync>> {

  let response = client
      .post(format!(
          "{}/api/v2/write?org={}&bucket={}&precision=ns",
          config.influxdb_url, config.influxdb_org, config.influxdb_bucket
      ))
      .header("Authorization", format!("Token {}", config.influxdb_token))
      .body(data)
      .send()
      .await;

      match response {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                log::info!("Successfully sent data to InfluxDB.");
            } else {
                let error_body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to get error body".to_string());
                log::error!(
                    "Failed to send data to InfluxDB: Status {}. Error: {}",
                    status, error_body
                );
            }
        }
        Err(e) => {
            log::error!("Error sending data to InfluxDB: {}", e);
        }
    }

  Ok(())
}

async fn watch_log_file(path: String, client: Client, config: LocalConfig, batch_size: usize) -> Result<(), Box<dyn Error + Send + Sync>> {

  let (tx, mut rx) = mpsc::channel::<String>(100); // Adjust the buffer size as needed

  tokio::spawn(async move {
      let mut buffer = Vec::new();
      while let Some(line) = rx.recv().await {
          let timestamp_ns = SystemTime::now()
              .duration_since(SystemTime::UNIX_EPOCH)
              .unwrap()
              .as_nanos();
          let log_message = format!(
              "log,host={},level={} message=\"{}\" {}",
              config.hostname, "INFO", line, timestamp_ns
          );
          buffer.push(log_message);
          if buffer.len() >= batch_size {
              let data = buffer.join("\n");
              send_data_to_influxdb(&client, &config, data).await?;
              buffer.clear();
          }
      }
      // Send remaining messages in the buffer
      if !buffer.is_empty() {
          let data = buffer.join("\n");
          send_data_to_influxdb(&client, &config, data).await?;
      }

      Ok::<(), Box<dyn Error + Send + Sync>>(())
  });

// Set up the log watcher (synchronous context)
loop {
  if Path::new(&path).exists() {
      let mut log_watcher = LogWatcher::register(&path).unwrap();
      log_watcher.watch(&mut move |line: String| {
          let tx = tx.clone();
          tokio::spawn(async move {
              tx.send(line).await.unwrap();
          });
          LogWatcherAction::None
      });
      break;
  } else {
      log::error!("File does not exist, re-checking in 2 seconds");
      sleep(Duration::from_secs(2)).await;
  }
}

  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {

    dotenv().ok();
    
    let local_config = LocalConfig::from_env().unwrap();
    let mut sys = System::new();
    let client = reqwest::Client::new();
    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} - {m}\n")))
        .build("log/daggermon.log")?;

    let log_config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(LevelFilter::Info))?;

    log4rs::init_config(log_config)?;

    tokio::spawn(watch_log_file("/home/dagger/config.log".to_string(), client.clone(), local_config.clone(), 10));

    log::info!("rust-sys-mon started");
    let ten_secs = time::Duration::from_secs(10);

    let rkind = RefreshKind::new()
        .with_cpu(CpuRefreshKind::everything().without_frequency())
        .with_memory()
        .with_networks();

    loop {
        // First we update all information of our `System` struct.
        sys.refresh_specifics(rkind);
        let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();

        // Timestamp
        log::info!("=> {}", timestamp_ns);
        // Network interfaces name, data received and data transmitted:
        log::info!("=> networks:");
        for (interface_name, data) in sys.networks() {
            let data_r = data.received();
            let data_t = data.transmitted();
            if data_r > 0 || data_t > 0 {
                log::info!(
                    "{}: {}/{}",
                    interface_name,
                    human_bytes(data.received() as f64),
                    human_bytes(data.transmitted() as f64)
                );
            }
        }

        log::info!("=> system:");
        // RAM and swap information:
        log::info!("total memory: {}", human_bytes(sys.total_memory() as f64));
        log::info!("used memory : {}", human_bytes(sys.used_memory() as f64));
        log::info!("total swap  : {}", human_bytes(sys.total_swap() as f64));
        log::info!("used swap   : {}", human_bytes(sys.used_swap() as f64));

        // Display system information:
        log::info!(
            "System name:             {:?}",
            sys.name().unwrap_or(String::from("Unavailable"))
        );
        log::info!(
            "System kernel version:   {:?}",
            sys.kernel_version().unwrap_or(String::from("Unavailable"))
        );
        log::info!(
            "System OS version:       {:?}",
            sys.os_version().unwrap_or(String::from("Unavailable"))
        );
        log::info!(
            "System host name:        {:?}",
            sys.host_name().unwrap_or(String::from("Unavailable"))
        );

        // Number of CPUs:
        log::info!("NB CPUs: {}", sys.cpus().len());

        for (index, cpu) in sys.cpus().iter().enumerate() {
            log::info!("{}: {}%", index, cpu.cpu_usage());
        }

        // Memory and Swap Metrics
        let memory_data = format!(
            "memory,host={} total={}i,used={}i,free={}i {}",
            local_config.hostname,
            sys.total_memory(),
            sys.used_memory(),
            sys.free_memory(),
            timestamp_ns
        );
        let swap_data = format!(
            "swap,host={} total={}i,used={}i,free={}i {}",
            local_config.hostname,
            sys.total_swap(),
            sys.used_swap(),
            sys.free_swap(),
            timestamp_ns
        );

        // Network Metrics
        let mut network_data = String::new();
        let mut total_data_received = 0;
        let mut total_data_transmitted = 0;

        for (interface_name, data) in sys.networks() {
            let interface_data = format!(
                "network,host={},interface={} received={}i,transmitted={}i {}\n",
                local_config.hostname,
                interface_name,
                data.received(),
                data.transmitted(),
                timestamp_ns
            );
            total_data_received += data.received();
            total_data_transmitted += data.transmitted();
            network_data.push_str(&interface_data);
        }

        let interface_data = format!(
            "network,host={},interface={} received={}i,transmitted={}i {}\n",
            local_config.hostname, "total", total_data_received, total_data_transmitted, timestamp_ns
        );
        network_data.push_str(&interface_data);

        // CPU Metrics
        let mut cpu_data = String::new();
        let mut total_cpu_usage = 0.;
        for (i, cpu) in sys.cpus().iter().enumerate() {
            let cpu_usage_data = format!(
                "cpu,host={},cpu={} usage={} {}\n",
                local_config.hostname,
                i,
                cpu.cpu_usage(),
                timestamp_ns
            );
            total_cpu_usage += cpu.cpu_usage();
            cpu_data.push_str(&cpu_usage_data);
        }

        let cpu_usage_data = format!(
            "cpu,host={},cpu={} usage={} {}\n",
            local_config.hostname, "total", total_cpu_usage, timestamp_ns
        );
        cpu_data.push_str(&cpu_usage_data);

        // Send Data to InfluxDB
        let data = format!(
            "{}\n{}\n{}\n{}",
            memory_data, swap_data, network_data, cpu_data
        );


        send_data_to_influxdb(&client, &local_config, data).await?;

        // Sleeping to let time for the system to run for long
        // enough to have useful information.
        tokio::time::sleep(ten_secs).await;
    }
}

const SUFFIX: [&str; 9] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"];

const UNIT: f64 = 1024.0;

/// Converts bytes to human-readable values
pub fn human_bytes<T: Into<f64>>(bytes: T) -> String {
    let size = bytes.into();

    if size <= 0.0 {
        return "0 B".to_string();
    }

    let base = size.log10() / UNIT.log10();

    let result = format!("{:.1}", UNIT.powf(base - base.floor()),)
        .trim_end_matches(".0")
        .to_owned();

    // Add suffix
    [&result, SUFFIX[base.floor() as usize]].join(" ")
}
