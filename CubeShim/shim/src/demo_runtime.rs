// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

//! Demo-only async shim runner.
//!
//! The upstream runner registers the v2 task service only.  Containerd 2.x
//! uses the v3 service name when it reuses a sandbox shim, so this runner keeps
//! the upstream lifecycle behavior and registers both names on the same socket.

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;
use std::{env, fs, path::Path, sync::Arc};

use containerd_shim::{
    asynchronous::{publisher::RemotePublisher, Shim},
    parse, Config, Error, Flags, StartOpts,
};
use containerd_shim_protos::{protobuf::Message, shim_async::create_task, ttrpc::r#async::Server};
use tokio::io::AsyncWriteExt;

use crate::common::utils::ADDRESS_FILE;

const TTRPC_ADDRESS: &str = "TTRPC_ADDRESS";

pub async fn run<T>(runtime_id: &str, opts: Option<Config>)
where
    T: Shim + Send + Sync + 'static,
{
    if let Err(err) = bootstrap::<T>(runtime_id, opts).await {
        eprintln!("{}: {:?}", runtime_id, err);
        std::process::exit(1);
    }
}

async fn bootstrap<T>(runtime_id: &str, opts: Option<Config>) -> Result<(), Error>
where
    T: Shim + Send + Sync + 'static,
{
    let os_args: Vec<_> = env::args_os().collect();
    let flags = parse(&os_args[1..])?;
    let ttrpc_address = env::var(TTRPC_ADDRESS)?;
    let mut config = opts.unwrap_or_default();
    let mut shim = T::new(runtime_id, &flags, &mut config).await;

    match flags.action.as_str() {
        "start" => {
            let address = shim
                .start_shim(StartOpts {
                    id: flags.id,
                    publish_binary: flags.publish_binary,
                    address: flags.address,
                    ttrpc_address,
                    namespace: flags.namespace,
                    debug: flags.debug,
                })
                .await?;
            let mut stdout = tokio::io::stdout();
            stdout
                .write_all(address.as_bytes())
                .await
                .map_err(|err| Error::IoError {
                    context: "write shim bootstrap response failed".to_string(),
                    err,
                })?;
            stdout.flush().await.map_err(|err| Error::IoError {
                context: "flush shim bootstrap response failed".to_string(),
                err,
            })?;
            Ok(())
        }
        "delete" => {
            let response = shim.delete_shim().await?;
            let response_bytes = response.write_to_bytes()?;
            tokio::io::stdout()
                .write_all(&response_bytes)
                .await
                .map_err(|err| Error::IoError {
                    context: "write shim delete response failed".to_string(),
                    err,
                })?;
            Ok(())
        }
        _ => serve::<T>(flags, config, ttrpc_address, shim).await,
    }
}

async fn serve<T>(
    flags: Flags,
    config: Config,
    ttrpc_address: String,
    mut shim: T,
) -> Result<(), Error>
where
    T: Shim + Send + Sync + 'static,
{
    if flags.socket.is_empty() {
        return Err(Error::InvalidArgument(
            "Shim socket cannot be empty".to_string(),
        ));
    }

    if !config.no_setup_logger {
        containerd_shim::logger::init(
            flags.debug,
            &config.default_log_level,
            &flags.namespace,
            &flags.id,
        )?;
    }

    let publisher = RemotePublisher::new(ttrpc_address).await?;
    let task = Arc::new(shim.create_task_service(publisher).await)
        as Arc<dyn containerd_shim_protos::shim_async::Task + Send + Sync>;

    let mut task_services = create_task(task.clone());
    let mut task_v3_services = create_task(task);
    if let Some(task_service) = task_v3_services.remove("containerd.task.v2.Task") {
        task_services.insert("containerd.task.v3.Task".to_string(), task_service);
    }

    let socket_path = flags
        .socket
        .strip_prefix("unix://")
        .unwrap_or(&flags.socket);
    if !socket_path.starts_with('@') {
        if let Some(parent) = Path::new(socket_path).parent() {
            fs::create_dir_all(parent).map_err(|err| Error::IoError {
                context: format!("create shim socket directory {}", parent.display()),
                err,
            })?;
        }
        if let Ok(metadata) = fs::metadata(socket_path) {
            if metadata.file_type().is_socket() {
                fs::remove_file(socket_path).map_err(|err| Error::IoError {
                    context: format!("remove stale shim socket {socket_path}"),
                    err,
                })?;
            }
        }
    }

    let bind_address = if flags.socket.starts_with("unix://") {
        flags.socket.clone()
    } else {
        format!("unix://{}", flags.socket)
    };
    let mut server = Server::new().bind(&bind_address)?;
    server = server.register_service(task_services);
    server.start().await?;
    signal_server_started();

    // Containerd normally shuts the shim down through the Task.Shutdown RPC;
    // consume termination signals so they do not bypass that cleanup path.
    #[cfg(unix)]
    tokio::spawn(async {
        use tokio::signal::unix::{signal, SignalKind};
        let Ok(mut signals) = signal(SignalKind::terminate()) else {
            return;
        };
        while signals.recv().await.is_some() {}
    });

    shim.wait().await;
    server.shutdown().await.unwrap_or_default();

    if let Ok(address) = fs::read_to_string(ADDRESS_FILE) {
        let address = address
            .trim()
            .strip_prefix("unix://")
            .unwrap_or(address.trim());
        if let Ok(metadata) = fs::metadata(address) {
            if metadata.file_type().is_socket() {
                let _ = fs::remove_file(address);
            }
        }
    }
    Ok(())
}

fn signal_server_started() {
    #[cfg(unix)]
    {
        use libc::{dup2, STDERR_FILENO, STDOUT_FILENO};

        // The parent start shim reads the child stdout until EOF. Match the
        // upstream runner's activation handshake once the TTRPC server is up.
        unsafe {
            if dup2(STDERR_FILENO, STDOUT_FILENO) < 0 {
                panic!(
                    "failed to close shim bootstrap pipe: {}",
                    std::io::Error::last_os_error()
                );
            }
        }
    }
}
