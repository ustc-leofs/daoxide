use daoxide::container::{ContainerOpen, flags::CONT_OPEN_RW};
use daoxide::io::{AKey, DKey, IoBuffer, Iod, IodArrayBuilder, IodSingleBuilder, Recx, Sgl};
use daoxide::iter::{EnumConfig, RecxOrder};
use daoxide::object::{Object, ObjectClass, ObjectClassHints, ObjectOpenMode, ObjectType};
use daoxide::pool::{PoolBuilder, flags::POOL_CONNECT_READWRITE};
use daoxide::prelude::*;
use daoxide::query::QueryKeyFlags;
use daoxide::runtime::{is_runtime_initialized, require_runtime};
use daoxide::tx::{Transaction, TxState};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_container_label() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("daoxide-e2e-{}-{}", std::process::id(), ts)
}

fn cleanup_container(pool: &str, container: &str) {
    let attempts: [(&str, &[&str]); 2] = [
        (
            "daos",
            &["container", "destroy", pool, container, "--force"],
        ),
        ("daos", &["cont", "destroy", pool, container, "--force"]),
    ];
    for (program, args) in attempts {
        if let Ok(status) = Command::new(program).args(args).status() {
            if status.success() {
                eprintln!("cleanup: destroyed container {}", container);
                return;
            }
        }
    }
    eprintln!(
        "cleanup warning: failed to destroy container {}; remove it manually",
        container
    );
}

struct CleanupGuard {
    pool: String,
    container: String,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        cleanup_container(&self.pool, &self.container);
    }
}

fn create_container_via_cli(pool: &str, container: &str, sys: Option<&str>) -> Result<()> {
    let mut cmd = Command::new("daos");
    cmd.arg("container").arg("create");
    if let Some(s) = sys {
        cmd.arg("--sys-name").arg(s);
    }
    cmd.arg(pool).arg(container);
    let output = cmd
        .output()
        .map_err(|e| DaosError::Internal(format!("failed to run daos cli: {e}")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(DaosError::Internal(format!(
        "daos container create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    )))
}

fn parse_pool_arg_from_test_binary() -> Result<String> {
    let mut pool_from_cli: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--pool" | "-p" => {
                let value = args
                    .next()
                    .ok_or_else(|| DaosError::Internal("missing value for --pool/-p".into()))?;
                pool_from_cli = Some(value);
            }
            "--help" | "-h" => {
                println!(
                    "Usage: cargo test --test real_daos_e2e -- --nocapture [-- --pool <POOL_LABEL>]"
                );
                std::process::exit(0);
            }
            _ => {}
        }
    }
    Ok(pool_from_cli
        .or_else(|| std::env::var("DAOXIDE_E2E_POOL").ok())
        .unwrap_or_else(|| "SHARED".to_string()))
}

#[test]
#[ignore = "requires real DAOS and daos CLI; run manually with -- --ignored --nocapture"]
fn real_daos_e2e() -> Result<()> {
    let pool_label = parse_pool_arg_from_test_binary()?;
    let system_name = std::env::var("DAOXIDE_E2E_SYS").ok();
    let container_label =
        std::env::var("DAOXIDE_E2E_CONT").unwrap_or_else(|_| unique_container_label());

    println!("== daoxide real DAOS e2e ==");
    println!("pool={pool_label}, container={container_label}");

    let _runtime = daoxide::runtime::DaosRuntime::new()?;
    assert!(is_runtime_initialized(), "runtime should be initialized");
    require_runtime()?;
    println!("[ok] runtime state");

    let mut state_pool_builder = PoolBuilder::new()
        .label(&pool_label)
        .flags(POOL_CONNECT_READWRITE);
    if let Some(sys) = system_name.as_deref() {
        state_pool_builder = state_pool_builder.system(sys);
    }
    let mut state_pool = state_pool_builder.build()?;
    state_pool.disconnect()?;
    assert!(
        state_pool.disconnect().is_err(),
        "second disconnect should fail"
    );
    println!("[ok] pool disconnect states");

    let mut pool_builder = PoolBuilder::new()
        .label(&pool_label)
        .flags(POOL_CONNECT_READWRITE);
    if let Some(sys) = system_name.as_deref() {
        pool_builder = pool_builder.system(sys);
    }
    let pool = pool_builder.build()?;

    create_container_via_cli(&pool_label, &container_label, system_name.as_deref())?;
    let _cleanup = CleanupGuard {
        pool: pool_label.clone(),
        container: container_label.clone(),
    };

    let mut state_container =
        pool.open_container(&container_label, ContainerOpen::ByLabel, CONT_OPEN_RW)?;
    let state_info = state_container.query()?;
    assert_ne!(
        state_info.uuid, [0u8; 16],
        "container uuid should not be zero"
    );
    let _ = state_container.alloc_oids(1)?;
    state_container.close()?;
    assert!(state_container.close().is_err(), "second close should fail");
    println!("[ok] container open/query/alloc/close states");

    let container = pool.open_container(&container_label, ContainerOpen::ByLabel, CONT_OPEN_RW)?;

    let mut tx = Transaction::new(&container, 0)?;
    assert!(tx.is_open());
    assert_eq!(tx.state(), TxState::Open);
    assert!(tx.as_handle().is_ok());
    assert!(tx.restart().is_err());
    assert_eq!(tx.state(), TxState::Open);
    tx.commit()?;
    assert_eq!(tx.state(), TxState::Committed);
    assert!(tx.as_handle().is_err());
    assert!(tx.commit().is_err());
    assert!(tx.abort().is_err());
    assert!(tx.restart().is_err());
    tx.close()?;
    assert_eq!(tx.state(), TxState::Closed);
    tx.close()?;

    let mut tx_abort = Transaction::new(&container, 0)?;
    tx_abort.abort()?;
    assert_eq!(tx_abort.state(), TxState::Aborted);
    tx_abort.close()?;
    println!("[ok] transaction state machine");

    let mut client_builder = DaosClient::builder()
        .pool_label(&pool_label)
        .pool_flags(POOL_CONNECT_READWRITE)
        .container_label(&container_label)
        .container_flags(CONT_OPEN_RW);
    if let Some(sys) = system_name.as_deref() {
        client_builder = client_builder.pool_system(sys);
    }
    let client = client_builder.build()?;

    let kv_oid = client.alloc_oid(
        ObjectType::KvHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;
    let expected = b"hello-daoxide-real-daos";
    client.put(kv_oid, b"dkey-e2e", b"akey-e2e", expected)?;
    let mut got = vec![0u8; expected.len()];
    client.get(kv_oid, b"dkey-e2e", b"akey-e2e", &mut got[..])?;
    assert_eq!(&got, expected);
    println!("[ok] facade put/get");

    let mut kv_obj = Object::open_in(&container, kv_oid, ObjectOpenMode::ReadWrite)?;
    assert!(kv_obj.is_open());
    assert!(
        kv_obj
            .query_key(&Tx::none(), QueryKeyFlags::default(), None, None)
            .is_err()
    );
    assert!(
        kv_obj
            .query_key(
                &Tx::none(),
                QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_MIN | QueryKeyFlags::GET_DKEY,
                Some(&DKey::new(b"dkey-e2e")?),
                None
            )
            .is_err()
    );
    assert!(
        kv_obj
            .query_key(
                &Tx::none(),
                QueryKeyFlags::GET_MAX | QueryKeyFlags::GET_AKEY,
                Some(&DKey::new(b"dkey-e2e")?),
                None
            )
            .is_err()
    );
    assert!(kv_obj.punch_dkeys(&Tx::none(), &[]).is_err());
    assert!(
        kv_obj
            .punch_akeys(&Tx::none(), &DKey::new(b"dkey-e2e")?, &[])
            .is_err()
    );
    println!("[ok] object input validation");

    let enum_oid = client.alloc_oid(
        ObjectType::MultiHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;
    let mut enum_obj = Object::open_in(&container, enum_oid, ObjectOpenMode::ReadWrite)?;
    let dkey = DKey::new(b"dkey-low")?;
    let akey = AKey::new(b"akey-low")?;
    let low_val = b"low-level-value";
    enum_obj.update(
        &Tx::none(),
        &dkey,
        &Iod::Single(
            IodSingleBuilder::new(akey.clone())
                .value_len(low_val.len())
                .build()?,
        ),
        &Sgl::builder()
            .push(IoBuffer::from_vec(low_val.to_vec()))
            .build()?,
    )?;
    let mut obj_for_enum = Object::open_in(&container, enum_oid, ObjectOpenMode::ReadWrite)?;
    let dkeys: Vec<Vec<u8>> = obj_for_enum
        .enumerate_dkeys()?
        .map(|x| x.map(|k| k.as_bytes().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert!(dkeys.iter().any(|k| k.as_slice() == b"dkey-low"));
    let akeys: Vec<Vec<u8>> = obj_for_enum
        .enumerate_akeys(&dkey)?
        .map(|x| x.map(|k| k.as_bytes().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert!(akeys.iter().any(|k| k.as_slice() == b"akey-low"));
    assert!(obj_for_enum.query_max_epoch(&Tx::none())? > 0);
    println!("[ok] object enumeration/query states");

    let multi_oid_a = client.alloc_oid(
        ObjectType::MultiHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;
    let multi_oid_b = client.alloc_oid(
        ObjectType::MultiHashed,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;
    let multi_obj_a = Object::open_in(&container, multi_oid_a, ObjectOpenMode::ReadWrite)?;
    let mut multi_obj_b = Object::open_in(&container, multi_oid_b, ObjectOpenMode::ReadWrite)?;

    let cases_a: [(&[u8], &[u8], &[u8]); 3] = [
        (b"dkey-a1", b"akey-a1", b"v-a1-a1"),
        (b"dkey-a1", b"akey-a2", b"v-a1-a2"),
        (b"dkey-a2", b"akey-a1", b"v-a2-a1"),
    ];
    for (dk, ak, val) in cases_a {
        multi_obj_a.update(
            &Tx::none(),
            &DKey::new(dk)?,
            &Iod::Single(
                IodSingleBuilder::new(AKey::new(ak)?)
                    .value_len(val.len())
                    .build()?,
            ),
            &Sgl::builder()
                .push(IoBuffer::from_vec(val.to_vec()))
                .build()?,
        )?;
    }
    multi_obj_b.update(
        &Tx::none(),
        &DKey::new(b"dkey-b1")?,
        &Iod::Single(
            IodSingleBuilder::new(AKey::new(b"akey-b1")?)
                .value_len(b"v-b".len())
                .build()?,
        ),
        &Sgl::builder()
            .push(IoBuffer::from_vec(b"v-b".to_vec()))
            .build()?,
    )?;
    for (dk, ak, val) in cases_a {
        let mut sgl = Sgl::builder()
            .push(IoBuffer::from_vec(vec![0u8; val.len()]))
            .build()?;
        multi_obj_a.fetch(
            &Tx::none(),
            &DKey::new(dk)?,
            &Iod::Single(
                IodSingleBuilder::new(AKey::new(ak)?)
                    .value_len(val.len())
                    .build()?,
            ),
            &mut sgl,
        )?;
        assert_eq!(sgl.buffers()[0].as_slice(), val);
    }
    let enum_cfg = EnumConfig {
        batch_size: 1,
        buffer_size: 256,
    };
    let b_dkeys: Vec<Vec<u8>> = multi_obj_b
        .enumerate_dkeys_with_config(enum_cfg.clone())?
        .map(|x| x.map(|k| k.as_bytes().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert!(b_dkeys.iter().any(|k| k.as_slice() == b"dkey-b1"));
    assert!(!b_dkeys.iter().any(|k| k.as_slice() == b"dkey-a1"));
    let b_akeys: Vec<Vec<u8>> = multi_obj_b
        .enumerate_akeys_with_config(&DKey::new(b"dkey-b1")?, enum_cfg)?
        .map(|x| x.map(|k| k.as_bytes().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert!(b_akeys.iter().any(|k| k.as_slice() == b"akey-b1"));
    println!("[ok] multi object/dkey/akey coverage");

    let arr_oid = client.alloc_oid(
        ObjectType::Array,
        ObjectClass::UNKNOWN,
        ObjectClassHints::NONE,
    )?;
    let mut arr_obj = Object::open_in(&container, arr_oid, ObjectOpenMode::ReadWrite)?;
    let arr_dkey = DKey::new(b"dkey-arr")?;
    let arr_akey = AKey::new(b"akey-arr")?;
    arr_obj.update(
        &Tx::none(),
        &arr_dkey,
        &Iod::Array(
            IodArrayBuilder::new(arr_akey.clone())
                .record_len(8)
                .add_recx(Recx::new(0, 1)?)
                .add_recx(Recx::new(5, 1)?)
                .build()?,
        ),
        &Sgl::builder()
            .push(IoBuffer::from_vec(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
            ]))
            .build()?,
    )?;
    assert!(
        !arr_obj
            .enumerate_recxs(&arr_dkey, &arr_akey)?
            .collect::<Result<Vec<_>>>()?
            .is_empty()
    );
    assert!(
        !arr_obj
            .enumerate_recxs_ordered(&arr_dkey, &arr_akey, RecxOrder::Decreasing)?
            .collect::<Result<Vec<_>>>()?
            .is_empty()
    );
    println!("[ok] recx enumeration");

    client.delete(
        enum_oid,
        Some(b"dkey-low"),
        Some(&[b"akey-low".as_slice()]),
        Tx::none(),
    )?;
    let post_delete_akeys: Vec<Vec<u8>> = enum_obj
        .enumerate_akeys(&dkey)?
        .map(|x| x.map(|k| k.as_bytes().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert!(
        !post_delete_akeys
            .iter()
            .any(|k| k.as_slice() == b"akey-low")
    );
    println!("[ok] delete/post-delete states");

    kv_obj.close()?;
    assert!(!kv_obj.is_open());
    assert!(kv_obj.close().is_err());
    println!("[ok] object close states");

    println!("== all e2e checks passed ==");
    Ok(())
}
