//! Test the symbolicate-stacks executable.
use std::{
    fs::File,
    io::{Write, copy},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

const EXE: &str = env!("CARGO_BIN_EXE_symbolicate-stacks");
const TESTDATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata");

fn command() -> Command {
    let mut command = Command::new(EXE);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn assert_detailed_output(output: String) {
    insta::allow_duplicates! {
        insta::assert_snapshot!("process_input_json_output", output);
    }
}

#[test]
fn process_input_using_cache() {
    let mut config_file = tempfile::NamedTempFile::new().unwrap();

    config_file
        .write_all(
            format!(
                r#"
symbol_servers = {{}}
cache.directory = "{TESTDATA}/cache""#
            )
            .as_bytes(),
        )
        .unwrap();

    let mut child = command()
        .arg("--config-file")
        .arg(config_file.path())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut f = File::open(Path::new(TESTDATA).join("input.json")).unwrap();
    copy(&mut f, &mut input).unwrap();
    drop(input);

    let output = child.wait_with_output().unwrap();
    dbg!(String::from_utf8(output.stderr).unwrap());
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_detailed_output(stdout);
}

#[test]
fn process_input_with_download() {
    let mut config_file = tempfile::NamedTempFile::new().unwrap();
    let mut server = mockito::Server::new();

    fn cached_path_from_request(request: &mockito::Request) -> PathBuf {
        // skip(1) for the leading '/' component
        let cachepath: PathBuf = Path::new(request.path())
            .components()
            .skip(1)
            .take(2)
            .collect();
        Path::new(TESTDATA)
            .join("cache")
            .join(cachepath)
            .join("_primary")
    }

    let mock = server
        .mock("GET", mockito::Matcher::Any)
        .with_status_code_from_request(|request| {
            if cached_path_from_request(request).exists() {
                200
            } else {
                404
            }
        })
        .with_body_from_request(|request| {
            std::fs::read(cached_path_from_request(request)).unwrap_or_default()
        })
        .expect_at_least(1)
        .create();

    let cachedir = tempfile::tempdir().unwrap();

    config_file
        .write_all(
            format!(
                r#"
symbol_servers.breakpad = ["{server}"]
cache.directory = "{cachedir}""#,
                server = server.url(),
                cachedir = cachedir.path().display()
            )
            .as_bytes(),
        )
        .unwrap();

    let mut child = command()
        .arg("--config-file")
        .arg(config_file.path())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut f = File::open(Path::new(TESTDATA).join("input.json")).unwrap();
    copy(&mut f, &mut input).unwrap();
    drop(input);

    let output = child.wait_with_output().unwrap();
    dbg!(String::from_utf8(output.stderr).unwrap());
    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_detailed_output(stdout);

    mock.assert();
}
