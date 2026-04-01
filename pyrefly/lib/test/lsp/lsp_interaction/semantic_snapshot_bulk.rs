/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;

use lsp_types::Location;
use lsp_types::Url;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

use crate::test::lsp::lsp_interaction::object_model::InitializeSettings;
use crate::test::lsp::lsp_interaction::object_model::LspInteraction;
use crate::test::lsp::lsp_interaction::util::get_test_files_root;

struct SemanticSnapshotBulk;

impl lsp_types::request::Request for SemanticSnapshotBulk {
    type Params = BulkParams;
    type Result = BulkResult;
    const METHOD: &'static str = "muffet/semanticSnapshotBulk";
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkParams {
    lines: Vec<BulkLine>,
    options: Option<BulkOptions>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkOptions {
    max_targets_per_site: u32,
    deadline_ms: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkLine {
    request_id: u64,
    uri: String,
    project_root_path: String,
    lsp_version: i32,
    content_source: String,
    text: Option<String>,
    call_sites: Vec<CallSite>,
    ref_sites: Vec<RefSite>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CallSite {
    line: u32,
    character: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RefSite {
    line: u32,
    character: u32,
    end_line: u32,
    end_character: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkResult {
    responses: Vec<BulkResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkResponse {
    result: Option<SnapshotResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotResult {
    defs_by_call_site: Vec<DefsAtSite>,
    defs_by_ref_site: Vec<DefsAtSite>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DefsAtSite {
    defs: Vec<Location>,
}

fn offset_to_position(text: &str, offset: usize) -> (u32, u32) {
    let prefix = &text[..offset];
    let line = prefix.bytes().filter(|b| *b == b'\n').count() as u32;
    let column = prefix
        .rsplit_once('\n')
        .map(|(_, tail)| tail.len() as u32)
        .unwrap_or(prefix.len() as u32);
    (line, column)
}

fn nth_span(text: &str, needle: &str, occurrence: usize) -> ((u32, u32), (u32, u32)) {
    let start = text
        .match_indices(needle)
        .nth(occurrence)
        .map(|(offset, _)| offset)
        .unwrap_or_else(|| panic!("missing occurrence {occurrence} for '{needle}'"));
    let end = start + needle.len();
    (
        offset_to_position(text, start),
        offset_to_position(text, end),
    )
}

fn bulk_line(
    root: &Path,
    file: &str,
    call_needles: &[(&str, usize)],
    ref_needles: &[(&str, usize)],
) -> BulkLine {
    let path = root.join(file);
    let text = fs::read_to_string(&path).unwrap();
    let call_sites = call_needles
        .iter()
        .map(|(needle, occurrence)| {
            let ((line, character), _) = nth_span(&text, needle, *occurrence);
            CallSite { line, character }
        })
        .collect();
    let ref_sites = ref_needles
        .iter()
        .map(|(needle, occurrence)| {
            let ((line, character), (end_line, end_character)) =
                nth_span(&text, needle, *occurrence);
            RefSite {
                line,
                character,
                end_line,
                end_character,
            }
        })
        .collect();

    BulkLine {
        request_id: 1,
        uri: Url::from_file_path(&path).unwrap().to_string(),
        project_root_path: root.display().to_string(),
        lsp_version: 1,
        content_source: "text".to_owned(),
        text: Some(text),
        call_sites,
        ref_sites,
    }
}

fn bulk_params(line: BulkLine) -> BulkParams {
    BulkParams {
        lines: vec![line],
        options: Some(BulkOptions {
            max_targets_per_site: 8,
            deadline_ms: 200,
        }),
    }
}

#[cfg(unix)]
fn setup_dummy_interpreter(custom_interpreter_path: &Path) -> PathBuf {
    let python_script = format!(
        r#"#!/usr/bin/env bash
if [[ "$1" == "-c" && "$2" == *"import json, sys"* ]]; then
    cat << 'EOF'
{{"python_platform": "linux", "python_version": "3.12.0", "site_package_path": ["{site_packages}"]}}
EOF
else
    echo "Mock python interpreter - args: $@" >&2
    exit 1
fi
"#,
        site_packages = custom_interpreter_path
            .join("bin/site-packages")
            .to_str()
            .unwrap()
    );

    let interpreter_path = custom_interpreter_path.join("bin/python");
    fs::write(&interpreter_path, python_script).unwrap();
    let mut perms = fs::metadata(&interpreter_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&interpreter_path, perms).unwrap();

    interpreter_path
}

#[test]
fn semantic_snapshot_bulk_preserves_repo_local_definition_locations() {
    let root = get_test_files_root();
    let fixture_root = root.path().join("semantic_snapshot_bulk");
    let mut interaction = LspInteraction::new();
    interaction.set_root(fixture_root.clone());
    interaction
        .initialize(InitializeSettings::default())
        .unwrap();

    interaction.client.did_open("same_file.py");
    interaction.client.did_open("cross_file.py");
    interaction.client.did_open("module_scope.py");

    interaction
        .client
        .send_request::<SemanticSnapshotBulk>(
            serde_json::to_value(bulk_params(bulk_line(
                &fixture_root,
                "same_file.py",
                &[("target()", 1)],
                &[],
            )))
            .unwrap(),
        )
        .expect_response_with(|result| {
            let defs = &result.responses[0]
                .result
                .as_ref()
                .unwrap()
                .defs_by_call_site[0]
                .defs;
            defs.len() == 1
                && defs[0].uri.to_file_path().ok().as_deref()
                    == Some(fixture_root.join("same_file.py").as_path())
        })
        .unwrap();

    interaction
        .client
        .send_request::<SemanticSnapshotBulk>(
            serde_json::to_value(bulk_params(bulk_line(
                &fixture_root,
                "cross_file.py",
                &[("imported()", 0)],
                &[],
            )))
            .unwrap(),
        )
        .expect_response_with(|result| {
            let defs = &result.responses[0]
                .result
                .as_ref()
                .unwrap()
                .defs_by_call_site[0]
                .defs;
            defs.len() == 1
                && defs[0].uri.to_file_path().ok().as_deref()
                    == Some(fixture_root.join("imported.py").as_path())
        })
        .unwrap();

    interaction
        .client
        .send_request::<SemanticSnapshotBulk>(
            serde_json::to_value(bulk_params(bulk_line(
                &fixture_root,
                "module_scope.py",
                &[],
                &[("LOCAL_VALUE", 1)],
            )))
            .unwrap(),
        )
        .expect_response_with(|result| {
            let defs = &result.responses[0]
                .result
                .as_ref()
                .unwrap()
                .defs_by_ref_site[0]
                .defs;
            defs.len() == 1
                && defs[0].uri.to_file_path().ok().as_deref()
                    == Some(fixture_root.join("module_scope.py").as_path())
        })
        .unwrap();

    interaction.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn semantic_snapshot_bulk_preserves_external_source_backed_locations() {
    let root = get_test_files_root();
    let workspace_root = root.path().join("custom_interpreter/src");
    let interpreter_path = setup_dummy_interpreter(&root.path().join("custom_interpreter"));
    let scope_uri = Url::from_file_path(&workspace_root).unwrap();
    let mut interaction = LspInteraction::new();
    interaction.set_root(workspace_root.clone());
    interaction
        .initialize(InitializeSettings {
            workspace_folders: Some(vec![("test".to_owned(), scope_uri)]),
            configuration: Some(Some(json!([{
                "pythonPath": interpreter_path.to_str().unwrap()
            }]))),
            ..Default::default()
        })
        .unwrap();

    interaction.client.did_open("foo.py");

    interaction
        .client
        .send_request::<SemanticSnapshotBulk>(
            serde_json::to_value(bulk_params(bulk_line(
                &workspace_root,
                "foo.py",
                &[],
                &[("CustomClass", 1)],
            )))
            .unwrap(),
        )
        .expect_response_with(|result| {
            let defs = &result.responses[0]
                .result
                .as_ref()
                .unwrap()
                .defs_by_ref_site[0]
                .defs;
            defs.len() == 1
                && defs[0].uri.to_file_path().ok().map(|path| {
                    path.ends_with("custom_interpreter/bin/site-packages/custom_module.py")
                }) == Some(true)
        })
        .unwrap();

    interaction.shutdown().unwrap();
}
