//! Check live source data without constructing or calling a Jev client.
use anyhow::Result;
use serde_json::json;
use std::{fmt::Write as _, path::PathBuf};
use stellar_raven_jev::{connectors, http::HttpRecorder, types::*};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let root = PathBuf::from("runs/source-checks").join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&root)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))?;
    }
    let sources = connectors::sources();
    let mut rows = Vec::new();
    let mut index = String::from("# Live source checks\n\nThese checks retrieve live data. They do not call Jev or score relevance.\n\n");
    for (number, (id, question)) in [
        ("lumenloop.jobs", "Find developer jobs"),
        ("stellarlight.audits", "Find audit reports."),
        ("algolia:docs:primary", "fee bump transactions"),
        ("algolia:site:pages", "wallet projects"),
    ]
    .into_iter()
    .enumerate()
    {
        let source = sources
            .iter()
            .find(|source| source.id == id)
            .ok_or_else(|| anyhow::anyhow!("Unknown source: {id}"))?;
        let dir = root.join(format!("{number:02}"));
        let config = RunConfig {
            output_dir: dir.clone(),
            max_documents: 2,
            per_source_documents: 2,
            max_pages: 2,
            timeout_secs: 20,
            ..Default::default()
        };
        let ctx = FetchContext {
            http: HttpRecorder::new(&dir, &config)?,
            config,
            deadline: None,
        };
        let result = match tokio::time::timeout(
            std::time::Duration::from_secs(90), connectors::fetch(&ctx, source, question),
        ).await {
            Ok(Ok(result)) => result,
            result => FetchResult { documents: vec![], failures: vec![Failure {
                stage: "source_check".into(), source_id: Some(id.into()),
                message: match result {
                    Ok(Err(error)) => error.to_string(),
                    Err(_) => "Source check timed out. Raw responses remain available; retrieval is incomplete.".into(),
                    Ok(Ok(_)) => unreachable!(),
                },
                cause: None,
            }] },
        };
        std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result)?)?;
        writeln!(
            index,
            "## {id}\n\nQuestion: {question}\n\n[Complete result]({number:02}/result.json)\n"
        )?;
        for (document_number, document) in result.documents.iter().enumerate() {
            let filename = format!("document-{document_number:02}.md");
            std::fs::write(
                dir.join(&filename),
                format!(
                    "# {}\n\n{}\n\n{}\n",
                    document.title, document.url, document.text
                ),
            )?;
            writeln!(
                index,
                "- [{}]({number:02}/{filename})",
                document.title.replace(['[', ']', '\n'], " ")
            )?;
        }
        let row = json!({"source_id":id,"question":question,"documents":result.documents.len(),
            "failures":result.failures,"result_path":format!("{number:02}/result.json")});
        println!(
            "{}",
            json!({"source_id":id,"documents":result.documents.len(),"failures":result.failures.len()})
        );
        rows.push(row);
        std::fs::write(
            root.join("summary.json"),
            serde_json::to_vec_pretty(&json!({
                "mode":"live-sources-only","jev_requests":0,"checks":rows
            }))?,
        )?;
        std::fs::write(root.join("INDEX.md"), &index)?;
    }
    println!("Evidence: {}", root.join("INDEX.md").display());
    Ok(())
}
