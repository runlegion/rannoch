//! The app: one migrator from the public `embed!`, over the directory build.rs fills.

/// Every crate's migrations as one migrator.
#[must_use]
pub fn migrator() -> migratr::Migrator {
    migratr::embed!("migrations")
}

#[cfg(test)]
mod tests {
    #[test]
    fn p11_to_p14_public_api_only() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mut exec = migratr::RusqliteExecutor::new(conn);
        let applied = super::migrator().up(&mut exec).unwrap().applied;
        println!("R3 applied: {applied:?}");
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        migratr::write_schema(&mut exec, &dir).unwrap();
        let schema = std::fs::read_to_string(dir.join("schema.json")).unwrap();
        let named: Vec<&str> = ["concept", "estimate", "symbol", "prediction"]
            .into_iter()
            .filter(|t| schema.contains(&format!("\"{t}\"")))
            .collect();
        println!("R3 schema.json names: {named:?}");
    }
}
