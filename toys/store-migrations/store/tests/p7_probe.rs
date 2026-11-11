//! Prediction 7: how many migrations sym embeds after a file is added to its directory.

#[test]
fn p7_probe_how_many_migrations_sym_embeds() {
    let mut exec = migratr::RusqliteExecutor::new(rusqlite::Connection::open_in_memory().unwrap());
    let n = sym::migrator().status(&mut exec).unwrap().len();
    println!("P7 sym embeds {n} migrations");
}
