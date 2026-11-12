#[test]
fn p20_another_crate_builds_a_symbol() {
    let conn = app5::forgr::empty_store();
    let s = app5::forgr::symbol::factory().id("s2").path("b.rs").insert(&conn).unwrap();
    println!("R5 P20 user inserted symbol {}", s.id);
}
