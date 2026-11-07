//! An event's domain is read from the event itself, its type and its source, before anything is
//! written. Where a fact lands, a row kind prefixed by its domain, follows from this; it does not
//! decide it.

/// Whether an event's type and source are of the same known domain.
///
/// # Errors
/// They are not: the event belongs to another domain.
#[inline]
pub fn check(ty: &str, source: &str) -> Result<(), String> {
    let of_type = ty.split_once('.').map_or(ty, |(d, _)| d);
    let of_source = source.split_once('/').map_or(source, |(d, _)| d);
    if of_type == of_source && (of_type == "westwind" || of_type == "oz") {
        Ok(())
    } else {
        Err(format!("type {ty} is {of_type}'s, source {source} is {of_source}'s"))
    }
}
