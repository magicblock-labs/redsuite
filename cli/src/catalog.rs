use redsuite_core::{
    catalog::{Family, ScenarioEntry},
    Result,
};

fn entries(family: Family) -> Vec<ScenarioEntry> {
    match family {
        Family::Redline => redline::scenarios::catalog(),
        Family::Redshift => redshift::scenarios::catalog(),
        Family::Redhat => redhat::scenarios::catalog(),
    }
}

/// Resolve selections within one family, preferring exact names over prefixes.
/// Overlapping selections form an ordered union; no selections means the family.
pub fn select(family: Family, targets: &[String]) -> Result<Vec<ScenarioEntry>> {
    let entries = entries(family);
    if targets.is_empty() {
        return Ok(entries);
    }
    let mut selected = Vec::new();
    for target in targets {
        let (exact, prefixed): (Vec<_>, Vec<_>) = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                entry.name(family).starts_with(target) || entry.short_name.starts_with(target)
            })
            .partition(|(_, entry)| entry.name(family) == *target || entry.short_name == target);
        let matches = if exact.is_empty() { prefixed } else { exact };
        if matches.is_empty() {
            return Err(format!(
                "nothing matches `{target}` — `redsuite {} list` shows what exists",
                family.prefix()
            )
            .into());
        }
        for (index, _) in matches {
            if !selected.contains(&index) {
                selected.push(index);
            }
        }
    }
    let mut entries: Vec<_> = entries.into_iter().map(Some).collect();
    Ok(selected.into_iter().filter_map(|index| entries[index].take()).collect())
}
