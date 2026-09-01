use crate::config::OutputMode;

pub fn render(bytes: &[u8], mode: OutputMode) -> Result<String, serde_json::Error> {
    let value = serde_json::from_slice::<serde_json::Value>(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()));
    match mode {
        OutputMode::Json => serde_json::to_string_pretty(&value),
        OutputMode::Human => Ok(human(&value)),
    }
}

fn human(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Array(rows) => rows.iter().map(compact).collect::<Vec<_>>().join("\n"),
        serde_json::Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| format!("{key}\t{}", compact(value)))
            .collect::<Vec<_>>()
            .join("\n"),
        serde_json::Value::String(value) => value.clone(),
        value => compact(value),
    }
}

fn compact(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).expect("JSON value serialization"),
    }
}
