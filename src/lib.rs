use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use chrono::DateTime;
use chrono::FixedOffset;
use serde_json;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

const MAX_BACKUPS: usize = 10;

#[derive(Debug)]
pub struct DBError(String);

pub type DB<T> = HashMap<String, T>;

impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DBError {}

impl From<serde_json::Error> for DBError {
    fn from(e: serde_json::Error) -> Self {
        DBError(format!("Serde error: {}", e))
    }
}

impl From<std::io::Error> for DBError {
    fn from(e: std::io::Error) -> Self {
        DBError(format!("IO error: {}", e))
    }
}

pub fn load_db<T>(path: &str) -> Result<DB<T>, DBError> where T: DeserializeOwned {
    let contents: String = fs::read_to_string(path).unwrap_or_default();
    let mut db: HashMap<String, T> = HashMap::new();
    for line in contents.lines() {
        let kv_option: Option<(&str, &str)> = line.split_once('=');
        if let Some((k, v)) = kv_option {
            let value: T = serde_json::from_str(v.trim())?;
            db.insert(k.trim().to_string(), value);
        }
        
    }
    return Ok(db);
}

pub fn save_db<T>(path: &str, contents: &DB<T>) -> Result<(), DBError> where T: Serialize {
    delete_old_backups()?;
    let temp_path  = format!("{}.{}", path, "temp");
    let backup_path  = format!("backups/{}", chrono::Local::now().to_rfc3339());
    let file_path = path.to_string();
    fs::File::create(&temp_path)?;
    if !(fs::exists(&file_path)?) {
        fs::File::create(&file_path)?;
    }
    if !(fs::exists("backups")?) {
        fs::create_dir("backups")?;
    }
    fs::copy(&file_path, &backup_path)?;
    let mut temp_file = fs::OpenOptions::new().write(true).create(true).append(true).open(&temp_path)?;
    for (key,value) in contents {
        temp_file.write(format!("{}={}\n", key, serde_json::to_string(value)?).as_bytes())?;
    }
    fs::copy(&temp_path, &file_path)?;
    fs::remove_file(temp_path)?;
    Ok(())
}

fn delete_old_backups() -> Result<(), std::io::Error>{{
    let backup_dir  = "backups";
    let backup_path = Path::new(backup_dir);
    let paths = fs::read_dir(backup_dir)?;
    let mut file_names: Vec<DateTime<FixedOffset>> = Vec::new();
    for path_result in paths {
        match path_result {
            Ok(path) => file_names.push(
                DateTime::parse_from_rfc3339(path.file_name().to_str().unwrap()).unwrap()
            ),
            Err(_) => {}
        }
    }
    file_names.sort();

    let backups_to_delete = file_names.len().saturating_sub(MAX_BACKUPS);
    for entry in file_names.iter().take(backups_to_delete) {
        let file_path = backup_path.join(entry.to_rfc3339());
        fs::remove_file(file_path)?;
    }
    Ok(())
}}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::collections::HashMap;

    #[test]
    fn save_and_load_round_trip_strings() {
        let path = "target/test_db_strings.txt";

        // ensure a clean slate for this test file
        let _ = fs::remove_file(path);

        let mut original: DB<String> = HashMap::new();
        original.insert("key1".to_string(), "value1".to_string());
        original.insert("key2".to_string(), "value2".to_string());

        save_db(path, &original).expect("saving db should succeed");
        let loaded: DB<String> = load_db(path).expect("loading db should succeed");

        assert_eq!(original, loaded);
    }

    #[test]
    fn save_and_load_empty_db() {
        let path = "target/test_db_empty.txt";

        let _ = fs::remove_file(path);

        let original: DB<String> = HashMap::new();

        save_db(path, &original).expect("saving empty db should succeed");
        let loaded: DB<String> = load_db(path).expect("loading empty db should succeed");

        assert!(loaded.is_empty());
    }
}
