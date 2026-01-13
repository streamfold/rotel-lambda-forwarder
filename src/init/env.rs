use dotenvy::Substitutor;
use tower::BoxError;

pub fn load_file(env_file: &String) -> Result<(), BoxError> {
    let subs = load_file_updates(env_file)?;

    for (key, val) in subs {
        unsafe { std::env::set_var(key, val) }
    }

    Ok(())
}

fn load_file_updates(env_file: &String) -> Result<Vec<(String, String)>, BoxError> {
    let mut updates = Vec::new();
    for item in dotenvy::from_filename_iter_custom_sub(env_file, ArnEnvSubstitutor {})
        .map_err(|e| format!("failed to open env file {}: {}", env_file, e))?
    {
        let (key, val) = item.map_err(|e| format!("unable to parse line: {}", e))?;
        updates.push((key, val))
    }

    Ok(updates)
}

#[derive(Clone)]
struct ArnEnvSubstitutor;
impl Substitutor for ArnEnvSubstitutor {
    fn substitute(&self, val: &str) -> Option<String> {
        // We'll expand this later
        if val.starts_with("arn:") {
            // need to escape curly braces
            return Some(format!("${{{}}}", val));
        }

        // Fall back to normal env expansion
        std::env::var(val).ok()
    }
}
