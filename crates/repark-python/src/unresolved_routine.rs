pub(crate) fn unresolved_routine(message: &str) -> Option<&str> {
    let (_, tail) = message.split_once("Invalid function '")?;
    let (name, _) = tail.split_once('\'')?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return None;
    }
    Some(name)
}
