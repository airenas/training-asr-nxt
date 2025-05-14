use std::{borrow::Cow, time::Duration};

pub fn format_duration_most_significant(dur: Duration) -> String {
    let secs = dur.as_secs();
    let trimmed = if secs / 86_400 > 1 {
        Duration::from_secs((secs / 3_600) * 3_600)
    } else if secs / 3_600 > 1 {
        Duration::from_secs((secs / 60) * 60)
    } else {
        Duration::from_secs(secs)
    };
    format_duration(trimmed).to_string()
}

fn format_duration(dur: Duration) -> String {
    let secs = dur.as_secs();
    let minutes = secs / 60;
    let hours = minutes / 60;
    let days = hours / 24;
    if days > 0 {
        return format!("{}d{}", days, if_non_zero(hours % 24, "h"));
    }
    if hours > 0 {
        return format!("{}h{}", hours, if_non_zero(minutes % 60, "m"));
    }
    if minutes > 0 {
        return format!("{}m{}", minutes, if_non_zero(secs % 60, "s"));
    }
    format!("{}s", secs)
}

fn if_non_zero(v: u64, s: &str) -> Cow<'static, str> {
    if v > 0 {
        return Cow::Owned(format!(" {}{}", v, s));
    }
    Cow::Borrowed("")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration_most_significant() {
        let test_cases = vec![
            // Test case: simple
            (Duration::from_secs(10), "10s"),
            (Duration::from_millis(10700), "10s"),
            (Duration::from_secs(3000), "50m"),
            (Duration::from_secs(10000), "2h 46m"),
            (Duration::from_secs(18 * 3_600 + 90), "18h 1m"),
            (Duration::from_secs(50 * 3_600 + 80), "2d 2h"),
            (
                Duration::from_secs(50 * 24 * 3_600 + 10 * 3600 + 50),
                "50d 10h",
            ),
        ];

        for (input, expected) in test_cases {
            let result = format_duration_most_significant(input);
            assert_eq!(result, expected);
        }
    }
}
