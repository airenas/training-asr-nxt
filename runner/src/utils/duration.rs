use std::{
    borrow::Cow,
    iter::Sum,
    ops::Div,
    time::{self, Duration},
};

use ansi_term::Color;

struct CycledArray<T> {
    max: usize,
    index: usize,
    data: Vec<T>,
}

pub struct ETACalculator {
    total: usize,
    completed: usize,
    durations: CycledArray<Duration>,
    last_added: time::Instant,
}

impl<T> CycledArray<T> {
    fn new(max: usize) -> anyhow::Result<Self> {
        if max == 0 {
            return Err(anyhow::anyhow!("max must be greater than 0"));
        }
        Ok(Self {
            max,
            index: 0,
            data: vec![],
        })
    }

    fn add(&mut self, value: T) {
        if self.data.len() < self.max {
            self.data.push(value);
            return;
        }
        self.data[self.index] = value;
        self.index = (self.index + 1) % self.max;
    }
}

impl<T> CycledArray<T>
where
    T: Clone + Default + Sum + Div<u32, Output = T>,
{
    fn average(&self) -> T {
        if self.data.is_empty() {
            return T::default();
        }
        let sum: T = self.data.iter().cloned().sum();
        sum / self.data.len() as u32
    }
}

impl ETACalculator {
    pub fn new(total: usize, max: usize) -> anyhow::Result<Self> {
        if total == 0 {
            return Err(anyhow::anyhow!("total must be greater than 0"));
        }
        let durations = CycledArray::new(max)?;
        Ok(Self {
            total,
            completed: 0,
            durations,
            last_added: time::Instant::now(),
        })
    }

    pub fn add_completed(&mut self) {
        self.completed += 1;
    }

    pub fn add_completed_with_duration(&mut self) {
        self.add_completed();
        let now = time::Instant::now();
        let value = now.duration_since(self.last_added);
        self.last_added = now;
        self.durations.add(value);
    }

    pub fn eta(&self) -> Duration {
        if self.completed == 0 {
            return Duration::ZERO;
        }
        let avg = self.durations.average();
        let remaining = self.total - self.completed;
        avg * remaining as u32
    }
    pub fn eta_str(&self) -> Cow<'static, str> {
        let eta = self.eta();
        if eta.as_secs() == 0 {
            return Cow::Borrowed("--");
        }
        // Cow::Owned(format_duration_most_significant(eta))
        Cow::Owned(format!(
            "{}-{}",
            format_duration_most_significant(eta),
            self.durations.data.len()
        ))
    }
    pub fn completed(&self) -> usize {
        self.completed
    }
    pub fn total(&self) -> usize {
        self.total
    }
    pub fn remaining(&self) -> usize {
        self.total - self.completed
    }
    pub fn speed_per_day(&self) -> f32 {
        let avg = self.durations.average();
        if avg.as_secs() == 0 {
            return 0.0;
        }
        86400_f32 / avg.as_secs_f32()
    }
    pub fn progress(&self) -> f32 {
        if self.total == 0 {
            return 0.0;
        }
        self.completed as f32 / self.total as f32
    }
}

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
        return format!("{days}d{}", if_non_zero(hours % 24, "h"));
    }
    if hours > 0 {
        return format!("{hours}h{}", if_non_zero(minutes % 60, "m"));
    }
    if minutes > 0 {
        return format!("{minutes}m{}", if_non_zero(secs % 60, "s"));
    }
    format!("{secs}s")
}

fn if_non_zero(v: u64, s: &str) -> Cow<'static, str> {
    if v > 0 {
        return Cow::Owned(format!(" {v}{s}"));
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

    #[test]
    fn test_cycled_array() {
        let test_cases = vec![
            // Test case: simple
            (3, vec![1, 2, 3], vec![1, 2, 3], 0),
            (3, vec![], vec![], 0),
            (3, vec![1, 2], vec![1, 2], 0),
            (3, vec![1, 2, 3, 4], vec![4, 2, 3], 1),
            (3, vec![1, 2, 3, 4, 5], vec![4, 5, 3], 2),
        ];

        for (l, input, expected, expected_index) in test_cases {
            let mut cycled_array = CycledArray::<u32>::new(l).unwrap();
            for i in input {
                cycled_array.add(i);
            }
            assert_eq!(cycled_array.data, expected);
            assert_eq!(cycled_array.index, expected_index);
        }
    }

    #[test]
    fn test_cycled_array_avg() {
        let test_cases = vec![
            // Test case: simple
            (3, vec![1, 2, 3], 2),
            (3, vec![], 0),
            (3, vec![1, 3], 2),
            (3, vec![1, 2, 3, 4], 3),
            (3, vec![1, 2, 3, 4, 5], 4),
        ];

        for (l, input, expected_avg) in test_cases {
            let mut cycled_array = CycledArray::<u32>::new(l).unwrap();
            for i in input {
                cycled_array.add(i);
            }
            assert_eq!(cycled_array.average(), expected_avg);
        }
    }

    #[test]
    fn test_cycled_array_fail() {
        let result = CycledArray::<u32>::new(0);
        assert!(result.is_err());
        let result = CycledArray::<u32>::new(1);
        assert!(result.is_ok());
    }
}

pub fn get_info_str(
    use_color: bool,
    ok: i32,
    skipped: i32,
    failed: i32,
    rem: i32,
    speed: f32,
) -> String {
    format!(
        "done: {}, skipped: {}, failed: {}, rem: {}, speed/d: {:.1}",
        paint(use_color, Color::Green, ok),
        paint(use_color, Color::Yellow, skipped),
        paint(use_color, Color::Red, failed),
        paint(use_color, Color::Green, rem),
        speed,
    )
}

fn paint(use_color: bool, colour: Color, v: i32) -> String {
    if use_color && v > 0 {
        return colour.bold().paint(format!("{v}")).to_string();
    }
    format!("{v}")
}