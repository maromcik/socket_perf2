use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Stats {
    pub id: usize,
    pub total_bytes: u64,
    pub total_messages: u64,
    /// Actual measured send window; rates must be derived from this,
    /// never from the configured duration (which is only a deadline).
    pub elapsed: Duration,
}

/// Scales `value` to the largest SI decimal prefix that keeps the
/// magnitude below 1000, e.g. `si_scale(3_500_000.0, "bps")` -> "3.50 Gbps".
pub fn si_scale(value: f64, unit: &str) -> String {
    const PREFIXES: [&str; 5] = ["", "K", "M", "G", "T"];
    let mut value = value;
    let mut prefix = 0;
    while value >= 1000.0 && prefix < PREFIXES.len() - 1 {
        value /= 1000.0;
        prefix += 1;
    }
    format!("{value:.2} {}{unit}", PREFIXES[prefix])
}

/// Renders a simple bordered ASCII table; all cells are right-aligned.
pub fn build_table(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }

    let border = format!(
        "+{}",
        widths
            .iter()
            .map(|w| "-".repeat(w + 2) + "+")
            .collect::<String>()
    );
    let render_row = |cells: &mut dyn Iterator<Item = &str>| {
        let mut line = String::from("|");
        for (cell, w) in cells.zip(&widths) {
            line.push_str(&format!(" {cell:>w$} |"));
        }
        line
    };

    let mut lines = vec![
        border.clone(),
        render_row(&mut headers.iter().copied()),
        border.clone(),
    ];
    for row in rows {
        lines.push(render_row(&mut row.iter().map(String::as_str)));
    }
    lines.push(border);
    lines.join("\n")
}
