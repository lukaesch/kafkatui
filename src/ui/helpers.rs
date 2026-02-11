//! UI utility functions
//!
//! Helper functions for UI layout and rendering.

use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Creates a centered rectangle within a given area
///
/// Useful for creating modal dialogs and popups. Centers the rectangle
/// both horizontally and vertically.
///
/// # Arguments
/// - `percent_x` - Width as a percentage (0-100)
/// - `percent_y` - Height as a percentage (0-100)
/// - `r` - The parent rectangle to center within
///
/// # Returns
/// A Rect centered within the parent area
pub(crate) fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
