//! User interface rendering module
//!
//! This module contains all TUI rendering logic using the ratatui library.
//! Each submodule handles rendering for a specific UI component or view.
//!
//! # Architecture
//!
//! The UI is organized into specialized rendering modules:
//! - `partition_view` - Partition list display
//! - `message_view` - Message list and detail views
//! - `search_view` - Global search interface
//! - `broker` - Broker and topic switchers
//! - `profile` - Profile management dialogs
//! - `dialogs` - Various input dialogs and modals
//! - `status` - Status bar and error panels
//! - `helpers` - UI utility functions

mod broker;
mod dialogs;
mod helpers;
mod message_view;
mod partition_view;
mod profile;
mod search_view;
mod status;

use crate::{App, AppMode};
use broker::{render_broker_switcher, render_topic_switcher};
use dialogs::{render_file_saved_modal, render_offset_jump_dialog, render_search_dialog};
use message_view::{render_message_detail, render_message_list};
use partition_view::render_partition_list;
use profile::{render_connection_test, render_profile_delete_confirm, render_profile_editor};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
    Frame,
};
use search_view::render_global_search;
use status::{render_error_panel, render_loading, render_status_bar};

/// Main UI rendering function
///
/// Orchestrates the rendering of all UI components based on the current
/// application mode. Handles the layout structure and delegates to
/// specialized rendering functions for each view.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state
pub(crate) fn ui(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Title
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Status bar
        ])
        .split(f.area());

    // Build comprehensive header with all connection and topic info
    let profile_name = app
        .connection_manager
        .current_profile
        .as_deref()
        .unwrap_or("default");
    let broker = &app.stats.broker_address;
    let cache_rate = app.stats.cache_hit_rate();

    let header_text = format!(
        "Topic: {} | Profile: {} | Broker: {} | Messages: {}/{} | Cache: {:.0}%",
        app.topic_name,
        profile_name,
        broker,
        app.stats.messages_loaded,
        app.stats.total_messages,
        cache_rate
    );

    let title = Paragraph::new(header_text)
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .alignment(Alignment::Center)
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(title, chunks[0]);

    // Split main area if showing errors
    let main_area = if app.show_errors && !app.error_log.is_empty() {
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
            .split(chunks[1]);

        render_error_panel(f, app, main_chunks[1]);
        main_chunks[0]
    } else {
        chunks[1]
    };

    match app.mode {
        AppMode::BrokerSwitcher => {
            render_partition_list(f, app, main_area);
            render_broker_switcher(f, app);
        }
        AppMode::TopicSwitcher => {
            render_partition_list(f, app, main_area);
            render_topic_switcher(f, app);
        }
        AppMode::ProfileEditor => {
            render_partition_list(f, app, main_area);
            render_profile_editor(f, app);
        }
        AppMode::ProfileDeleting => {
            render_partition_list(f, app, main_area);
            render_profile_delete_confirm(f, app);
        }
        AppMode::ConnectionTesting => {
            render_partition_list(f, app, main_area);
            render_connection_test(f, app);
        }
        AppMode::PartitionList => render_partition_list(f, app, main_area),
        AppMode::MessageList => render_message_list(f, app, main_area),
        AppMode::MessageDetail => render_message_detail(f, app, main_area),
        AppMode::OffsetJump => {
            render_message_list(f, app, main_area);
            render_offset_jump_dialog(f, app);
        }
        AppMode::Search => {
            render_message_list(f, app, main_area);
            render_search_dialog(f, app);
        }
        AppMode::Loading => render_loading(f, app, main_area),
        AppMode::KeyAnalysis => render_loading(f, app, main_area), // Reuse loading view for analysis
        AppMode::GlobalSearch => render_global_search(f, app, main_area),
        AppMode::FileSaved => {
            render_message_detail(f, app, main_area);
            render_file_saved_modal(f, app);
        }
    }

    // Render status bar at the bottom
    render_status_bar(f, app, chunks[2]);
}
