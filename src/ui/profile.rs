//! Profile editor and management dialogs
//!
//! Handles the rendering of profile creation/editing forms, deletion
//! confirmation, and connection testing dialogs.

use crate::ui::helpers::centered_rect;
use crate::{App, ProfileEditMode, ProfileField};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

/// Renders the profile editor dialog
///
/// Shows a form for creating or editing a broker profile with fields:
/// - Name
/// - Broker address
/// - Username
/// - Password (with show/hide and environment variable support)
/// - Schema Registry URL (optional)
///
/// Displays validation errors if any fields are invalid.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state (mutable for list state)
pub(crate) fn render_profile_editor(f: &mut Frame, app: &mut App) {
    let area = centered_rect(70, 80, f.area());
    f.render_widget(Clear, area);

    if let Some(editor) = &app.profile_editor {
        let title = match editor.mode {
            ProfileEditMode::Creating => "Create New Profile",
            ProfileEditMode::Editing => "Edit Profile",
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(2)
            .constraints([
                Constraint::Length(3), // Name
                Constraint::Length(3), // Broker
                Constraint::Length(3), // User
                Constraint::Length(3), // Password
                Constraint::Length(3), // Schema Registry
                Constraint::Min(2),    // Validation errors
                Constraint::Length(2), // Help
            ])
            .split(area);

        // Name field
        let name_style = if editor.active_field == ProfileField::Name {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };
        let name = Paragraph::new(editor.name.as_str()).block(
            Block::default()
                .title("Name")
                .borders(Borders::ALL)
                .border_style(name_style),
        );
        f.render_widget(name, chunks[0]);

        // Broker field
        let broker_style = if editor.active_field == ProfileField::Broker {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };
        let broker = Paragraph::new(editor.broker.as_str()).block(
            Block::default()
                .title("Broker (host:port)")
                .borders(Borders::ALL)
                .border_style(broker_style),
        );
        f.render_widget(broker, chunks[1]);

        // User field
        let user_style = if editor.active_field == ProfileField::User {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };
        let user = Paragraph::new(editor.user.as_str()).block(
            Block::default()
                .title("Username")
                .borders(Borders::ALL)
                .border_style(user_style),
        );
        f.render_widget(user, chunks[2]);

        // Password field
        let password_style = if editor.active_field == ProfileField::Password {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };

        let password_display = if editor.use_env_var {
            format!("${{{}}}", editor.env_var_name)
        } else if editor.show_password {
            editor.password.clone()
        } else {
            "•".repeat(editor.password.len())
        };

        let password_title = if editor.use_env_var {
            "Password (Environment Variable) [Ctrl+E to toggle]"
        } else {
            "Password [Ctrl+E for env var, Ctrl+S to show/hide]"
        };

        let password = Paragraph::new(password_display).block(
            Block::default()
                .title(password_title)
                .borders(Borders::ALL)
                .border_style(password_style),
        );
        f.render_widget(password, chunks[3]);

        // Schema Registry field
        let schema_style = if editor.active_field == ProfileField::SchemaRegistry {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };
        let schema = Paragraph::new(editor.schema_registry.as_str()).block(
            Block::default()
                .title("Schema Registry URL (optional)")
                .borders(Borders::ALL)
                .border_style(schema_style),
        );
        f.render_widget(schema, chunks[4]);

        // Validation errors
        if !editor.validation_errors.is_empty() {
            let errors: Vec<Line> = editor
                .validation_errors
                .iter()
                .map(|e| {
                    Line::from(vec![
                        Span::styled("! ", Style::default().fg(Color::Red)),
                        Span::raw(e),
                    ])
                })
                .collect();
            let error_widget = Paragraph::new(errors)
                .style(Style::default().fg(Color::Red))
                .block(Block::default().borders(Borders::NONE));
            f.render_widget(error_widget, chunks[5]);
        }

        // Help text
        let help = Paragraph::new(
            "Tab/Shift+Tab: Navigate | Enter: Save | Ctrl+T: Test Connection | ESC: Cancel",
        )
        .style(Style::default().fg(Color::Gray))
        .alignment(Alignment::Center);
        f.render_widget(help, chunks[6]);

        // Main block
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));
        f.render_widget(block, area);
    }
}

/// Renders the profile deletion confirmation dialog
///
/// Shows a warning modal before deleting a profile.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state containing the profile to delete
pub(crate) fn render_profile_delete_confirm(f: &mut Frame, app: &App) {
    let area = centered_rect(50, 20, f.area());
    f.render_widget(Clear, area);

    if let Some(profile_name) = &app.profile_to_delete {
        let text = vec![
            Line::from(""),
            Line::from(vec![
                Span::raw("Delete profile '"),
                Span::styled(profile_name, Style::default().fg(Color::Yellow)),
                Span::raw("'?"),
            ]),
            Line::from(""),
            Line::from("This action cannot be undone."),
            Line::from(""),
            Line::from(vec![
                Span::styled("Press ", Style::default()),
                Span::styled(
                    "Y",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                ),
                Span::raw(" to confirm or "),
                Span::styled("N/ESC", Style::default().fg(Color::Green)),
                Span::raw(" to cancel"),
            ]),
        ];

        let paragraph = Paragraph::new(text).alignment(Alignment::Center).block(
            Block::default()
                .title("Confirm Deletion")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Red)),
        );

        f.render_widget(paragraph, area);
    }
}

/// Renders the connection test dialog
///
/// Shows the progress and results of testing a profile's connection
/// to Kafka.
///
/// # Arguments
/// - `f` - The Frame to render into
/// - `app` - The application state containing test results
pub(crate) fn render_connection_test(f: &mut Frame, app: &App) {
    let area = centered_rect(60, 30, f.area());
    f.render_widget(Clear, area);

    let content = if let Some(result) = &app.test_result {
        vec![
            Line::from(""),
            Line::from("Test Result:"),
            Line::from(""),
            Line::from(result.as_str()),
        ]
    } else {
        vec![
            Line::from(""),
            Line::from(Span::styled(
                "Testing connection...",
                Style::default().fg(Color::Yellow),
            )),
            Line::from(""),
            Line::from("Please wait..."),
        ]
    };

    let paragraph = Paragraph::new(content).alignment(Alignment::Center).block(
        Block::default()
            .title("Connection Test")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );

    f.render_widget(paragraph, area);
}
