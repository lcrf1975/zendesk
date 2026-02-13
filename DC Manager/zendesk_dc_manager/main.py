"""
Application entry point for Zendesk DC Manager.

This module provides the main() function to start the application.
"""

import sys

from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QColor, QPalette

from zendesk_dc_manager.config import configure_qt_environment
from zendesk_dc_manager.ui_styles import get_main_stylesheet
from zendesk_dc_manager.ui_main import ZendeskWizard


def main() -> int:
    """
    Application entry point.

    Initializes the Qt application, configures the theme,
    and starts the main window.

    Returns:
        Exit code (0 for success).
    """
    configure_qt_environment()

    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    palette = app.palette()
    palette.setColor(QPalette.ColorRole.Window, QColor("#F3F4F6"))
    palette.setColor(QPalette.ColorRole.WindowText, QColor("#111827"))
    palette.setColor(QPalette.ColorRole.Base, QColor("#FFFFFF"))
    palette.setColor(QPalette.ColorRole.AlternateBase, QColor("#F9FAFB"))
    palette.setColor(QPalette.ColorRole.Text, QColor("#111827"))
    palette.setColor(QPalette.ColorRole.Button, QColor("#FFFFFF"))
    palette.setColor(QPalette.ColorRole.ButtonText, QColor("#111827"))
    palette.setColor(QPalette.ColorRole.Highlight, QColor("#2563EB"))
    palette.setColor(QPalette.ColorRole.HighlightedText, QColor("#FFFFFF"))
    app.setPalette(palette)

    app.setStyleSheet(get_main_stylesheet())

    window = ZendeskWizard()
    window.show()

    return app.exec()


if __name__ == "__main__":
    sys.exit(main())