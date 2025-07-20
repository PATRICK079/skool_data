"""
notifications.py

last updated: 2025-07-12

todo
- add unit tests
- add google style docs for doc generation
- replace with gmail style notifications
- expand for reports on the scraper
"""

# external
import subprocess
import os


def send_desktop_notification(title, message, sticky=True):
    """
    Send a desktop notification on macOS.

    Args:
        title (str): The notification title
        message (str): The notification message
        sticky (bool): If True, notification will stay until manually dismissed
    """
    # Only try to send notifications on macOS where osascript exists
    if not os.path.exists("/usr/bin/osascript"):
        # We're not on macOS, just print the notification
        print(f"NOTIFICATION: {title} - {message}")
        return

    try:
        if sticky:
            script = f"""
            on run argv
                set theTitle to item 1 of argv
                set theMessage to item 2 of argv
                display notification theMessage with title theTitle with sticky
            end run
            """
        else:
            script = f"""
            on run argv
                set theTitle to item 1 of argv
                set theMessage to item 2 of argv
                display notification theMessage with title theTitle
            end run
            """

        subprocess.run(["osascript", "-e", script, title, message], check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Fallback to just printing the notification
        print(f"NOTIFICATION: {title} - {message}")


if __name__ == "__main__":
    pass
