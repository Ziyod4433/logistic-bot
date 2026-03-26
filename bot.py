"""
Legacy entrypoint kept only to avoid confusion with the old polling setup.

The production bot now works through the webhook handled in app.py.
"""


def main():
    print(
        "This project no longer uses a separate polling bot.\n"
        "Run `python app.py` and configure BOT_TOKEN + WEBHOOK_BASE_URL instead."
    )


if __name__ == "__main__":
    main()
