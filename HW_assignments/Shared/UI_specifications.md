# UI specifications

Our application uses **Slack** as its primary interface. During customer discovery, we learned that many business users already rely on Slack day to day.

Rather than asking them to adopt another standalone web app, we chose to meet users where they already work.

To use the product, someone mentions **@DataGrep bot** in a channel or direct message and describes what they need. That message triggers our backend; we process the request and post the response in the **same Slack thread** so the conversation stays in one place.

Screenshots of the UI are listed in the [Implementation Evidence and References](../../README.md#implementation-evidence-and-references) section of the project README.

## At a glance

| Item | Detail |
|------|--------|
| Primary interface | Slack |
| Bot | @DataGrep bot |
| Interaction | User @-mentions the bot with a request; backend runs processing; bot replies in the originating thread |
