yPort is a Telegram bot that reports and tracks personal deposits in Yearn Vaults.

On initialization by sending /start, send your EOAs after the bot's prompt. The bot will process the submitted addresses to send a report immediately. Daily reports at 0000 UTC will be sent, and on demand reporting is available with /report. A maximum history of seven days will be tracked for 1D and 7D changes in APRs and USD values.

To avoid multiple RPC and API requests, please don't spam /report.

The bot is available for public use (untested) here: [@yPort](https://t.me/yPortBot)
